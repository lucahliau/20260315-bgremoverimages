#!/usr/bin/env python3
"""
embed_worker.py — Local CLIP image embeddings → Postgres (pgvector), resumable.

Reads ClothingItem rows with hasNobg=true missing an ItemEmbedding for the chosen model.
Downloads the -nobg.png from R2_PUBLIC_URL (same path rule as backend getNobgUrl).

Usage:
  source .env   # DATABASE_URL, R2_PUBLIC_URL required
  python3 embed_worker.py [--model clip-vit-b-32-image] [--batch-size 32] [--download-workers 12] [--limit N] [--dry-run] [--device mps|cpu]

Setup: npm run embed:setup   # or pip install in venv (see package.json)
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

# psycopg2 / libpq accept a strict set of query params. Anything else
# (e.g. ?pgbouncer=true added by Prisma for Supabase) raises
# "invalid URI query parameter".
LIBPQ_QUERY_PARAMS = {
    "sslmode",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "sslpassword",
    "sslcrl",
    "connect_timeout",
    "application_name",
    "options",
    "fallback_application_name",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
    "replication",
    "gssencmode",
    "target_session_attrs",
    "service",
    "passfile",
    "channel_binding",
}


def sanitize_db_url_for_psycopg2(url: str) -> str:
    """Drop query params that libpq doesn't recognise (e.g. pgbouncer=true).

    Returns the URL with only libpq-known params kept.
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k in LIBPQ_QUERY_PARAMS]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment))

SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "embed-progress.json"
HISTORY_FILE = SCRIPT_DIR / "embed-history.jsonl"
LOG_FILE = SCRIPT_DIR / "embed.log"

# model id -> sentence-transformers name, embedding dim
MODEL_REGISTRY: dict[str, dict[str, Any]] = {
    "clip-vit-b-32-image": {
        "st_name": "clip-ViT-B-32",
        "dim": 512,
    },
}

CLIP_DIM = 512


def log_line(msg: str, also_print: bool = True, lock: threading.Lock | None = None) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"

    def _write() -> None:
        if also_print:
            print(line, flush=True)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    if lock is not None:
        with lock:
            _write()
    else:
        _write()


def get_nobg_key(original_url: str, r2_base_url: str) -> str | None:
    """R2 object KEY for the -nobg.png variant (products/.../x-nobg.png), or None
    if the url isn't a product image. Mirrors backend getNobgUrl's path rule, but
    returns the key (the S3 API needs the key; the public URL is base + '/' + key)."""
    base = r2_base_url.rstrip("/")
    clean = original_url.split("?")[0].split("#")[0]
    if clean.startswith(base):
        path = clean[len(base) :].lstrip("/")
    elif clean.startswith("products/"):
        path = clean
    else:
        return None
    if not path.startswith("products/"):
        return None
    last_slash = path.rfind("/")
    filename = path[last_slash + 1 :]
    if "." in filename:
        name_without_ext = filename.rsplit(".", 1)[0]
    else:
        name_without_ext = filename
    return path[: last_slash + 1] + name_without_ext + "-nobg.png"


def get_nobg_url(original_url: str, r2_base_url: str) -> str | None:
    """Public r2.dev URL for the -nobg.png variant. Mirror backend getNobgUrl."""
    key = get_nobg_key(original_url, r2_base_url)
    return f"{r2_base_url.rstrip('/')}/{key}" if key else None


# --- R2 download transport ---------------------------------------------------
# Prefer the AUTHENTICATED S3 API (the same endpoint remove-bg-parallel.ts and
# upload.ts already use). The public r2.dev URL is per-IP rate-limited, so the
# embed worker's concurrent downloads draw 429s from one home IP; the S3 endpoint
# has no such per-IP throttle. boto3 may be absent (the managed venv predates
# this change); if so we transparently fall back to the public URL + retries, so
# this file is safe to land BEFORE boto3 is in the venv.
try:
    import boto3 as _boto3
    from botocore.config import Config as _BotoConfig

    _BOTO3_OK = True
except Exception:
    _BOTO3_OK = False

_USE_S3 = os.environ.get("USE_S3_DOWNLOAD", "auto").strip().lower()
_S3_BUCKET = os.environ.get("R2_BUCKET_NAME", "").strip()
_s3_client = None

# Status codes worth retrying (transient). Any other HTTP error — notably 404/403
# — is permanent: the object is missing or forbidden and re-trying won't help.
_TRANSIENT_HTTP = {408, 425, 429, 500, 502, 503, 504}
_MAX_DOWNLOAD_ATTEMPTS = max(1, int(os.environ.get("EMBED_DOWNLOAD_ATTEMPTS", "3")))


class _PermanentDownloadError(Exception):
    """Missing/forbidden/corrupt — quarantine the item, don't retry."""


class _TransientDownloadError(Exception):
    """Throttle/timeout/5xx — retry; if it persists, leave the item for next run."""

    def __init__(self, msg: str, retry_after: float | None = None) -> None:
        super().__init__(msg)
        self.retry_after = retry_after


def s3_enabled() -> bool:
    """True when we can and should fetch via the authenticated S3 API."""
    if _USE_S3 in ("0", "false", "no", "off"):
        return False
    if not _BOTO3_OK or not _S3_BUCKET:
        return False
    return bool(
        os.environ.get("R2_ACCOUNT_ID")
        and os.environ.get("R2_ACCESS_KEY_ID")
        and os.environ.get("R2_SECRET_ACCESS_KEY")
    )


def get_s3_client() -> Any:
    global _s3_client
    if _s3_client is None:
        acct = os.environ["R2_ACCOUNT_ID"]
        _s3_client = _boto3.client(
            "s3",
            endpoint_url=f"https://{acct}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            region_name="auto",
            config=_BotoConfig(
                # We run our own classified retry loop, so disable boto's.
                retries={"max_attempts": 0, "mode": "standard"},
                connect_timeout=10,
                read_timeout=30,
                max_pool_connections=64,
            ),
        )
    return _s3_client


def load_progress() -> tuple[set[str], set[str]]:
    """Return (completed_ids, permanent_failed_ids).

    Backward compatible: an old file with only {completed, failed} loads
    `completed` and starts the permanent set EMPTY — the legacy flat `failed`
    set conflated transient 429s with real 404s, so we drop it ONCE here. Real
    permanent failures re-accumulate under the new classifier within a chunk or
    two, and genuinely-embeddable items wrongly marked failed get a fresh try.
    """
    if not PROGRESS_FILE.exists():
        return set(), set()
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        completed = set(str(x) for x in (data.get("completed") or []))
        permanent = set(str(x) for x in (data.get("permanent") or []))
        return completed, permanent
    except (json.JSONDecodeError, OSError):
        return set(), set()


def save_progress(completed: set[str], permanent: set[str]) -> None:
    """Write progress snapshot. Caller MUST already hold the progress lock."""
    PROGRESS_FILE.write_text(
        json.dumps(
            {"completed": sorted(completed), "permanent": sorted(permanent)},
            indent=2,
        ),
        encoding="utf-8",
    )


def append_history(record: dict[str, Any]) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def require_env() -> tuple[str, str]:
    db = os.environ.get("DATABASE_URL", "").strip()
    r2 = os.environ.get("R2_PUBLIC_URL", "").strip()
    if not db:
        print("Missing DATABASE_URL", file=sys.stderr)
        sys.exit(1)
    if not r2:
        print("Missing R2_PUBLIC_URL (needed to build -nobg.png URLs)", file=sys.stderr)
        sys.exit(1)
    return db, r2.rstrip("/")


def fetch_work_batch(
    conn: Any, model: str, limit: int, exclude_ids: list[str] | None = None
) -> list[tuple[str, str]]:
    """Return list of (item_id, imageUrl) needing embeddings (DB is source of truth).

    `exclude_ids` are items known to fail permanently (missing -nobg.png, etc.).
    Without excluding them, a block of dead rows with the newest createdAt sits at
    the TOP of every batch and re-fails forever, so the run exhausts its --limit on
    poison and never reaches the real backlog underneath — the cause of ~0
    throughput. The anti-join with an empty array is a no-op (keeps all rows).
    """
    cur = conn.cursor()
    exclude = exclude_ids or []
    cur.execute(
        """
        SELECT ci.id::text, ci."imageUrl"
        FROM "ClothingItem" ci
        WHERE ci.active = true AND ci."hasNobg" = true
          AND NOT (ci.id::text = ANY(%s::text[]))
          AND NOT EXISTS (
            SELECT 1 FROM "ItemEmbedding" ie
            WHERE ie."itemId" = ci.id AND ie.model = %s
          )
        ORDER BY ci."createdAt" DESC
        LIMIT %s
        """,
        (exclude, model, limit),
    )
    rows = cur.fetchall()
    cur.close()
    return [(r[0], r[1]) for r in rows]


@dataclass
class DownloadOutcome:
    item_id: str
    image: Any = None  # PIL.Image when ok
    error: str | None = None
    error_kind: str | None = None  # 'permanent' | 'transient' when error is set
    bytes_downloaded: int = 0
    elapsed_ms: int = 0


def _sleep_backoff(attempt: int, retry_after: float | None) -> None:
    import random

    if retry_after is not None and retry_after >= 0:
        time.sleep(min(retry_after, 30.0))
        return
    time.sleep(min(2.0 ** attempt, 20.0) + random.uniform(0, 0.5))


def _http_get(url: str, connect_timeout: float, read_timeout: float, max_bytes: int) -> bytes:
    """Fetch via the public r2.dev URL. Raises _Transient/_PermanentDownloadError."""
    import requests

    try:
        with requests.get(url, timeout=(connect_timeout, read_timeout), stream=True) as r:
            if r.status_code in _TRANSIENT_HTTP:
                ra = r.headers.get("Retry-After")
                retry_after: float | None = None
                if ra:
                    try:
                        retry_after = float(ra)
                    except ValueError:
                        retry_after = None
                raise _TransientDownloadError(f"{r.status_code} {r.reason}", retry_after)
            if r.status_code >= 400:
                raise _PermanentDownloadError(f"{r.status_code} {r.reason}")
            buf = bytearray()
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                buf.extend(chunk)
                if len(buf) > max_bytes:
                    raise _PermanentDownloadError(f"oversized: {len(buf)} > {max_bytes}")
            return bytes(buf)
    except requests.Timeout as e:
        raise _TransientDownloadError(f"timeout: {e}")
    except requests.ConnectionError as e:
        raise _TransientDownloadError(f"connection: {e}")


def _s3_get(key: str, max_bytes: int) -> bytes:
    """Fetch via the authenticated S3 API. Raises _Transient/_PermanentDownloadError."""
    from botocore.exceptions import (
        ClientError,
        ConnectTimeoutError,
        EndpointConnectionError,
        ReadTimeoutError,
    )

    client = get_s3_client()
    try:
        resp = client.get_object(Bucket=_S3_BUCKET, Key=key)
        data = resp["Body"].read(max_bytes + 1)
        if len(data) > max_bytes:
            raise _PermanentDownloadError(f"oversized: > {max_bytes}")
        return data
    except ClientError as e:
        meta = e.response.get("ResponseMetadata", {})
        code = meta.get("HTTPStatusCode")
        ecode = e.response.get("Error", {}).get("Code", "")
        if code in _TRANSIENT_HTTP or ecode in (
            "SlowDown",
            "RequestTimeout",
            "InternalError",
            "ServiceUnavailable",
        ):
            raise _TransientDownloadError(f"s3 {code} {ecode}")
        # NoSuchKey / 404 / 403 — the object is genuinely missing/forbidden.
        raise _PermanentDownloadError(f"s3 {code} {ecode or e}")
    except (EndpointConnectionError, ConnectTimeoutError, ReadTimeoutError) as e:
        raise _TransientDownloadError(f"s3 conn: {e}")


def download_one(
    item_id: str,
    image_url: str,
    r2_base: str,
    connect_timeout: float = 10.0,
    read_timeout: float = 30.0,
    max_bytes: int = 25 * 1024 * 1024,
) -> DownloadOutcome:
    """Download the -nobg.png and return a PIL Image, or a CLASSIFIED error.

    Transient errors (429/5xx/timeout) are retried with backoff (honoring
    Retry-After) and, if still failing, reported with error_kind='transient' so
    the caller leaves the item for a later run instead of poisoning it. Permanent
    errors (missing object, forbidden, oversized, no key) are reported once with
    error_kind='permanent' so the caller can quarantine the item.
    """
    from PIL import Image
    from io import BytesIO

    key = get_nobg_key(image_url, r2_base)
    if not key:
        return DownloadOutcome(item_id=item_id, error="no_nobg_path", error_kind="permanent")

    use_s3 = s3_enabled()
    started = time.monotonic()
    last_err = "unknown"

    for attempt in range(_MAX_DOWNLOAD_ATTEMPTS):
        retry_after: float | None = None
        try:
            data = _s3_get(key, max_bytes) if use_s3 else _http_get(
                f"{r2_base}/{key}", connect_timeout, read_timeout, max_bytes
            )
            img = Image.open(BytesIO(data)).convert("RGB")
            return DownloadOutcome(
                item_id=item_id,
                image=img,
                bytes_downloaded=len(data),
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except _PermanentDownloadError as e:
            return DownloadOutcome(
                item_id=item_id,
                error=str(e),
                error_kind="permanent",
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )
        except _TransientDownloadError as e:
            last_err, retry_after = str(e), e.retry_after
        except Exception as e:  # decode error, unexpected — treat as permanent
            return DownloadOutcome(
                item_id=item_id,
                error=f"decode/unknown: {e}",
                error_kind="permanent",
                elapsed_ms=int((time.monotonic() - started) * 1000),
            )

        if attempt < _MAX_DOWNLOAD_ATTEMPTS - 1:
            _sleep_backoff(attempt, retry_after)

    return DownloadOutcome(
        item_id=item_id,
        error=f"{last_err} (after {_MAX_DOWNLOAD_ATTEMPTS} attempts)",
        error_kind="transient",
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def _is_missing_nobg(error: str | None) -> bool:
    """A permanent error that means the -nobg.png object genuinely doesn't exist."""
    if not error:
        return False
    e = error.lower()
    return "404" in e or "nosuchkey" in e or "no_nobg_path" in e


@dataclass
class ReadyRow:
    item_id: str
    image: Any  # PIL.Image


def insert_embeddings(
    conn: Any,
    model: str,
    dim: int,
    rows: list[tuple[str, Any]],
) -> list[str]:
    """rows: list of (item_id, numpy 1d float32 length dim). Returns item_ids actually inserted."""
    if not rows:
        return []
    cur = conn.cursor()
    inserted_ids: list[str] = []
    for item_id, vec in rows:
        cur.execute(
            """
            INSERT INTO "ItemEmbedding" ("id", "itemId", "model", "dim", "vector")
            VALUES (gen_random_uuid()::text, %s, %s, %s, %s)
            ON CONFLICT ("itemId", "model") DO NOTHING
            """,
            (item_id, model, dim, vec),
        )
        if cur.rowcount and cur.rowcount > 0:
            inserted_ids.append(item_id)
    conn.commit()
    cur.close()
    return inserted_ids


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed clothing -nobg images into Postgres (pgvector)")
    parser.add_argument("--model", default="clip-vit-b-32-image", choices=list(MODEL_REGISTRY.keys()))
    parser.add_argument("--batch-size", type=int, default=32, help="CLIP encode batch size")
    parser.add_argument("--download-workers", type=int, default=8, help="Parallel HTTP downloads (R2 public URL is rate-limited per IP)")
    parser.add_argument("--work-chunk", type=int, default=64, help="Rows to fetch from DB per outer iteration (smaller = faster feedback)")
    parser.add_argument("--limit", type=int, default=0, help="Max items to process this run (0 = unlimited)")
    parser.add_argument("--dry-run", action="store_true", help="Only print how many rows need work, then exit")
    parser.add_argument("--device", default="mps", choices=["mps", "cpu", "cuda"])
    args = parser.parse_args()

    db_url, r2_base = require_env()
    meta = MODEL_REGISTRY[args.model]
    st_name = meta["st_name"]
    dim = int(meta["dim"])
    if dim != CLIP_DIM:
        print(f"Model dim {dim} must match DB vector(512) for this migration.", file=sys.stderr)
        sys.exit(1)

    log_lock = threading.Lock()
    progress_lock = threading.RLock()
    completed_ids, permanent_ids = load_progress()
    demote_missing = os.environ.get("EMBED_DEMOTE_MISSING_NOBG", "0").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
        "",
    )

    log_line("=" * 60, lock=log_lock)
    log_line(
        f"embed_worker start model={args.model} st={st_name} device={args.device} "
        f"batch={args.batch_size} dl_workers={args.download_workers} work_chunk={args.work_chunk} "
        f"limit={args.limit or 'unlimited'}",
        lock=log_lock,
    )
    log_line("=" * 60, lock=log_lock)

    try:
        import psycopg2
    except ImportError:
        print("Install psycopg2-binary: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    safe_db_url = sanitize_db_url_for_psycopg2(db_url)
    if safe_db_url != db_url:
        log_line("DATABASE_URL had non-libpq query params; sanitized for psycopg2.", lock=log_lock)
    conn = psycopg2.connect(safe_db_url)
    conn.autocommit = False
    try:
        from pgvector.psycopg2 import register_vector

        register_vector(conn)
    except ImportError:
        conn.close()
        print("Install pgvector: pip install pgvector", file=sys.stderr)
        sys.exit(1)

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)::bigint FROM "ClothingItem" ci
            WHERE ci.active = true AND ci."hasNobg" = true
              AND NOT EXISTS (SELECT 1 FROM "ItemEmbedding" ie WHERE ie."itemId" = ci.id AND ie.model = %s)
            """,
            (args.model,),
        )
        remaining_total = int(cur.fetchone()[0])
        cur.close()
        conn.commit()
    except Exception as e:
        conn.close()
        log_line(f"DB error (is pgvector migration applied?): {e}", lock=log_lock)
        raise

    log_line(f"Items needing embedding (DB): {remaining_total}", lock=log_lock)

    if args.dry_run:
        conn.close()
        log_line("Dry run — exiting.", lock=log_lock)
        return

    if remaining_total == 0:
        conn.close()
        log_line("Nothing to do.", lock=log_lock)
        return

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np
    except ImportError:
        print("Install sentence-transformers: pip install sentence-transformers", file=sys.stderr)
        conn.close()
        sys.exit(1)

    device = args.device
    if device == "mps":
        try:
            import torch

            if not torch.backends.mps.is_available():
                log_line("MPS not available, falling back to CPU", lock=log_lock)
                device = "cpu"
        except Exception:
            device = "cpu"

    log_line(f"Loading SentenceTransformer({st_name!r}) on {device!r}…", lock=log_lock)
    model_st = SentenceTransformer(st_name, device=device)

    # Warm up MPS kernels with a dummy image so the first real encode in
    # the chunk loop doesn't appear to hang for 60-90s while Metal JITs.
    try:
        from PIL import Image as _PILImage

        log_line(
            f"Warming up {device.upper()} kernels (first encode JIT-compiles, ~30-90s on cold MPS)…",
            lock=log_lock,
        )
        t_warm = time.monotonic()
        _dummy = _PILImage.new("RGB", (224, 224), color=(128, 128, 128))
        model_st.encode(
            [_dummy],
            batch_size=1,
            convert_to_numpy=True,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        log_line(
            f"Warmup done in {int((time.monotonic() - t_warm) * 1000)}ms; subsequent batches will be fast.",
            lock=log_lock,
        )
    except Exception as e:
        log_line(f"Warmup skipped ({e}); first chunk may take longer.", lock=log_lock)

    stop_requested = threading.Event()
    sigint_count = {"n": 0}

    def on_sigint(_sig: int, _frame: Any) -> None:
        sigint_count["n"] += 1
        if sigint_count["n"] == 1:
            log_line(
                "SIGINT received — cancelling pending downloads. "
                "Process will hard-exit in 5s if it doesn't stop on its own. "
                "Press Ctrl+C again to force-exit immediately.",
                lock=log_lock,
            )
            stop_requested.set()
            # If the main thread is parked in a C call (e.g., model_st.encode
            # on MPS, a blocking C-level lock, or a stuck network read) it
            # will never observe stop_requested. Schedule a hard exit so a
            # single Ctrl+C is always effective within 5 seconds.
            t = threading.Timer(5.0, lambda: os._exit(130))
            t.daemon = True
            t.start()
        else:
            log_line("Second SIGINT — force exiting.", lock=log_lock)
            os._exit(130)

    signal.signal(signal.SIGINT, on_sigint)

    processed_this_run = 0
    outer_limit = args.limit if args.limit > 0 else None

    while not stop_requested.is_set():
        chunk_limit = args.work_chunk
        if outer_limit is not None:
            left = outer_limit - processed_this_run
            if left <= 0:
                break
            chunk_limit = min(chunk_limit, left)

        # Exclude permanently-failed items (missing -nobg.png, etc.) so the batch
        # advances past any poison block instead of re-failing it every run. Cap
        # the list so the anti-join stays cheap even if the set grows large.
        work = fetch_work_batch(
            conn, args.model, chunk_limit, sorted(permanent_ids)[:20000]
        )
        conn.rollback()

        if not work:
            log_line("No more rows to process.", lock=log_lock)
            break

        log_line(f"Fetched {len(work)} rows from DB", lock=log_lock)

        ready: list[ReadyRow | None] = [None] * len(work)
        id_to_index = {work[i][0]: i for i in range(len(work))}
        demote_ids: set[str] = set()  # permanent-404 items to demote after downloads

        chunk_started = time.monotonic()
        n_total = len(work)
        n_done = 0
        n_ok = 0
        n_failed = 0
        sum_bytes = 0
        sum_ms = 0
        chunk_lock = threading.Lock()
        heartbeat_stop = threading.Event()

        def heartbeat() -> None:
            while not heartbeat_stop.wait(15.0):
                with chunk_lock:
                    in_flight = n_total - n_done
                    elapsed = time.monotonic() - chunk_started
                    avg_ms = (sum_ms / n_ok) if n_ok else 0
                    rate = (n_ok / elapsed) if elapsed > 0 else 0
                    mb = sum_bytes / (1024 * 1024)
                log_line(
                    f"download heartbeat: done={n_done}/{n_total} ok={n_ok} failed={n_failed} "
                    f"in_flight={in_flight} ~{rate:.1f} ok/s avg={avg_ms:.0f}ms total={mb:.1f}MB",
                    lock=log_lock,
                )

        hb_thread = threading.Thread(target=heartbeat, daemon=True)
        hb_thread.start()

        ex = ThreadPoolExecutor(max_workers=args.download_workers)
        try:
            futures = {
                ex.submit(download_one, item_id, image_url, r2_base): item_id
                for item_id, image_url in work
            }
            for fut in as_completed(futures):
                if stop_requested.is_set():
                    log_line("SIGINT during downloads — cancelling pending futures.", lock=log_lock)
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                item_id = futures[fut]
                try:
                    outcome: DownloadOutcome = fut.result()
                except Exception as e:
                    log_line(f"download future error {item_id}: {e}", lock=log_lock)
                    idx = id_to_index[item_id]
                    ready[idx] = None
                    with chunk_lock:
                        n_done += 1
                        n_failed += 1
                    continue

                idx = id_to_index[outcome.item_id]

                with chunk_lock:
                    n_done += 1
                    if outcome.error:
                        n_failed += 1
                    else:
                        n_ok += 1
                        sum_bytes += outcome.bytes_downloaded
                        sum_ms += outcome.elapsed_ms
                    done_now = n_done

                if outcome.error:
                    ready[idx] = None
                    kind = outcome.error_kind or "transient"
                    log_line(
                        f"download {kind}-fail [{n_done}/{n_total}] {outcome.item_id} "
                        f"({outcome.elapsed_ms}ms): {outcome.error}",
                        lock=log_lock,
                    )
                    # Only PERMANENT failures are quarantined (persisted + excluded
                    # from future work). Transient failures (429/5xx/timeout) are
                    # left un-embedded and retried on the next chunk/run — persisting
                    # them would poison good items that merely hit a rate limit.
                    if kind == "permanent":
                        with progress_lock:
                            permanent_ids.add(outcome.item_id)
                            append_history(
                                {
                                    "itemId": outcome.item_id,
                                    "model": args.model,
                                    "status": "failed",
                                    "kind": "permanent",
                                    "error": outcome.error,
                                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                }
                            )
                            save_progress(completed_ids, permanent_ids)
                        if _is_missing_nobg(outcome.error):
                            demote_ids.add(outcome.item_id)
                    continue

                ready[idx] = ReadyRow(item_id=outcome.item_id, image=outcome.image)

                # Per-N progress so the user sees life
                if done_now <= 5 or done_now % 5 == 0 or done_now == n_total:
                    log_line(
                        f"download ok [{done_now}/{n_total}] {outcome.item_id} "
                        f"{outcome.bytes_downloaded // 1024}KB in {outcome.elapsed_ms}ms",
                        lock=log_lock,
                    )
        finally:
            heartbeat_stop.set()
            ex.shutdown(wait=False, cancel_futures=True)

        if stop_requested.is_set():
            break

        # Optional source-of-truth heal: demote items whose -nobg.png is genuinely
        # missing (hard 404) so the nobg reconcile re-generates the file, instead of
        # them sitting in the backlog forever. OFF by default (the worker's reconcile
        # loop already demotes on a definitive 404); enable EMBED_DEMOTE_MISSING_NOBG=1
        # to also do it inline here for faster cleanup.
        if demote_missing and demote_ids:
            try:
                dcur = conn.cursor()
                dcur.execute(
                    'UPDATE "ClothingItem" SET "hasNobg" = false, "updatedAt" = NOW() '
                    'WHERE id = ANY(%s::text[]) AND "hasNobg" = true',
                    (sorted(demote_ids),),
                )
                demoted = dcur.rowcount
                conn.commit()
                dcur.close()
                log_line(
                    f"demoted {demoted} missing-nobg item(s) → hasNobg=false (reconcile will regenerate)",
                    lock=log_lock,
                )
            except Exception as e:
                conn.rollback()
                log_line(f"demote failed (non-fatal): {e}", lock=log_lock)

        # Encode in batches (preserve order for logging)
        to_encode: list[ReadyRow] = [r for r in ready if r is not None]
        if not to_encode:
            log_line("Chunk had no successful downloads; continuing.", lock=log_lock)
            processed_this_run += len(work)
            if outer_limit is not None and processed_this_run >= outer_limit:
                log_line("Reached --limit for this run.", lock=log_lock)
                break
            continue

        # Encode + commit per encode-batch so we don't lose a whole chunk
        # if the process is interrupted mid-encode (e.g., laptop sleeps).
        chunk_total_encoded = 0
        chunk_total_inserted = 0
        n_encode_batches = (len(to_encode) + args.batch_size - 1) // args.batch_size
        log_line(
            f"Encoding {len(to_encode)} images on device={device} "
            f"({n_encode_batches} batch(es) of up to {args.batch_size})…",
            lock=log_lock,
        )
        for b in range(0, len(to_encode), args.batch_size):
            batch_idx = b // args.batch_size + 1
            batch = to_encode[b : b + args.batch_size]
            log_line(
                f"  encode batch [{batch_idx}/{n_encode_batches}] starting size={len(batch)}…",
                lock=log_lock,
            )
            pil_list = [r.image for r in batch]
            t_enc = time.monotonic()
            # MPS is known to occasionally wedge inside encode() with no
            # observable progress. Arm a watchdog so the outer restart loop
            # gets a chance to recover instead of silently sleeping forever.
            encode_timeout_s = max(60.0, len(pil_list) * 5.0)

            def _encode_watchdog_fire(item_ids: list[str] = [r.item_id for r in batch]) -> None:
                log_line(
                    f"  encode watchdog FIRED after {encode_timeout_s:.0f}s "
                    f"(batch_size={len(item_ids)}). Force-exiting (124) so the "
                    f"outer restart loop can resume. Stuck items: {item_ids[:3]}…",
                    lock=log_lock,
                )
                os._exit(124)

            encode_watchdog = threading.Timer(encode_timeout_s, _encode_watchdog_fire)
            encode_watchdog.daemon = True
            encode_watchdog.start()
            try:
                vecs = model_st.encode(
                    pil_list,
                    batch_size=len(pil_list),
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    show_progress_bar=False,
                )
            finally:
                encode_watchdog.cancel()
            enc_ms = int((time.monotonic() - t_enc) * 1000)

            batch_rows: list[tuple[str, Any]] = []
            for i, row in enumerate(batch):
                vec = np.asarray(vecs[i], dtype=np.float32).reshape(-1)
                if vec.shape[0] != dim:
                    log_line(f"dim mismatch {row.item_id}: got {vec.shape[0]} expected {dim}", lock=log_lock)
                    continue
                batch_rows.append((row.item_id, vec))

            t_db = time.monotonic()
            inserted_ids = insert_embeddings(conn, args.model, dim, batch_rows)
            db_ms = int((time.monotonic() - t_db) * 1000)

            chunk_total_encoded += len(batch_rows)
            chunk_total_inserted += len(inserted_ids)

            now_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            with progress_lock:
                for item_id in inserted_ids:
                    completed_ids.add(item_id)
                    append_history(
                        {"itemId": item_id, "model": args.model, "status": "success", "ts": now_ts}
                    )
                save_progress(completed_ids, permanent_ids)

            log_line(
                f"  encode batch [{batch_idx}/{n_encode_batches}] done size={len(batch)} "
                f"encoded_ok={len(batch_rows)} inserted={len(inserted_ids)} "
                f"encode={enc_ms}ms db={db_ms}ms",
                lock=log_lock,
            )

        processed_this_run += len(work)
        log_line(
            f"Chunk done: encoded {chunk_total_encoded} inserted={chunk_total_inserted} "
            f"run_rows_fetched={processed_this_run}",
            lock=log_lock,
        )

        if outer_limit is not None and processed_this_run >= outer_limit:
            log_line("Reached --limit for this run.", lock=log_lock)
            break

    conn.close()
    log_line("embed_worker finished.", lock=log_lock)


if __name__ == "__main__":
    main()
