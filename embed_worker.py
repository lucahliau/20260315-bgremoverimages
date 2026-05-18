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


def get_nobg_url(original_url: str, r2_base_url: str) -> str | None:
    """Mirror backend src/lib/images.ts getNobgUrl."""
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
    nobg_path = path[: last_slash + 1] + name_without_ext + "-nobg.png"
    return f"{base}/{nobg_path}"


def load_progress() -> tuple[set[str], set[str]]:
    if not PROGRESS_FILE.exists():
        return set(), set()
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        completed = set(str(x) for x in (data.get("completed") or []))
        failed = set(str(x) for x in (data.get("failed") or []))
        return completed, failed
    except (json.JSONDecodeError, OSError):
        return set(), set()


def save_progress(completed: set[str], failed: set[str]) -> None:
    """Write progress snapshot. Caller MUST already hold the progress lock."""
    PROGRESS_FILE.write_text(
        json.dumps(
            {"completed": sorted(completed), "failed": sorted(failed)},
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


def fetch_work_batch(conn: Any, model: str, limit: int) -> list[tuple[str, str]]:
    """Return list of (item_id, imageUrl) needing embeddings (DB is source of truth)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ci.id::text, ci."imageUrl"
        FROM "ClothingItem" ci
        WHERE ci.active = true AND ci."hasNobg" = true
          AND NOT EXISTS (
            SELECT 1 FROM "ItemEmbedding" ie
            WHERE ie."itemId" = ci.id AND ie.model = %s
          )
        ORDER BY ci."createdAt" DESC
        LIMIT %s
        """,
        (model, limit),
    )
    rows = cur.fetchall()
    cur.close()
    return [(r[0], r[1]) for r in rows]


@dataclass
class DownloadOutcome:
    item_id: str
    image: Any = None  # PIL.Image when ok
    error: str | None = None
    bytes_downloaded: int = 0
    elapsed_ms: int = 0


def download_one(
    item_id: str,
    image_url: str,
    r2_base: str,
    connect_timeout: float = 10.0,
    read_timeout: float = 30.0,
    max_bytes: int = 25 * 1024 * 1024,
) -> DownloadOutcome:
    """Download -nobg.png and return PIL Image or error.

    Uses a strict connect+read timeout so hung connections fail fast
    rather than blocking the whole chunk.
    """
    try:
        import requests
        from PIL import Image
        from io import BytesIO
    except ImportError as e:
        return DownloadOutcome(item_id=item_id, error=str(e))

    nobg = get_nobg_url(image_url, r2_base)
    if not nobg:
        return DownloadOutcome(item_id=item_id, error="no_nobg_path")

    started = time.monotonic()
    try:
        with requests.get(nobg, timeout=(connect_timeout, read_timeout), stream=True) as r:
            r.raise_for_status()
            buf = bytearray()
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                buf.extend(chunk)
                if len(buf) > max_bytes:
                    return DownloadOutcome(
                        item_id=item_id,
                        error=f"oversized: {len(buf)} > {max_bytes}",
                        elapsed_ms=int((time.monotonic() - started) * 1000),
                    )
            data = bytes(buf)
        img = Image.open(BytesIO(data)).convert("RGB")
        return DownloadOutcome(
            item_id=item_id,
            image=img,
            bytes_downloaded=len(data),
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
    except Exception as e:
        return DownloadOutcome(
            item_id=item_id,
            error=str(e),
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )


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
    completed_ids, failed_ids = load_progress()

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

        work = fetch_work_batch(conn, args.model, chunk_limit)
        conn.rollback()

        if not work:
            log_line("No more rows to process.", lock=log_lock)
            break

        log_line(f"Fetched {len(work)} rows from DB", lock=log_lock)

        ready: list[ReadyRow | None] = [None] * len(work)
        id_to_index = {work[i][0]: i for i in range(len(work))}

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
                    log_line(
                        f"download failed [{n_done}/{n_total}] {outcome.item_id} "
                        f"({outcome.elapsed_ms}ms): {outcome.error}",
                        lock=log_lock,
                    )
                    with progress_lock:
                        failed_ids.add(outcome.item_id)
                        append_history(
                            {
                                "itemId": outcome.item_id,
                                "model": args.model,
                                "status": "failed",
                                "error": outcome.error,
                                "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            }
                        )
                        save_progress(completed_ids, failed_ids)
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
                save_progress(completed_ids, failed_ids)

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
