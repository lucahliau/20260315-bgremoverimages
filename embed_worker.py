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


def save_progress(completed: set[str], failed: set[str], lock: threading.Lock) -> None:
    with lock:
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


def download_one(
    item_id: str,
    image_url: str,
    r2_base: str,
    timeout: int = 120,
) -> DownloadOutcome:
    """Download -nobg.png and return PIL Image or error."""
    try:
        import requests
        from PIL import Image
        from io import BytesIO
    except ImportError as e:
        return DownloadOutcome(item_id=item_id, error=str(e))

    nobg = get_nobg_url(image_url, r2_base)
    if not nobg:
        return DownloadOutcome(item_id=item_id, error="no_nobg_path")

    try:
        r = requests.get(nobg, timeout=timeout, stream=True)
        r.raise_for_status()
        data = r.content
        img = Image.open(BytesIO(data)).convert("RGB")
        return DownloadOutcome(item_id=item_id, image=img)
    except Exception as e:
        return DownloadOutcome(item_id=item_id, error=str(e))


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
    parser.add_argument("--download-workers", type=int, default=12, help="Parallel HTTP downloads")
    parser.add_argument("--work-chunk", type=int, default=256, help="Rows to fetch from DB per outer iteration")
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
    progress_lock = threading.Lock()
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

    conn = psycopg2.connect(db_url)
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

    stop_requested = threading.Event()

    def on_sigint(_sig: int, _frame: Any) -> None:
        log_line("SIGINT — will finish current batch then exit.", lock=log_lock)
        stop_requested.set()

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

        # Parallel download
        ready: list[ReadyRow | None] = [None] * len(work)
        id_to_index = {work[i][0]: i for i in range(len(work))}

        with ThreadPoolExecutor(max_workers=args.download_workers) as ex:
            futures = {
                ex.submit(download_one, item_id, image_url, r2_base): item_id
                for item_id, image_url in work
            }
            for fut in as_completed(futures):
                item_id = futures[fut]
                try:
                    outcome: DownloadOutcome = fut.result()
                except Exception as e:
                    log_line(f"download future error {item_id}: {e}", lock=log_lock)
                    idx = id_to_index[item_id]
                    ready[idx] = None
                    continue
                idx = id_to_index[outcome.item_id]
                if outcome.error:
                    ready[idx] = None
                    log_line(f"download failed {outcome.item_id}: {outcome.error}", lock=log_lock)
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
                    save_progress(completed_ids, failed_ids, progress_lock)
                    continue
                ready[idx] = ReadyRow(item_id=outcome.item_id, image=outcome.image)

        # Encode in batches (preserve order for logging)
        to_encode: list[ReadyRow] = [r for r in ready if r is not None]
        if not to_encode:
            log_line("Chunk had no successful downloads; continuing.", lock=log_lock)
            processed_this_run += len(work)
            if outer_limit is not None and processed_this_run >= outer_limit:
                log_line("Reached --limit for this run.", lock=log_lock)
                break
            continue

        all_embeddings: list[tuple[str, Any]] = []
        for b in range(0, len(to_encode), args.batch_size):
            batch = to_encode[b : b + args.batch_size]
            pil_list = [r.image for r in batch]
            vecs = model_st.encode(
                pil_list,
                batch_size=len(pil_list),
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            for i, row in enumerate(batch):
                vec = np.asarray(vecs[i], dtype=np.float32).reshape(-1)
                if vec.shape[0] != dim:
                    log_line(f"dim mismatch {row.item_id}: got {vec.shape[0]} expected {dim}", lock=log_lock)
                    continue
                all_embeddings.append((row.item_id, vec))

        inserted_ids = insert_embeddings(conn, args.model, dim, all_embeddings)

        now_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with progress_lock:
            for item_id in inserted_ids:
                completed_ids.add(item_id)
                append_history(
                    {"itemId": item_id, "model": args.model, "status": "success", "ts": now_ts}
                )
            save_progress(completed_ids, failed_ids, progress_lock)

        processed_this_run += len(work)
        log_line(
            f"Chunk done: encoded {len(all_embeddings)} inserted={len(inserted_ids)} "
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
