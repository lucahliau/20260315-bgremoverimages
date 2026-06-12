#!/usr/bin/env python3
"""
tag_clothing_worker.py — Local zero-shot CLIP tagging of ClothingItems.

Wires the dormant `fashion_tagger.py` (Clothes/scraping/enrichment/) into the
post-upload pipeline. Mirrors the structure of embed_worker.py in this same
directory, so it runs in the same operational style:

    - polls Postgres for ClothingItem rows that need tagging
    - downloads the -nobg.png from R2_PUBLIC_URL (same path rule as backend)
    - runs OpenCLIP zero-shot scoring against fixed label sets
    - writes category / colors / tags back to the row
    - resumable via a JSON progress file
    - hot path on Apple Silicon: MPS device, batched image encodes, and
      pre-computed text embeddings (this fixes a 50× wasted-compute bug in
      the original fashion_tagger.py which re-encoded the same labels per
      image).

Why a separate worker instead of doing this inline at upload time:
    - the upload path lives on Railway with no GPU; running CLIP there would
      either be slow CPU inference or another paid API call. Doing it here
      keeps it free, on the user's M4.
    - the upload path must not block on a 2nd model load. Decoupling lets us
      batch tag in the background.
    - resumability: a crash mid-batch never loses progress.

Usage:
    source .env   # DATABASE_URL, R2_PUBLIC_URL required
    python3 tag_clothing_worker.py [--batch-size 16] [--download-workers 8]
                                   [--limit N] [--dry-run] [--device mps|cpu]
                                   [--rescore]            # also re-tag items
                                                          # that already have
                                                          # category set

Setup: same venv as embed_worker.py (open_clip_torch + torch + psycopg2-binary
       + Pillow + requests + numpy).
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

# Reuse embed_worker's libpq URL sanitization + nobg-URL builder so this worker
# behaves identically with respect to the prod Supabase URL and R2 path rules.
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from embed_worker import (  # type: ignore[import-not-found]
    get_nobg_url,
    sanitize_db_url_for_psycopg2,
)

PROGRESS_FILE = SCRIPT_DIR / "tag-clothing-progress.json"
HISTORY_FILE = SCRIPT_DIR / "tag-clothing-history.jsonl"
LOG_FILE = SCRIPT_DIR / "tag-clothing.log"

# Label vocabularies — taken from Clothes/scraping/enrichment/fashion_tagger.py
# but tightened to match the backend's ClothingItem schema (lowercase singular
# nouns, no spaces in tags).
CATEGORIES = [
    "t-shirt", "shirt", "blouse", "sweater", "hoodie", "jacket", "coat",
    "jeans", "trousers", "shorts", "skirt", "dress",
    "sneakers", "boots", "sandals", "heels",
    "bag", "hat", "scarf", "jewelry", "watch", "sunglasses",
]
COLORS = [
    "black", "white", "grey", "navy", "blue", "red", "pink",
    "green", "yellow", "orange", "purple", "brown", "beige", "cream",
]
PATTERNS = [
    "solid", "striped", "plaid", "floral", "polka-dot",
    "geometric", "abstract", "animal-print", "camo",
]
STYLES = [
    "casual", "formal", "streetwear", "athletic", "bohemian",
    "minimalist", "vintage", "preppy", "punk", "elegant",
]

CATEGORY_THRESHOLD = 0.18
COLOR_THRESHOLD = 0.20
PATTERN_THRESHOLD = 0.20
STYLE_THRESHOLD = 0.20


def log_line(msg: str, lock: threading.Lock | None = None) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"

    def _write() -> None:
        print(line, flush=True)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    if lock is not None:
        with lock:
            _write()
    else:
        _write()


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
    """Caller MUST hold the progress lock."""
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


def fetch_work_batch(
    conn: Any,
    limit: int,
    skip_ids: set[str],
    rescore: bool,
) -> list[tuple[str, str, str | None, list[str] | None, list[str] | None]]:
    """Find items that need tagging.

    Selection rule:
      - active = true AND hasNobg = true  (we tag on the bg-removed image)
      - if --rescore: include everything not in skip_ids
      - else: only items missing tags or category
    """
    cur = conn.cursor()
    where = "WHERE ci.active = true AND ci.\"hasNobg\" = true"
    if not rescore:
        # "Needs tagging" = no category OR no tags. We do NOT touch items with
        # non-trivial existing tags (the user values their existing 95%-good
        # data).
        where += (
            " AND (ci.\"category\" IS NULL OR ci.\"category\" = ''"
            " OR ci.\"tags\" IS NULL OR cardinality(ci.\"tags\") = 0)"
        )
    cur.execute(
        f"""
        SELECT ci.id::text, ci."imageUrl", ci."category", ci."colors", ci."tags"
        FROM "ClothingItem" ci
        {where}
        ORDER BY ci."createdAt" DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    cur.close()
    # Filter completed/failed in Python (faster than NOT IN with a giant list).
    return [r for r in rows if r[0] not in skip_ids]


@dataclass
class DownloadOutcome:
    item_id: str
    image: Any = None
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
                        error=f"oversized: {len(buf)}",
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


# ----------------------------------------------------------------------------
# CLIP model + pre-computed text vectors.
# ----------------------------------------------------------------------------
_model = None
_preprocess = None
_tokenizer = None
_device = None
_text_vectors: dict[str, "Any"] = {}  # group -> torch.Tensor [N_labels, dim]
_text_labels: dict[str, list[str]] = {}


def load_model(device_pref: str) -> None:
    global _model, _preprocess, _tokenizer, _device
    if _model is not None:
        return
    import torch
    import open_clip

    if device_pref == "mps" and getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        _device = torch.device("mps")
    elif device_pref == "cuda" and torch.cuda.is_available():
        _device = torch.device("cuda")
    else:
        _device = torch.device("cpu")

    _model, _, _preprocess = open_clip.create_model_and_transforms(
        "ViT-B-32", pretrained="laion2b_s34b_b79k"
    )
    _model = _model.to(_device).eval()
    _tokenizer = open_clip.get_tokenizer("ViT-B-32")


def precompute_text_vectors() -> None:
    """Encode every label exactly once and keep the result on-device.

    This avoids the per-image text encoding the original fashion_tagger.py
    did (50 redundant text encodes per image = >95% of inference time).
    """
    import torch

    for group_name, labels in (
        ("category", CATEGORIES),
        ("color", COLORS),
        ("pattern", PATTERNS),
        ("style", STYLES),
    ):
        prompts = [f"a photo of a {label}" if group_name == "category" else f"a photo of a {label} clothing item" for label in labels]
        tokens = _tokenizer(prompts).to(_device)
        with torch.no_grad():
            vecs = _model.encode_text(tokens)
            vecs = vecs / vecs.norm(dim=-1, keepdim=True)
        _text_vectors[group_name] = vecs
        _text_labels[group_name] = labels


def encode_images(images: list[Any]) -> "Any":
    import torch

    if not images:
        return None
    tensors = torch.stack([_preprocess(img) for img in images]).to(_device)
    with torch.no_grad():
        feats = _model.encode_image(tensors)
        feats = feats / feats.norm(dim=-1, keepdim=True)
    return feats  # [B, D]


def score_batch(image_feats: "Any") -> list[dict[str, Any]]:
    """For each image in the batch, return its top labels per group."""
    import torch

    results = []
    for i in range(image_feats.shape[0]):
        v = image_feats[i:i + 1]
        out: dict[str, Any] = {}
        for group_name in ("category", "color", "pattern", "style"):
            scores = (v @ _text_vectors[group_name].T).squeeze(0)
            scores_list = scores.detach().cpu().tolist()
            ranked = sorted(
                zip(_text_labels[group_name], scores_list),
                key=lambda x: x[1],
                reverse=True,
            )
            out[group_name] = ranked
        results.append(out)
    return results


def pick_tags(
    ranked: dict[str, list[tuple[str, float]]],
) -> dict[str, Any]:
    """Translate per-group rankings into the fields we'll write to the DB."""
    cat_top = ranked["category"][0]
    category = cat_top[0] if cat_top[1] >= CATEGORY_THRESHOLD else None

    colors = [c for c, s in ranked["color"][:3] if s >= COLOR_THRESHOLD]
    patterns = [p for p, s in ranked["pattern"][:2] if s >= PATTERN_THRESHOLD]
    styles = [st for st, s in ranked["style"][:3] if s >= STYLE_THRESHOLD]

    # Tags = patterns + styles (plus the top category for searchability).
    tags = []
    if category:
        tags.append(category)
    tags.extend(patterns)
    tags.extend(styles)
    # Dedupe while preserving order.
    seen = set()
    deduped = []
    for t in tags:
        if t in seen:
            continue
        seen.add(t)
        deduped.append(t)

    return {
        "category": category,
        "colors": colors,
        "tags": deduped,
        "scores": {
            "category": ranked["category"][0][1] if ranked["category"] else None,
            "top_pattern": ranked["pattern"][0][1] if ranked["pattern"] else None,
            "top_style": ranked["style"][0][1] if ranked["style"] else None,
        },
    }


def update_item(
    conn: Any,
    item_id: str,
    current_category: str | None,
    current_colors: list[str] | None,
    current_tags: list[str] | None,
    inferred: dict[str, Any],
    rescore: bool,
) -> None:
    """Write inferred tags. NEVER clobber existing good data unless --rescore."""
    cat = current_category
    if not cat and inferred.get("category"):
        cat = inferred["category"]
    if rescore and inferred.get("category"):
        cat = inferred["category"]

    cols = current_colors or []
    if not cols and inferred.get("colors"):
        cols = inferred["colors"]
    if rescore and inferred.get("colors"):
        cols = inferred["colors"]

    existing_tags = current_tags or []
    merged_tags = list(existing_tags)
    for t in inferred.get("tags", []):
        if t not in merged_tags:
            merged_tags.append(t)
    if rescore:
        merged_tags = inferred.get("tags", []) or existing_tags

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE "ClothingItem"
        SET "category" = %s,
            "colors" = %s,
            "tags" = %s
        WHERE "id" = %s
        """,
        (cat, cols, merged_tags, item_id),
    )
    conn.commit()
    cur.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Zero-shot CLIP tagging of ClothingItems")
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--download-workers", type=int, default=8)
    parser.add_argument("--work-chunk", type=int, default=64)
    parser.add_argument("--limit", type=int, default=0, help="0 = unlimited")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--rescore", action="store_true",
                        help="Re-tag items even if they already have category/colors")
    parser.add_argument("--device", default="mps", choices=["mps", "cpu", "cuda"])
    args = parser.parse_args()

    db_url, r2_base = require_env()
    log_lock = threading.Lock()
    progress_lock = threading.RLock()
    completed_ids, failed_ids = load_progress()

    log_line("=" * 60, lock=log_lock)
    log_line(
        f"tag_clothing_worker start device={args.device} batch={args.batch_size} "
        f"dl_workers={args.download_workers} work_chunk={args.work_chunk} "
        f"limit={args.limit or 'unlimited'} rescore={args.rescore}",
        lock=log_lock,
    )
    log_line("=" * 60, lock=log_lock)

    try:
        import psycopg2
    except ImportError:
        print("Install psycopg2-binary: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(sanitize_db_url_for_psycopg2(db_url))
    conn.autocommit = False

    if args.dry_run:
        rows = fetch_work_batch(conn, 10_000, completed_ids | failed_ids, args.rescore)
        log_line(f"DRY RUN: {len(rows)} items would be tagged", lock=log_lock)
        return

    log_line("Loading CLIP model…", lock=log_lock)
    load_model(args.device)
    precompute_text_vectors()
    log_line(f"CLIP loaded on {_device}; pre-computed text vectors for "
             f"{sum(len(v) for v in _text_labels.values())} labels.", lock=log_lock)

    # Graceful shutdown.
    stop = {"flag": False}

    def handle_sig(*_):
        log_line("Signal received — finishing current batch then exiting.", lock=log_lock)
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    processed_this_run = 0
    started_at = time.monotonic()

    while not stop["flag"]:
        if args.limit and processed_this_run >= args.limit:
            break
        skip = completed_ids | failed_ids
        remaining_budget = args.limit - processed_this_run if args.limit else args.work_chunk
        chunk_size = min(args.work_chunk, remaining_budget) if args.limit else args.work_chunk
        rows = fetch_work_batch(conn, chunk_size, skip, args.rescore)
        if not rows:
            log_line("No more items to tag — exiting.", lock=log_lock)
            break

        # Parallel downloads.
        downloaded: list[tuple[tuple[str, str, str | None, list[str] | None, list[str] | None], Any]] = []
        with ThreadPoolExecutor(max_workers=args.download_workers) as ex:
            futures = {
                ex.submit(download_one, r[0], r[1], r2_base): r
                for r in rows
            }
            for fut in as_completed(futures):
                row = futures[fut]
                outcome = fut.result()
                if outcome.error or outcome.image is None:
                    with progress_lock:
                        failed_ids.add(row[0])
                        save_progress(completed_ids, failed_ids)
                    append_history({
                        "item_id": row[0],
                        "stage": "download",
                        "ok": False,
                        "error": outcome.error,
                        "ts": time.time(),
                    })
                    continue
                downloaded.append((row, outcome.image))

        if not downloaded:
            continue

        # Encode + score in batches.
        for start in range(0, len(downloaded), args.batch_size):
            sub = downloaded[start:start + args.batch_size]
            try:
                feats = encode_images([img for _, img in sub])
                ranked_list = score_batch(feats)
            except Exception as e:
                log_line(f"Encode/score failed: {e}", lock=log_lock)
                with progress_lock:
                    for (row, _) in sub:
                        failed_ids.add(row[0])
                    save_progress(completed_ids, failed_ids)
                continue

            # Per-item DB write.
            for (row, _img), ranked in zip(sub, ranked_list):
                item_id, image_url, cur_cat, cur_cols, cur_tags = row
                inferred = pick_tags(ranked)
                try:
                    update_item(conn, item_id, cur_cat, cur_cols, cur_tags, inferred, args.rescore)
                    with progress_lock:
                        completed_ids.add(item_id)
                        save_progress(completed_ids, failed_ids)
                    append_history({
                        "item_id": item_id,
                        "stage": "tagged",
                        "ok": True,
                        "category": inferred["category"],
                        "colors": inferred["colors"],
                        "tags": inferred["tags"],
                        "scores": inferred["scores"],
                        "ts": time.time(),
                    })
                    processed_this_run += 1
                except Exception as e:
                    conn.rollback()
                    log_line(f"DB write failed for {item_id}: {e}", lock=log_lock)
                    with progress_lock:
                        failed_ids.add(item_id)
                        save_progress(completed_ids, failed_ids)
                    append_history({
                        "item_id": item_id,
                        "stage": "db_write",
                        "ok": False,
                        "error": str(e),
                        "ts": time.time(),
                    })

        elapsed = time.monotonic() - started_at
        rate = processed_this_run / elapsed if elapsed > 0 else 0
        log_line(
            f"progress this_run={processed_this_run} rate={rate:.2f}/s "
            f"completed_total={len(completed_ids)} failed_total={len(failed_ids)}",
            lock=log_lock,
        )

    conn.close()
    log_line(f"DONE — processed {processed_this_run} items this run.", lock=log_lock)


if __name__ == "__main__":
    main()
