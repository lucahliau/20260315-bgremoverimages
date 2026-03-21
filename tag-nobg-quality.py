#!/usr/bin/env python3
"""
tag-nobg-quality.py — Scan R2 for *-nobg.png, flag blank (alpha) or person (YOLOv8),
rename bad assets to *-nobg__REJECT-{blank|person}.png.

Requires: pip install boto3 pillow ultralytics numpy
Env (same as remove-bg): R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME

Usage: source .env && python3 tag-nobg-quality.py [--dry-run] [--limit N] [--parallel 8] ...
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ── Paths (next to this script) ─────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "tag-nobg-quality-progress.json"
HISTORY_FILE = SCRIPT_DIR / "tag-nobg-quality-history.jsonl"
LOG_FILE = SCRIPT_DIR / "tag-nobg-quality.log"
TMP_DIR = SCRIPT_DIR / "tmp-tag-nobg"


def log_line(msg: str, also_print: bool = True, _lock: threading.Lock | None = None) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    def _write() -> None:
        if also_print:
            print(line)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    if _lock is not None:
        with _lock:
            _write()
    else:
        _write()


def load_progress() -> tuple[list[str], list[str]]:
    if not PROGRESS_FILE.exists():
        return [], []
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        return list(data.get("completed") or []), list(data.get("failed") or [])
    except (json.JSONDecodeError, OSError):
        return [], []


def save_progress(completed: list[str], failed: list[str]) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(
        json.dumps({"completed": completed, "failed": failed}, indent=2),
        encoding="utf-8",
    )


def append_history(record: dict[str, Any]) -> None:
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def require_env() -> dict[str, str]:
    keys = [
        "R2_ACCOUNT_ID",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET_NAME",
    ]
    out: dict[str, str] = {}
    for k in keys:
        v = os.environ.get(k)
        if not v:
            print(f"Missing env var: {k}", file=sys.stderr)
            sys.exit(1)
        out[k] = v
    return out


def make_s3_client(env: dict[str, str]):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        print("Install boto3: pip install boto3", file=sys.stderr)
        sys.exit(1)

    cfg = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    )
    return boto3.client(
        "s3",
        endpoint_url=f"https://{env['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=env["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=env["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        config=cfg,
    )


def list_nobg_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents") or []:
            k = obj.get("Key") or ""
            if is_canonical_nobg(k):
                keys.append(k)
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return keys


def is_canonical_nobg(key: str) -> bool:
    """True for ...-nobg.png that is not already a __REJECT rename."""
    if not key.endswith("-nobg.png"):
        return False
    if "-nobg__REJECT-" in key:
        return False
    return True


def reject_key_from_original(nobg_key: str, reason: str) -> str:
    """products/a/foo-nobg.png -> products/a/foo-nobg__REJECT-{reason}.png"""
    if not nobg_key.endswith("-nobg.png"):
        raise ValueError(f"not a -nobg.png key: {nobg_key}")
    base = nobg_key[: -len("-nobg.png")]
    return f"{base}-nobg__REJECT-{reason}.png"


def download_object(s3, bucket: str, key: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(dest))


def rename_object(s3, bucket: str, src: str, dst: str) -> None:
    s3.copy_object(
        Bucket=bucket,
        Key=dst,
        CopySource={"Bucket": bucket, "Key": src},
        ContentType="image/png",
    )
    s3.delete_object(Bucket=bucket, Key=src)


def alpha_opaque_fraction(path: Path, alpha_thresh: int) -> float:
    from PIL import Image
    import numpy as np

    img = Image.open(path)
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    a = np.asarray(img.getchannel("A"), dtype=np.uint8)
    n = int(np.count_nonzero(a > alpha_thresh))
    return n / float(a.size)


def composite_white_rgba(path: Path):
    from PIL import Image

    img = Image.open(path).convert("RGBA")
    bg = Image.new("RGB", img.size, (255, 255, 255))
    bg.paste(img, mask=img.split()[3])
    return bg


def detect_person_yolo(
    rgb_image,
    model,
    conf: float,
    min_box_area_frac: float,
    inference_lock: threading.Lock | None = None,
) -> bool:
    import numpy as np

    def _infer() -> bool:
        w, h = rgb_image.size
        area_img = float(w * h)
        results = model.predict(
            source=rgb_image, classes=[0], conf=conf, verbose=False
        )
        if not results or len(results[0].boxes) == 0:
            return False
        for box in results[0].boxes:
            xyxy = box.xyxy[0].cpu().numpy()
            bw = float(xyxy[2] - xyxy[0])
            bh = float(xyxy[3] - xyxy[1])
            if bw * bh / area_img >= min_box_area_frac:
                return True
        return False

    if inference_lock is not None:
        with inference_lock:
            return _infer()
    return _infer()


@dataclass
class RunConfig:
    dry_run: bool
    limit: int | None
    prefix: str
    min_alpha_fraction: float
    alpha_thresh: int
    person_conf: float
    min_box_area_frac: float


def process_one(
    s3,
    bucket: str,
    key: str,
    cfg: RunConfig,
    model,
    inference_lock: threading.Lock | None = None,
) -> str:
    """
    Returns outcome: ok | reject-blank | reject-person
    (raises RuntimeError on download/analysis failure)
    """
    with tempfile.TemporaryDirectory(dir=TMP_DIR) as td:
        tdir = Path(td)
        local = tdir / "input.png"
        try:
            download_object(s3, bucket, key, local)
        except Exception as e:
            raise RuntimeError(f"download failed: {e}") from e

        try:
            frac = alpha_opaque_fraction(local, cfg.alpha_thresh)
        except Exception as e:
            raise RuntimeError(f"alpha analysis failed: {e}") from e

        if frac < cfg.min_alpha_fraction:
            return "reject-blank"

        if model is None:
            return "ok"

        try:
            rgb = composite_white_rgba(local)
        except Exception as e:
            raise RuntimeError(f"composite failed: {e}") from e

        if detect_person_yolo(
            rgb,
            model,
            cfg.person_conf,
            cfg.min_box_area_frac,
            inference_lock,
        ):
            return "reject-person"
        return "ok"


def run_key_pipeline(
    key: str,
    cfg: RunConfig,
    s3,
    bucket: str,
    model,
    inference_lock: threading.Lock | None,
) -> dict[str, Any]:
    """Run download/analyze/rename for one key. Returns a result dict for handle_result."""
    try:
        outcome = process_one(s3, bucket, key, cfg, model, inference_lock)
    except Exception as e:
        return {"key": key, "outcome": "error", "error": str(e)}

    if outcome == "ok":
        return {"key": key, "outcome": "ok"}

    reason = "blank" if outcome == "reject-blank" else "person"
    new_key = reject_key_from_original(key, reason)

    if cfg.dry_run:
        return {
            "key": key,
            "outcome": outcome,
            "new_key": new_key,
            "dry_run": True,
        }

    try:
        rename_object(s3, bucket, key, new_key)
    except Exception as e:
        return {
            "key": key,
            "outcome": "error",
            "error": str(e),
            "phase": "rename",
            "failed_outcome": outcome,
            "new_key": new_key,
        }

    return {"key": key, "outcome": outcome, "new_key": new_key}


def main() -> None:
    p = argparse.ArgumentParser(description="Tag bad -nobg.png on R2")
    p.add_argument("--dry-run", action="store_true", help="Log actions only")
    p.add_argument("--limit", type=int, default=None, help="Max keys this run")
    p.add_argument("--prefix", default="products/", help="S3 prefix to list")
    p.add_argument(
        "--min-alpha-fraction",
        type=float,
        default=0.0005,
        help="Min fraction of pixels with alpha above threshold (default 0.05%%)",
    )
    p.add_argument(
        "--alpha-thresh",
        type=int,
        default=12,
        help="Alpha value above this counts as opaque (0-255)",
    )
    p.add_argument(
        "--person-conf",
        type=float,
        default=0.35,
        help="YOLO confidence threshold for person class",
    )
    p.add_argument(
        "--min-box-area-frac",
        type=float,
        default=0.002,
        help="Min bounding-box area as fraction of image to count as person",
    )
    p.add_argument(
        "--skip-yolo",
        action="store_true",
        help="Only alpha test (no person detection)",
    )
    p.add_argument(
        "-j",
        "--parallel",
        type=int,
        default=1,
        metavar="N",
        help="Concurrent workers (default 1). Overlaps R2 I/O; YOLO inference is "
        "serialized with a lock when N>1. Very high N may hit R2 rate limits.",
    )
    args = p.parse_args()
    if args.parallel < 1:
        print("--parallel must be >= 1", file=sys.stderr)
        sys.exit(1)
    cfg = RunConfig(
        dry_run=args.dry_run,
        limit=args.limit,
        prefix=args.prefix,
        min_alpha_fraction=args.min_alpha_fraction,
        alpha_thresh=args.alpha_thresh,
        person_conf=args.person_conf,
        min_box_area_frac=args.min_box_area_frac,
    )

    env = require_env()
    bucket = env["R2_BUCKET_NAME"]

    TMP_DIR.mkdir(parents=True, exist_ok=True)

    completed, failed = load_progress()
    completed_set = set(completed)
    failed_set = set(failed)

    s3 = make_s3_client(env)

    log_line("============================================================")
    log_line(
        f"tag-nobg-quality starting (dry_run={cfg.dry_run}, "
        f"skip_yolo={args.skip_yolo}, parallel={args.parallel})"
    )

    all_nobg = list_nobg_keys(s3, bucket, cfg.prefix)
    # Work queue: not yet successfully completed
    queue = [k for k in all_nobg if k not in completed_set]
    if cfg.limit is not None:
        queue = queue[: cfg.limit]

    total = len(queue)
    log_line(
        f"Listed {len(all_nobg)} canonical -nobg.png keys; "
        f"{len(completed_set)} already completed; "
        f"{total} to process this run."
    )

    if total == 0:
        log_line("Nothing to do.")
        return

    model = None
    if not args.skip_yolo:
        try:
            from ultralytics import YOLO
        except ImportError:
            print(
                "Install ultralytics: pip install ultralytics",
                file=sys.stderr,
            )
            sys.exit(1)
        log_line("Loading YOLOv8n (first run may download weights)...")
        model = YOLO("yolov8n.pt")

    counts: dict[str, int] = {
        "ok": 0,
        "reject-blank": 0,
        "reject-person": 0,
        "error": 0,
    }

    max_workers = max(1, min(args.parallel, total))
    inference_lock = (
        threading.Lock() if args.parallel > 1 and not args.skip_yolo else None
    )
    io_lock = threading.Lock()

    t0 = time.time()
    done = 0

    def handle_result(res: dict[str, Any]) -> None:
        nonlocal done
        with io_lock:
            done += 1
            key = res["key"]
            pct = 100.0 * done / total
            prefix = f"[{done}/{total}] ({pct:.1f}%)"
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            o = res.get("outcome")

            if o == "error":
                err = res.get("error", "")
                if res.get("phase") == "rename":
                    fo = res.get("failed_outcome", "")
                    if fo in counts:
                        counts[fo] -= 1
                    counts["error"] += 1
                    nk = res.get("new_key", "")
                    log_line(
                        f"{prefix} rename failed {key} -> {nk} — {err}",
                        _lock=None,
                    )
                else:
                    counts["error"] += 1
                    log_line(f"{prefix} error {key} — {err}", _lock=None)
                if not cfg.dry_run:
                    if key not in failed_set:
                        failed.append(key)
                        failed_set.add(key)
                    save_progress(completed, failed)
                    append_history(
                        {
                            "key": key,
                            "outcome": "error",
                            "error": err,
                            "phase": res.get("phase"),
                            "ts": ts,
                        }
                    )
                return

            if o == "ok":
                counts["ok"] += 1
                log_line(f"{prefix} ok {key}", _lock=None)
                if not cfg.dry_run:
                    completed.append(key)
                    completed_set.add(key)
                    if key in failed_set:
                        failed[:] = [x for x in failed if x != key]
                        failed_set.discard(key)
                    save_progress(completed, failed)
                    append_history({"key": key, "outcome": "ok", "ts": ts})
                return

            if res.get("dry_run"):
                outcome = str(res.get("outcome", ""))
                if outcome in ("reject-blank", "reject-person"):
                    counts[outcome] += 1
                new_key = res.get("new_key", "")
                reason = "blank" if outcome == "reject-blank" else "person"
                log_line(
                    f"{prefix} would reject-{reason} {key} -> {new_key}",
                    _lock=None,
                )
                return

            if o in ("reject-blank", "reject-person"):
                counts[o] += 1
                new_key = res.get("new_key", "")
                reason = "blank" if o == "reject-blank" else "person"
                elapsed = time.time() - t0
                avg = elapsed / done if done else 0.0
                eta_s = avg * (total - done)
                log_line(
                    f"{prefix} reject-{reason} {key} -> {new_key} "
                    f"(elapsed {elapsed:.0f}s, ETA ~{eta_s:.0f}s)",
                    _lock=None,
                )
                if not cfg.dry_run:
                    completed.append(key)
                    completed_set.add(key)
                    if key in failed_set:
                        failed[:] = [x for x in failed if x != key]
                        failed_set.discard(key)
                    save_progress(completed, failed)
                    append_history(
                        {
                            "key": key,
                            "outcome": f"reject-{reason}",
                            "new_key": new_key,
                            "ts": ts,
                        }
                    )

    futures: dict[concurrent.futures.Future[Any], str] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for key in queue:
            fut = executor.submit(
                run_key_pipeline, key, cfg, s3, bucket, model, inference_lock
            )
            futures[fut] = key
        for fut in concurrent.futures.as_completed(futures):
            try:
                res = fut.result()
            except Exception as e:
                res = {
                    "key": futures[fut],
                    "outcome": "error",
                    "error": str(e),
                }
            handle_result(res)

    log_line(
        f"Done. ok={counts['ok']} reject-blank={counts['reject-blank']} "
        f"reject-person={counts['reject-person']} errors={counts['error']}"
    )
    log_line("============================================================")


if __name__ == "__main__":
    main()
