#!/usr/bin/env python3
"""
person_scan_worker.py — Detect product photos that contain a PERSON/MODEL and
clean the catalog, resumable. Runs a local YOLO model (no Gemini/API).

For every ClothingItem it scans the ORIGINAL images (the ones the app actually
shows — `imageUrl` + `images[]`), classifies each as "person" or "clean", then:
  • mixed (some clean, some person) → strip the person URLs from images[], repoint
    imageUrl to the first clean image, rename the rejected R2 objects to
    *__REJECT-person.* , and (if the primary changed) drop the stale ItemEmbedding
    so it re-embeds against the new primary. Product stays visible, now clean.
  • all images are person → hasPerson=true (the backend gate hides it), strip
    images[], rename the rejected objects, drop the embedding.
  • no person → hasPerson=false. (Either way personScannedAt is set.)

This is the original-image, DB-writing, scheduled/continuous successor to
tag-nobg-quality.py (which only renamed -nobg.png objects and never touched the
DB). Improvements: scans originals (works even when the flaky -nobg pipeline has
no PNG), upgraded model (yolo11s) + a tuned person-DOMINANCE rule (so tight
detail shots with an incidental hand/foot are kept), DB writes + product hiding,
classified retry/quarantine (no poison loop), and a calibration dry-run that
emits annotated contact sheets.

Usage:
  source .env   # DATABASE_URL, R2_PUBLIC_URL, R2_* required
  # Calibrate (no writes) — sample the catalog and emit contact sheets:
  python3 person_scan_worker.py --dry-run --limit 800 --sample-out tmp/person-samples
  # Apply (DB + R2 writes), resumable, newest first:
  python3 person_scan_worker.py --apply --limit 2000

Requires (managed venv): ultralytics, torch, pillow, numpy, boto3, psycopg2-binary, requests.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Reuse the battle-tested DB/S3/backoff helpers from embed_worker (same dir).
# Importing the module reads env + probes boto3 but does NOT run its main().
from embed_worker import (  # noqa: E402
    sanitize_db_url_for_psycopg2,
    get_nobg_key,
    s3_enabled,
    get_s3_client,
    _http_get,
    _s3_get,
    _sleep_backoff,
    _PermanentDownloadError,
    _TransientDownloadError,
    _MAX_DOWNLOAD_ATTEMPTS,
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROGRESS_FILE = SCRIPT_DIR / "person-scan-progress.json"
HISTORY_FILE = SCRIPT_DIR / "person-scan-history.jsonl"
LOG_FILE = SCRIPT_DIR / "person-scan.log"

# COCO "person" class index (yolov8 / yolo11 share the COCO class map).
PERSON_CLASS = 0


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


# ── R2 key helpers ───────────────────────────────────────────────────────────


def get_original_key(image_url: str, r2_base_url: str) -> str | None:
    """R2 object KEY for an ORIGINAL product image (products/.../n.jpg), or None
    if the url isn't an R2 product image. Sibling to embed_worker.get_nobg_key,
    but does NOT swap the suffix to -nobg.png."""
    base = r2_base_url.rstrip("/")
    clean = image_url.split("?")[0].split("#")[0]
    if clean.startswith(base):
        path = clean[len(base):].lstrip("/")
    elif clean.startswith("products/"):
        path = clean
    else:
        return None
    return path if path.startswith("products/") else None


def reject_key(key: str) -> str:
    """products/a/0.jpg -> products/a/0__REJECT-person.jpg (reversible rename)."""
    if "." in key.rsplit("/", 1)[-1]:
        stem, ext = key.rsplit(".", 1)
        return f"{stem}__REJECT-person.{ext}"
    return f"{key}__REJECT-person"


# ── progress / quarantine (mirrors embed_worker) ──────────────────────────────


def load_progress() -> set[str]:
    """Return permanently-quarantined item ids (all images un-downloadable)."""
    if not PROGRESS_FILE.exists():
        return set()
    try:
        data = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        return set(str(x) for x in (data.get("permanent") or []))
    except (json.JSONDecodeError, OSError):
        return set()


def save_progress(permanent: set[str]) -> None:
    PROGRESS_FILE.write_text(
        json.dumps({"permanent": sorted(permanent)}, indent=2), encoding="utf-8"
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
        print("Missing R2_PUBLIC_URL", file=sys.stderr)
        sys.exit(1)
    return db, r2.rstrip("/")


# ── image download (originals; reuses embed_worker transport) ──────────────────


@dataclass
class DownloadResult:
    url: str
    image: Any = None  # PIL.Image (RGB) when ok
    error: str | None = None
    error_kind: str | None = None  # 'permanent' | 'transient'


def download_image(url: str, r2_base: str, max_bytes: int = 25 * 1024 * 1024) -> DownloadResult:
    """Download one ORIGINAL image as a PIL RGB image, or a classified error.
    Prefers the authenticated S3 API for R2 keys (no per-IP throttle), else a
    plain HTTP GET (covers any non-R2 image URL). Retries transient errors."""
    from PIL import Image
    from io import BytesIO

    key = get_original_key(url, r2_base)
    use_s3 = bool(key) and s3_enabled()
    last_err = "unknown"

    for attempt in range(_MAX_DOWNLOAD_ATTEMPTS):
        retry_after: float | None = None
        try:
            if use_s3:
                data = _s3_get(key, max_bytes)
            else:
                data = _http_get(url, 10.0, 30.0, max_bytes)
            img = Image.open(BytesIO(data)).convert("RGB")
            return DownloadResult(url=url, image=img)
        except _PermanentDownloadError as e:
            return DownloadResult(url=url, error=str(e), error_kind="permanent")
        except _TransientDownloadError as e:
            last_err, retry_after = str(e), e.retry_after
        except Exception as e:  # decode/unknown — permanent
            return DownloadResult(url=url, error=f"decode/unknown: {e}", error_kind="permanent")
        if attempt < _MAX_DOWNLOAD_ATTEMPTS - 1:
            _sleep_backoff(attempt, retry_after)

    return DownloadResult(
        url=url, error=f"{last_err} (after {_MAX_DOWNLOAD_ATTEMPTS} attempts)", error_kind="transient"
    )


# ── detector ───────────────────────────────────────────────────────────────


@dataclass
class PersonVerdict:
    is_person: bool
    confidence: float = 0.0   # top person-box confidence
    area_frac: float = 0.0    # largest person box area / image area
    height_frac: float = 0.0  # largest person box height / image height
    keypoints: int = 0        # confident body keypoints on the best detection
    reason: str = ""          # see detect()


# COCO pose keypoint indices (yolo*-pose order).
HEAD_KPS = [0, 1, 2, 3, 4]   # nose, eyes, ears
L_SHO, R_SHO = 5, 6
L_HIP, R_HIP = 11, 12
L_KNEE, R_KNEE = 13, 14
L_ANK, R_ANK = 15, 16


@dataclass
class DetectorConfig:
    conf: float = 0.40             # person box confidence floor
    candidate_area: float = 0.05   # min person-box area to consider (below = incidental → keep)
    kp_conf: float = 0.70          # a body keypoint counts as "confident" above this
    face_min_frac: float = 0.07    # min detected-face side / min(image side)
    face_min_neighbors: int = 10   # Haar minNeighbors (higher = fewer false faces)
    # Fallback when no pose model loads (bbox-only, noisier):
    dominance_frac: float = 0.12   # bbox area >= this → person
    tall_frac: float = 0.55        # bbox height >= this → person


class Detector:
    """Distinguishes a real person/model from a garment that merely *looks*
    person-shaped (a flat-lay raincoat, a folded knit) by running a POSE model:
    a real human yields confident body keypoints (shoulders/hips/knees/face),
    while a flat garment yields a bounding box with almost none. An image is
    flagged as a person photo when a sufficiently large detection also has
    >= min_keypoints confident keypoints. If the pose model can't be loaded it
    falls back to plain bbox dominance (the original, noisier heuristic)."""

    def __init__(self, cfg: DetectorConfig, weights: str, device: str, log_lock: threading.Lock):
        self.cfg = cfg
        self.device = device
        self.log_lock = log_lock
        self.lock = threading.Lock()  # serialize inference (MPS is single-stream)
        from ultralytics import YOLO

        self.has_pose = False
        try:
            self.model = YOLO(weights)
            self.has_pose = "pose" in str(weights).lower()
            log_line(f"Loaded YOLO weights {weights!r} (pose={self.has_pose}) on {device}", lock=log_lock)
        except Exception as e:  # offline / download failed → local bbox fallback
            fallback = str(SCRIPT_DIR / "yolov8n.pt")
            log_line(
                f"YOLO {weights!r} load failed ({e}); falling back to {fallback} (bbox-only)",
                lock=log_lock,
            )
            self.model = YOLO(fallback)
            self.has_pose = False

        # Real frontal-face detector (cv2 Haar). A genuine face is near-impossible
        # on a flat garment, so this is the high-precision signal for on-model
        # shots; the pose full-body rule adds faceless/back-view models.
        self.face_cascade = None
        try:
            import cv2

            cas = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
            if not cas.empty():
                self.face_cascade = cas
                log_line("Face detector (Haar frontal) enabled", lock=log_lock)
            else:
                log_line("Haar cascade empty; face detection disabled", lock=log_lock)
        except Exception as e:
            log_line(f"cv2 face detector unavailable ({e}); pose-only", lock=log_lock)

    def _has_face(self, image: Any) -> bool:
        import numpy as np

        if self.face_cascade is None:
            return False
        w, h = image.size
        gray = np.asarray(image.convert("L"))
        side = max(20, int(min(w, h) * self.cfg.face_min_frac))
        with self.lock:
            faces = self.face_cascade.detectMultiScale(
                gray, scaleFactor=1.1, minNeighbors=self.cfg.face_min_neighbors, minSize=(side, side)
            )
        return len(faces) > 0

    def detect(self, image: Any) -> PersonVerdict:
        cfg = self.cfg
        w, h = image.size
        area_img = float(w * h) or 1.0

        # A real detected face is the strongest, cleanest person signal.
        if self._has_face(image):
            return PersonVerdict(True, 1.0, 0.0, 0.0, 0, "face")

        with self.lock:
            results = self.model.predict(
                source=image, classes=[PERSON_CLASS], conf=cfg.conf, verbose=False
            )
        res = results[0] if results else None
        boxes = res.boxes if res is not None else None
        if boxes is None or len(boxes) == 0:
            return PersonVerdict(False, reason="no-detection")

        # Per-detection keypoint confidences (pose models only); aligned to boxes.
        kp_rows = None
        kp = getattr(res, "keypoints", None)
        if self.has_pose and kp is not None and kp.conf is not None:
            kp_rows = kp.conf.cpu().numpy()  # shape (N, 17)

        best = PersonVerdict(False, reason="below-floor")
        best_metric = -1.0
        for i in range(len(boxes)):
            xyxy = boxes[i].xyxy[0].cpu().numpy()
            bw = float(xyxy[2] - xyxy[0])
            bh = float(xyxy[3] - xyxy[1])
            af = (bw * bh) / area_img
            if af < cfg.candidate_area:
                continue  # incidental hand/foot/figure → ignore this box
            hf = bh / float(h)
            c = float(boxes[i].conf[0].cpu().numpy()) if boxes[i].conf is not None else 0.0

            if kp_rows is not None and i < len(kp_rows):
                m = kp_rows[i] >= cfg.kp_conf  # bool (17,)
                n_kp = int(m.sum())
                torso = bool(m[L_SHO] and m[R_SHO] and m[L_HIP] and m[R_HIP])
                legs = bool(
                    m[L_KNEE] and m[R_KNEE] and ((m[L_HIP] and m[R_HIP]) or (m[L_ANK] and m[R_ANK]))
                )
                # A full standing body needs BOTH a torso and legs. A single flat
                # garment is shaped like one or the other (a shirt has no legs,
                # pants have no shoulders), never a coherent full body.
                if torso and legs:
                    is_person, reason = True, "full-body"
                else:
                    is_person = False
                    reason = "torso-only" if torso else "legs-only" if legs else "no-anatomy"
            else:  # no pose info → bbox dominance fallback
                n_kp = -1
                is_person = af >= cfg.dominance_frac or hf >= cfg.tall_frac
                reason = "bbox-dominant" if is_person else "bbox-weak"

            # Prefer the most informative detection: a real person outranks a
            # reject, then larger area wins.
            metric = (1000.0 if is_person else 0.0) + af
            if metric > best_metric:
                best_metric = metric
                best = PersonVerdict(is_person, c, af, hf, max(n_kp, 0), reason)
        return best


# ── per-item decision ─────────────────────────────────────────────────────────


@dataclass
class ItemDecision:
    item_id: str
    outcome: str  # 'clean' | 'stripped' | 'hidden' | 'quarantine-failed' | 'skip-no-images'
    new_images: list[str] = field(default_factory=list)
    new_primary: str | None = None
    rejected_urls: list[str] = field(default_factory=list)
    has_person: bool = False
    primary_changed: bool = False
    confidence: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)


def decide_item(
    item_id: str,
    primary: str,
    images: list[str],
    verdicts: dict[str, PersonVerdict],
    failures: dict[str, str],  # url -> error_kind for un-downloaded images
) -> ItemDecision:
    """Aggregate per-image verdicts into one product decision.

    Order of `images` is preserved. A URL with no verdict and a TRANSIENT failure
    blocks a confident decision (we can't tell if it's a person), so we defer the
    whole item to a later run. Permanently-failed URLs (404) are treated as gone
    and dropped from the kept set.
    """
    originals = [u for u in images if u]
    scan_set = list(originals)
    if primary and primary not in scan_set:
        scan_set.insert(0, primary)
    if not scan_set:
        return ItemDecision(item_id, "skip-no-images")

    # Defer if any image is only transiently unavailable (verdict unknown).
    transient = [u for u in scan_set if u not in verdicts and failures.get(u) == "transient"]
    if transient:
        return ItemDecision(item_id, "quarantine-failed", detail={"transient": len(transient)})

    def is_person(u: str) -> bool:
        v = verdicts.get(u)
        return bool(v and v.is_person)

    # Kept originals: clean + still-downloadable (drop permanent-404 entirely).
    kept = [u for u in originals if (u in verdicts and not verdicts[u].is_person)]
    rejected = [u for u in scan_set if is_person(u)]
    person_confs = [verdicts[u].confidence for u in scan_set if is_person(u)]
    top_conf = max(person_confs) if person_confs else None

    primary_clean = primary in verdicts and not verdicts[primary].is_person

    if not rejected:
        # No person anywhere. (Permanent-404 images already dropped from `kept`;
        # only rewrite images[] if something actually fell off.)
        return ItemDecision(
            item_id, "clean", new_images=kept,
            new_primary=primary if (primary_clean or primary not in verdicts) else (kept[0] if kept else primary),
            has_person=False, confidence=top_conf,
            detail={"changed_images": kept != originals},
        )

    if kept:
        new_primary = primary if primary_clean else kept[0]
        return ItemDecision(
            item_id, "stripped", new_images=kept, new_primary=new_primary,
            rejected_urls=rejected, has_person=False,
            primary_changed=(new_primary != primary), confidence=top_conf,
            detail={"kept": len(kept), "rejected": len(rejected)},
        )

    # No clean image survives → person-only product, hide it.
    return ItemDecision(
        item_id, "hidden", new_images=[], new_primary=primary,
        rejected_urls=rejected, has_person=True, primary_changed=True, confidence=top_conf,
        detail={"rejected": len(rejected)},
    )


# ── R2 + DB writes ─────────────────────────────────────────────────────────


def rename_r2_object(s3: Any, bucket: str, key: str) -> bool:
    """Copy key -> reject_key(key), delete original. Returns True if renamed.
    Missing source (already renamed / never existed) is treated as a no-op."""
    dst = reject_key(key)
    try:
        s3.copy_object(Bucket=bucket, Key=dst, CopySource={"Bucket": bucket, "Key": key})
        s3.delete_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def reject_in_r2(s3: Any, bucket: str, r2_base: str, urls: list[str], log_lock: threading.Lock) -> int:
    """Rename each rejected original (and its -nobg.png sibling, if any) to
    *__REJECT-person.* . Best-effort; reversible. Returns count renamed."""
    renamed = 0
    for url in urls:
        ok = get_original_key(url, r2_base)
        nobg = get_nobg_key(url, r2_base)
        if ok:
            renamed += int(rename_r2_object(s3, bucket, ok))
        if nobg:
            rename_r2_object(s3, bucket, nobg)  # sibling; ignore result
    return renamed


def apply_decision(conn: Any, d: ItemDecision) -> None:
    """Persist one item decision: images[], imageUrl, hasPerson, personScannedAt,
    confidence; drop the stale embedding when the primary image changed."""
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE "ClothingItem"
        SET images = %s,
            "imageUrl" = %s,
            "hasPerson" = %s,
            "personScannedAt" = now(),
            "personScanConfidence" = %s
        WHERE id = %s
        """,
        (d.new_images, d.new_primary, d.has_person, d.confidence, d.item_id),
    )
    if d.primary_changed or d.has_person:
        # The CLIP embedding was computed from the old primary's -nobg; drop it so
        # the embed worker re-embeds against the new primary (NOT EXISTS picks it up).
        cur.execute('DELETE FROM "ItemEmbedding" WHERE "itemId" = %s', (d.item_id,))
    conn.commit()
    cur.close()


def mark_scanned_no_change(conn: Any, item_id: str, has_person: bool, confidence: float | None) -> None:
    cur = conn.cursor()
    cur.execute(
        'UPDATE "ClothingItem" SET "hasPerson" = %s, "personScannedAt" = now(), "personScanConfidence" = %s WHERE id = %s',
        (has_person, confidence, item_id),
    )
    conn.commit()
    cur.close()


# ── work query ────────────────────────────────────────────────────────────


def column_exists(conn: Any, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_name = %s AND column_name = %s",
        (table, column),
    )
    found = cur.fetchone() is not None
    cur.close()
    conn.rollback()
    return found


def fetch_work_batch(
    conn: Any, limit: int, exclude_ids: list[str], scanned_col: bool, order: str,
    num_shards: int = 1, shard: int = 0,
) -> list[tuple[str, str, list[str]]]:
    """Return (id, imageUrl, images[]) for active items needing a scan.

    When num_shards > 1 the catalog is split into N disjoint, deterministic
    hash-shards (by id), so several processes can churn in parallel without ever
    fetching the same row — no double-work and no R2 rename races."""
    where = ['ci.active = true', 'NOT (ci.id::text = ANY(%s::text[]))']
    params: list[Any] = [exclude_ids]
    if scanned_col:
        where.append('ci."personScannedAt" IS NULL')
    if num_shards > 1:
        # ((h % n) + n) % n keeps the bucket in [0, n) even when hashtext is negative.
        where.append("((hashtext(ci.id) %% %s) + %s) %% %s = %s")
        params += [num_shards, num_shards, num_shards, shard]
    order_sql = "RANDOM()" if order == "random" else 'ci."createdAt" DESC'
    params.append(limit)
    cur = conn.cursor()
    cur.execute(
        f'SELECT ci.id::text, ci."imageUrl", ci.images FROM "ClothingItem" ci '
        f'WHERE {" AND ".join(where)} ORDER BY {order_sql} LIMIT %s',
        tuple(params),
    )
    rows = cur.fetchall()
    cur.close()
    conn.rollback()
    return [(r[0], r[1], list(r[2] or [])) for r in rows]


# ── sample contact sheets (dry-run calibration) ───────────────────────────


def annotate(image: Any, label: str, v: PersonVerdict, thumb: int = 320) -> Any:
    """Return a thumbnail with the verdict drawn on it (for the contact sheet)."""
    from PIL import Image, ImageDraw

    im = image.copy()
    im.thumbnail((thumb, thumb))
    d = ImageDraw.Draw(im)
    color = (220, 30, 30) if v.is_person else (20, 160, 60)
    d.rectangle([0, 0, im.size[0] - 1, im.size[1] - 1], outline=color, width=4)
    txt = f"{label} a{v.area_frac:.2f} kp{v.keypoints} c{v.confidence:.2f} {v.reason}"
    d.rectangle([0, 0, im.size[0], 16], fill=color)
    d.text((3, 3), txt[:52], fill=(255, 255, 255))
    return im


def contact_sheet(thumbs: list[Any], path: Path, cols: int = 6, cell: int = 330) -> None:
    from PIL import Image

    if not thumbs:
        return
    rows = (len(thumbs) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * cell, rows * cell), (245, 245, 245))
    for i, t in enumerate(thumbs):
        x = (i % cols) * cell + (cell - t.size[0]) // 2
        y = (i // cols) * cell + (cell - t.size[1]) // 2
        sheet.paste(t, (x, y))
    path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(path, "JPEG", quality=85)


# ── main ──────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(description="Detect & clean product photos containing people")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="Write DB + R2 changes")
    mode.add_argument("--dry-run", action="store_true", help="No writes; report counts (+ samples)")
    p.add_argument("--limit", type=int, default=0, help="Max items this run (0 = unlimited)")
    p.add_argument("--work-chunk", type=int, default=64, help="Items fetched from DB per iteration")
    p.add_argument("--download-workers", type=int, default=8, help="Parallel image downloads")
    p.add_argument("--order", choices=["recent", "random"], default="recent",
                   help="recent = newest first (resumable, prioritizes new brands); random = fair sample")
    p.add_argument("--num-shards", type=int, default=1,
                   help="Split the catalog into N disjoint hash-shards (by id) for safe parallel runs")
    p.add_argument("--shard", type=int, default=0,
                   help="Which shard in [0, num-shards) THIS process handles")
    p.add_argument("--device", default="mps", choices=["mps", "cpu", "cuda"])
    p.add_argument("--weights", default=os.environ.get("PERSON_SCAN_WEIGHTS", "yolo11s-pose.pt"))
    p.add_argument("--conf", type=float, default=0.40)
    p.add_argument("--candidate-area", type=float, default=0.05,
                   help="Min person-box area fraction to treat as a candidate (below = kept)")
    p.add_argument("--kp-conf", type=float, default=0.70,
                   help="A body keypoint counts as confident above this score (raise = stricter)")
    p.add_argument("--face-min-frac", type=float, default=0.07,
                   help="Min detected-face side as a fraction of the shorter image side")
    p.add_argument("--face-min-neighbors", type=int, default=10,
                   help="Haar minNeighbors — higher rejects more false faces")
    p.add_argument("--dominance-frac", type=float, default=0.12, help="bbox-only fallback")
    p.add_argument("--tall-frac", type=float, default=0.55, help="bbox-only fallback")
    p.add_argument("--sample-out", default=None, help="(dry-run) dir for annotated contact sheets")
    p.add_argument("--sample-cap", type=int, default=42, help="Max thumbnails per contact sheet")
    args = p.parse_args()

    if not args.apply and not args.dry_run:
        args.dry_run = True  # default to the safe path

    if args.num_shards < 1 or not (0 <= args.shard < args.num_shards):
        print(f"Invalid sharding: shard={args.shard} num_shards={args.num_shards}", file=sys.stderr)
        sys.exit(1)
    if args.num_shards > 1:
        # Per-shard progress/history/log files so parallel shards never clobber each other.
        global PROGRESS_FILE, HISTORY_FILE, LOG_FILE
        PROGRESS_FILE = SCRIPT_DIR / f"person-scan-progress.shard{args.shard}.json"
        HISTORY_FILE = SCRIPT_DIR / f"person-scan-history.shard{args.shard}.jsonl"
        LOG_FILE = SCRIPT_DIR / f"person-scan.shard{args.shard}.log"

    db_url, r2_base = require_env()
    log_lock = threading.Lock()
    bucket = os.environ.get("R2_BUCKET_NAME", "").strip()

    log_line("=" * 60, lock=log_lock)
    log_line(
        f"person_scan start mode={'apply' if args.apply else 'dry-run'} weights={args.weights} "
        f"device={args.device} conf={args.conf} dominance={args.dominance_frac} tall={args.tall_frac} "
        f"kp_conf={args.kp_conf} face_min_frac={args.face_min_frac} order={args.order} "
        f"shard={args.shard}/{args.num_shards} limit={args.limit or 'unlimited'} "
        f"s3={'on' if s3_enabled() else 'off (public URL)'}",
        lock=log_lock,
    )

    try:
        import psycopg2
    except ImportError:
        print("Install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(sanitize_db_url_for_psycopg2(db_url))
    conn.autocommit = False

    scanned_col = column_exists(conn, "ClothingItem", "personScannedAt")
    if args.apply and not scanned_col:
        log_line("ERROR: --apply needs the hasPerson/personScannedAt migration deployed first.", lock=log_lock)
        conn.close()
        sys.exit(1)
    if not scanned_col:
        log_line("personScannedAt column absent (pre-migration) — sampling without the scanned filter.", lock=log_lock)

    device = args.device
    if device == "mps":
        try:
            import torch
            if not torch.backends.mps.is_available():
                device = "cpu"
        except Exception:
            device = "cpu"

    cfg = DetectorConfig(
        conf=args.conf, candidate_area=args.candidate_area, kp_conf=args.kp_conf,
        face_min_frac=args.face_min_frac, face_min_neighbors=args.face_min_neighbors,
        dominance_frac=args.dominance_frac, tall_frac=args.tall_frac
    )
    detector = Detector(cfg, args.weights, device, log_lock)

    s3 = None
    if args.apply:
        if not s3_enabled():
            log_line("ERROR: --apply needs authenticated R2 (R2_ACCOUNT_ID/ACCESS/SECRET/BUCKET) to rename objects.", lock=log_lock)
            conn.close()
            sys.exit(1)
        s3 = get_s3_client()

    permanent = load_progress()
    counts = {"clean": 0, "stripped": 0, "hidden": 0, "quarantine-failed": 0, "skip-no-images": 0,
              "images_rejected": 0, "items_scanned": 0}
    reason_counts: dict[str, int] = {}
    thumbs_by_reason: dict[str, list[Any]] = {}
    processed = 0
    outer_limit = args.limit if args.limit > 0 else None
    t0 = time.time()

    while True:
        chunk = args.work_chunk
        if outer_limit is not None:
            left = outer_limit - processed
            if left <= 0:
                break
            chunk = min(chunk, left)

        work = fetch_work_batch(
            conn, chunk, sorted(permanent)[:20000], scanned_col, args.order, args.num_shards, args.shard
        )
        if not work:
            log_line("No more items to scan.", lock=log_lock)
            break
        log_line(f"Fetched {len(work)} items", lock=log_lock)

        # Download every image in the chunk in parallel.
        tasks: list[tuple[str, str]] = []  # (item_id, url)
        for item_id, primary, images in work:
            urls = list(images)
            if primary and primary not in urls:
                urls.insert(0, primary)
            for u in urls:
                tasks.append((item_id, u))

        dl: dict[tuple[str, str], DownloadResult] = {}
        with ThreadPoolExecutor(max_workers=max(1, args.download_workers)) as ex:
            futs = {ex.submit(download_image, u, r2_base): (iid, u) for iid, u in tasks}
            for fut in as_completed(futs):
                iid, u = futs[fut]
                try:
                    dl[(iid, u)] = fut.result()
                except Exception as e:
                    dl[(iid, u)] = DownloadResult(url=u, error=f"task: {e}", error_kind="transient")

        # Run the detector (serialized) over downloaded images, then decide per item.
        for item_id, primary, images in work:
            urls = list(images)
            if primary and primary not in urls:
                urls.insert(0, primary)
            verdicts: dict[str, PersonVerdict] = {}
            failures: dict[str, str] = {}
            for u in urls:
                r = dl.get((item_id, u))
                if r is None or r.image is None:
                    failures[u] = (r.error_kind if r else "transient") or "transient"
                    continue
                v = detector.detect(r.image)
                verdicts[u] = v
                reason_counts[v.reason] = reason_counts.get(v.reason, 0) + 1
                if args.sample_out and v.reason != "no-detection":
                    bucket = thumbs_by_reason.setdefault(v.reason, [])
                    if len(bucket) < args.sample_cap:
                        bucket.append(annotate(r.image, "PERSON" if v.is_person else "KEEP", v))

            d = decide_item(item_id, primary, images, verdicts, failures)
            counts[d.outcome] = counts.get(d.outcome, 0) + 1
            counts["images_rejected"] += len(d.rejected_urls)

            if d.outcome == "quarantine-failed":
                # All-images-transiently-failed → leave for a later run, don't poison.
                continue
            if d.outcome == "skip-no-images":
                permanent.add(item_id)
                save_progress(permanent)
                continue

            # An item whose images ALL permanently 404'd surfaces as 'clean' with
            # empty kept + no verdicts → quarantine instead of marking it clean.
            if not verdicts and failures:
                permanent.add(item_id)
                save_progress(permanent)
                counts[d.outcome] -= 1
                counts["quarantine-failed"] += 1
                continue

            append_history({
                "id": item_id, "outcome": d.outcome, "has_person": d.has_person,
                "rejected": len(d.rejected_urls), "primary_changed": d.primary_changed,
                "conf": d.confidence, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            })

            if args.apply:
                if d.rejected_urls:
                    counts["images_rejected"] += 0  # already counted
                    reject_in_r2(s3, bucket, r2_base, d.rejected_urls, log_lock)
                if d.outcome in ("stripped", "hidden") or (d.outcome == "clean" and d.detail.get("changed_images")):
                    apply_decision(conn, d)
                else:
                    mark_scanned_no_change(conn, item_id, d.has_person, d.confidence)
            counts["items_scanned"] += 1
            processed += 1

        # progress log
        el = time.time() - t0
        log_line(
            f"[{processed} scanned, {el:.0f}s] clean={counts['clean']} stripped={counts['stripped']} "
            f"hidden={counts['hidden']} deferred={counts['quarantine-failed']} rejected_imgs={counts['images_rejected']}",
            lock=log_lock,
        )
        if not args.apply and outer_limit is None:
            # dry-run with no limit would scan the whole catalog; that's fine but
            # the chunk loop continues until work is exhausted.
            pass

    conn.close()

    if args.sample_out:
        out = Path(args.sample_out)
        out.mkdir(parents=True, exist_ok=True)
        for reason, thumbs in thumbs_by_reason.items():
            contact_sheet(thumbs, out / f"{reason}.jpg")
        (out / "summary.json").write_text(
            json.dumps({"counts": counts, "reasons": reason_counts}, indent=2), encoding="utf-8"
        )
        log_line(f"Samples (per reason) → {out}/<reason>.jpg", lock=log_lock)

    log_line(f"per-image verdict reasons: {dict(sorted(reason_counts.items(), key=lambda kv: -kv[1]))}", lock=log_lock)
    log_line(
        f"DONE ({'APPLIED' if args.apply else 'DRY-RUN'}): "
        f"items clean={counts['clean']} stripped={counts['stripped']} hidden={counts['hidden']} "
        f"deferred={counts['quarantine-failed']} no-images={counts['skip-no-images']} "
        f"| images rejected={counts['images_rejected']}",
        lock=log_lock,
    )
    log_line("=" * 60, lock=log_lock)


if __name__ == "__main__":
    main()
