#!/usr/bin/env python3
"""Revert person_scan_worker.py --apply writes from a given run window.

For items HIDDEN after --since: un-rename their R2 objects back (REJECT-person ->
original), restore images[]=[imageUrl], and clear hasPerson/personScannedAt/
personScanConfidence so they re-scan after the crawler gallery fix. Also clears
the scan flags on the clean-but-scanned items from the same window (no R2 change).

  source .env && ./venv/bin/python person_scan_revert.py --since 2026-06-20T17:37:00Z          # dry-run
  source .env && ./venv/bin/python person_scan_revert.py --since 2026-06-20T17:37:00Z --apply
"""
import os, sys, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from embed_worker import sanitize_db_url_for_psycopg2, get_s3_client, s3_enabled, get_nobg_key
from person_scan_worker import get_original_key, reject_key
import psycopg2


def unrename(s3, bucket, key):
    """Move reject_key(key) back to key. No-op if the reject object is absent."""
    src = reject_key(key)
    if src == key:
        return False
    try:
        s3.copy_object(Bucket=bucket, Key=key, CopySource={"Bucket": bucket, "Key": src})
        s3.delete_object(Bucket=bucket, Key=src)
        return True
    except Exception:
        return False


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2026-06-20T17:37:00Z")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()

    r2_base = os.environ["R2_PUBLIC_URL"].rstrip("/")
    bucket = os.environ.get("R2_BUCKET_NAME", "").strip()
    conn = psycopg2.connect(sanitize_db_url_for_psycopg2(os.environ["DATABASE_URL"]))
    cur = conn.cursor()

    cur.execute('SELECT id, "imageUrl" FROM "ClothingItem" WHERE "hasPerson"=true AND "personScannedAt">=%s', (args.since,))
    hidden = cur.fetchall()
    cur.execute('SELECT id FROM "ClothingItem" WHERE ("hasPerson" IS NOT TRUE) AND "personScannedAt">=%s', (args.since,))
    clean = [r[0] for r in cur.fetchall()]
    print(f"to-revert: hidden={len(hidden)}  clean-scanned={len(clean)}  since={args.since}")

    if not args.apply:
        print("DRY-RUN (pass --apply to revert). sample hidden:")
        for id_, u in hidden[:5]:
            print(f"  {id_}  {u}")
        conn.close()
        return

    if not s3_enabled():
        print("ERROR: need R2 creds to un-rename", file=sys.stderr)
        sys.exit(1)
    s3 = get_s3_client()

    def restore_r2(item):
        id_, u = item
        ok = get_original_key(u, r2_base) if u else None
        nobg = get_nobg_key(u, r2_base) if u else None
        renamed = unrename(s3, bucket, ok) if ok else False
        if nobg:
            unrename(s3, bucket, nobg)  # sibling, best-effort
        return (id_, u, renamed)

    restored = 0
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for f in as_completed([ex.submit(restore_r2, it) for it in hidden]):
            r = f.result()
            results.append(r)
            restored += int(r[2])
    print(f"R2 objects un-renamed: {restored}/{len(hidden)}")

    n = 0
    for id_, u, _ in results:
        if u:
            cur.execute(
                'UPDATE "ClothingItem" SET images=ARRAY[%s]::text[], "imageUrl"=%s, '
                '"hasPerson"=NULL, "personScannedAt"=NULL, "personScanConfidence"=NULL WHERE id=%s',
                (u, u, id_),
            )
        else:
            cur.execute(
                'UPDATE "ClothingItem" SET "hasPerson"=NULL, "personScannedAt"=NULL, "personScanConfidence"=NULL WHERE id=%s',
                (id_,),
            )
        n += 1
        if n % 100 == 0:
            conn.commit()
    for id_ in clean:
        cur.execute(
            'UPDATE "ClothingItem" SET "hasPerson"=NULL, "personScannedAt"=NULL, "personScanConfidence"=NULL WHERE id=%s',
            (id_,),
        )
    conn.commit()
    print(f"DB restored: hidden={len(hidden)} (images+flags), clean cleared={len(clean)}")
    conn.close()


if __name__ == "__main__":
    main()
