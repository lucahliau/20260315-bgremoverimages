# Local embeddings (CLIP → Postgres / pgvector)

## 1. Database migration (backend repo)

From `20260311 Clothes backend`:

```bash
npx prisma migrate deploy
```

Requires PostgreSQL with the **pgvector** extension available (e.g. `CREATE EXTENSION vector` — included in the migration).

## 2. Python dependencies (this folder)

Either use the helper (creates `./venv`):

```bash
npm run embed:setup
```

Then run the worker with the venv interpreter:

```bash
source .env   # DATABASE_URL + R2_PUBLIC_URL (same as remove-bg)
./venv/bin/python embed_worker.py --dry-run
./venv/bin/python embed_worker.py --limit 100 --download-workers 12 --batch-size 32
```

For unattended runs (overnight, lid-closed, etc.) use the launcher — it
sources `.env`, runs under `caffeinate -dimsu` so the system / display /
disk never sleep, and auto-restarts on the encode-watchdog exit code 124:

```bash
./run_embed.sh                                   # default args
./run_embed.sh --download-workers 12 --batch-size 32
npm run embed:run -- --download-workers 12 --batch-size 32
```

Or install into your own environment:

```bash
pip install sentence-transformers psycopg2-binary pgvector pillow requests torch
```

## 3. Environment

- `DATABASE_URL` — Postgres connection string (same DB as the clothes app).
- `R2_PUBLIC_URL` — used to build `-nobg.png` URLs (same rule as `getNobgUrl` in the backend).

## 4. Dashboard

```bash
source .env   # add DATABASE_URL for embed stats
npm run ui
```

Open the root URL: R2 no-bg section + **Embeddings** section (counts from Postgres, throughput from `embed-history.jsonl`).

## 5. Resuming

The worker skips any item that already has a row in `ItemEmbedding` for the chosen `--model`. `embed-progress.json` is optional bookkeeping; the database is the source of truth.
