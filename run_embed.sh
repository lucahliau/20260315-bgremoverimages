#!/usr/bin/env bash
# run_embed.sh — run embed_worker.py unattended with full sleep prevention
# and an automatic restart loop so the encode watchdog (exit 124) just
# resumes from the DB instead of leaving the queue stalled.
#
# Usage:
#   ./run_embed.sh [extra args passed to embed_worker.py]
# Example:
#   ./run_embed.sh --download-workers 12 --batch-size 32

set -u

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "run_embed.sh: missing .env (need DATABASE_URL and R2_PUBLIC_URL)" >&2
  exit 1
fi

# Pull DATABASE_URL / R2_PUBLIC_URL into the environment.
set -a
# shellcheck disable=SC1091
source .env
set +a

PY=./venv/bin/python
if [[ ! -x "$PY" ]]; then
  PY=python3
fi

# Default args if none provided.
if [[ $# -eq 0 ]]; then
  set -- --download-workers 12 --batch-size 32
fi

# caffeinate flags:
#   -d  prevent display sleep
#   -i  prevent idle sleep
#   -m  prevent disk sleep
#   -s  prevent system sleep (only effective on AC power)
#   -u  declare user activity (resets idle timer)
CAFFEINATE_FLAGS="-dimsu"

attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "[run_embed.sh] attempt #${attempt} starting at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  caffeinate ${CAFFEINATE_FLAGS} "$PY" embed_worker.py "$@"
  rc=$?
  echo "[run_embed.sh] embed_worker exited with code ${rc} at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

  case "$rc" in
    0)
      echo "[run_embed.sh] clean exit — nothing more to do."
      exit 0
      ;;
    124)
      echo "[run_embed.sh] encode watchdog tripped — restarting in 5s."
      sleep 5
      ;;
    130)
      echo "[run_embed.sh] interrupted by user (SIGINT) — stopping."
      exit 130
      ;;
    *)
      echo "[run_embed.sh] unexpected exit ${rc} — restarting in 30s (Ctrl+C to stop)."
      sleep 30
      ;;
  esac
done
