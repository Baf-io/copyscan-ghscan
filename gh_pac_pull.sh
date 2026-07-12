#!/bin/bash
# gh_pac_pull.sh - resolve the newest successful pacifica-sweep run and OFFLINE-vet its artifacts.
# The collect job commits only a compact addr count; RAW positions/history + live positions live
# in run ARTIFACTS. Idempotent via a pulled-runs ledger. OFF-network (only gh API + CPU vetting).
# Usage: gh_pac_pull.sh [RUN_ID]   (RUN_ID omitted = latest success)
set -u
export HOME=/root
SRC="${SRC:-pac-off}"
NTRIALS="${NTRIALS:-259}"
LEDGER=/root/copyscan/scratch_expand/pulled_pac_runs.txt
touch "$LEDGER"
RUN="${1:-}"
if [ -z "$RUN" ]; then
  RUN=$(gh run list -R Baf-io/copyscan-ghscan -w pacifica-sweep -s success -L 1 --json databaseId -q '.[0].databaseId')
fi
[ -z "$RUN" ] && { echo "no successful pacifica-sweep run"; exit 0; }
grep -q "^$RUN$" "$LEDGER" && { echo "run $RUN already ingested"; exit 0; }
echo "=== offline-vetting pacifica-sweep run $RUN $(date -u +%FT%TZ)"
/opt/bafscaper/venv/bin/python3 /root/copyscan/megadb_pac_offline.py \
    --run "$RUN" --src "$SRC" --n-trials "$NTRIALS" --all \
  && echo "$RUN" >> "$LEDGER"
