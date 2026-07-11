#!/bin/bash
# gh_pull_latest.sh - daily artifact-based ingest of the newest completed hl-sweep run.
# The collect job now commits only a compact addr summary (push-size safe); raw per-shard
# records live in run ARTIFACTS. Resolve latest success, skip already-ingested runs
# (pulled_runs.txt ledger), hand off to gh_delta_run.sh (idempotent, one artifact at a time).
set -u
export HOME=/root
cd /root/copyscan/ghscan || exit 1
LEDGER=/root/copyscan/scratch_expand/pulled_runs.txt
touch "$LEDGER"
RUN=$(gh run list -R Baf-io/copyscan-ghscan -w hl-sweep -s success -L 1 --json databaseId -q '.[0].databaseId')
[ -z "$RUN" ] && { echo "no successful run"; exit 0; }
grep -q "^$RUN$" "$LEDGER" && { echo "run $RUN already ingested"; exit 0; }
echo "=== ingesting run $RUN $(date -u +%FT%TZ)"
bash /root/copyscan/ghscan/gh_delta_run.sh "$RUN" && echo "$RUN" >> "$LEDGER"
