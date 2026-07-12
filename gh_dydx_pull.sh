#!/bin/bash
# gh_dydx_pull.sh — LXC merge lane for the GH-distributed dYdX v4 enumeration (dydx-enum.yml:
# 20 cloud-IP runners crawl a recent dYdX block window, decode clob owner addrs, upload artifacts).
# ZERO home-IP chain-crawl budget spent — this script only talks to the GitHub API.
# Companion to gh_enum_pull.sh (HL). Separate workflow, separate ledger.
#   1. resolve newest completed dydx-enum run; skip if already ingested (ledger)
#   2. download per-shard ARTIFACTS (source of truth — the committed results/dydx_enum_<runid>.txt
#      is a compact summary only, and the collect commit can lose a push race)
#   3. gh_dydx_enum_ingest.py = extract dydx1 owners + meta, dedup vs roster + prior pool, merge into
#      probes/onchain_dydx_addrs.txt (NEVER touches out/roster.jsonl)
#   4. ntfy the net-new headline to bafscraper-1
# Cron: 0 7 * * * UTC (the 04:15 UTC dydx-enum lands ~04:30). READ-ONLY vs dYdX (no chain crawl here).
set -u
export HOME=/root
export PATH=/usr/local/bin:/usr/bin:/bin:$PATH
cd /root/copyscan/ghscan || exit 1
LEDGER=/root/copyscan/scratch_expand/pulled_dydx_runs.txt
NTFY=https://ntfy.sh/bafscraper-1
ADDRS=/root/copyscan/probes/onchain_dydx_addrs.txt
touch "$LEDGER"
RUN=$(gh run list -R Baf-io/copyscan-ghscan -w dydx-enum -s success -L 1 --json databaseId -q '.[0].databaseId')
[ -z "$RUN" ] && { echo "no successful dydx-enum run"; exit 0; }
grep -q "^$RUN$" "$LEDGER" && { echo "dydx-enum run $RUN already ingested"; exit 0; }
echo "=== $(date -u +%FT%TZ) ingesting dydx-enum run $RUN"
DEST=$(mktemp -d /root/copyscan/scratch_expand/dydx_art.XXXXXX)
trap 'rm -rf "$DEST"' EXIT
gh run download "$RUN" -R Baf-io/copyscan-ghscan -p 'dydx-*' -D "$DEST" || {
  echo "artifact download failed for run $RUN"
  curl -s -H "Title: dydx-enum merge FAILED" -d "run $RUN: artifact download failed" "$NTFY" >/dev/null
  exit 1
}
find "$DEST" -name '*.jsonl' -exec cat {} + > "$DEST/dydx_all.jsonl"
if [ ! -s "$DEST/dydx_all.jsonl" ]; then
  echo "empty dydx-enum output for run $RUN (quiet chain / LCDs down?)"
  curl -s -H "Title: dydx-enum merge" -d "run $RUN: shards produced 0 rows (quiet chain / LCD down?)" "$NTFY" >/dev/null
  echo "$RUN" >> "$LEDGER"
  exit 0
fi
BEFORE=$(grep -c '^dydx1' "$ADDRS" 2>/dev/null || true); BEFORE=${BEFORE:-0}
python3 /root/copyscan/ghscan/gh_dydx_enum_ingest.py "$DEST/dydx_all.jsonl"
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "ingest failed rc=$RC (run NOT ledgered; retries next cycle)"
  curl -s -H "Title: dydx-enum merge FAILED" -d "run $RUN: gh_dydx_enum_ingest.py rc=$RC" "$NTFY" >/dev/null
  exit 1
fi
AFTER=$(grep -c '^dydx1' "$ADDRS" 2>/dev/null || true); AFTER=${AFTER:-0}
NEW=$((AFTER - BEFORE))
echo "$RUN" >> "$LEDGER"
echo "run $RUN: net-new dydx addrs=$NEW pool_now=$AFTER"
curl -s -H "Title: dydx-enum merge" -d "run $RUN: +$NEW net-new dydx addrs -> onchain_dydx_addrs.txt (pool $AFTER). SYBIL entity-dedup required at vet." "$NTFY" >/dev/null
