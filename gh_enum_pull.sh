#!/bin/bash
# gh_enum_pull.sh — LXC merge lane for the GH-distributed on-chain enumeration (hl-enum.yml:
# 20 runner IPs x ~85 explorer blocks = ~1,700 blocks/day vs the LXC sampler's ~85; ZERO home-IP
# explorer budget spent — this script only talks to the GitHub API). Companion to
# gh_pull_latest.sh (the hl-sweep ingest) — separate workflow, separate ledger.
#   1. resolve newest completed onchain-enum run; skip if already ingested (ledger)
#   2. download the per-shard ARTIFACTS (source of truth — the committed results/enum_<runid>.txt
#      is a compact summary only, and the collect commit can lose a push race)
#   3. gh_enum_ingest.py = cross-shard MM filter + dedup vs every known/committed pool + append
#      net-new to ghscan/extra_addrs.txt under the onchain.hl lane lock (same code path as the
#      03:40 daily sampler — identical dedup semantics)
#   4. commit+push extra_addrs.txt so the NEXT 06:00 UTC hl-sweep (runners read the REPO copy of
#      the pool) fetches the new addrs' fills
#   5. ntfy the headline to bafscraper-1
# Cron: 30 9 * * * UTC (results from the 05:35 UTC enum land ~05:50). READ-ONLY vs HL.
set -u
export HOME=/root
export PATH=/usr/local/bin:/usr/bin:/bin:$PATH
cd /root/copyscan/ghscan || exit 1
LEDGER=/root/copyscan/scratch_expand/pulled_enum_runs.txt
NTFY=https://ntfy.sh/bafscraper-1
touch "$LEDGER"
RUN=$(gh run list -R Baf-io/copyscan-ghscan -w onchain-enum -s success -L 1 --json databaseId -q '.[0].databaseId')
[ -z "$RUN" ] && { echo "no successful onchain-enum run"; exit 0; }
grep -q "^$RUN$" "$LEDGER" && { echo "enum run $RUN already ingested"; exit 0; }
echo "=== $(date -u +%FT%TZ) ingesting enum run $RUN"
DEST=$(mktemp -d /root/copyscan/scratch_expand/enum_art.XXXXXX)
trap 'rm -rf "$DEST"' EXIT
gh run download "$RUN" -R Baf-io/copyscan-ghscan -p 'enum-*' -D "$DEST" || {
  echo "artifact download failed for run $RUN"
  curl -s -H "Title: ghscan enum merge FAILED" -d "run $RUN: artifact download failed" "$NTFY" >/dev/null
  exit 1
}
find "$DEST" -name '*.jsonl' -exec cat {} + > "$DEST/enum_all.jsonl"
if [ ! -s "$DEST/enum_all.jsonl" ]; then
  echo "empty enum output for run $RUN"
  curl -s -H "Title: ghscan enum merge" -d "run $RUN: shards produced 0 rows (archive caps / explorer down?)" "$NTFY" >/dev/null
  echo "$RUN" >> "$LEDGER"
  exit 0
fi
# sync the local repo BEFORE merging so the push below rebases nothing big; autostash carries the
# daily sampler's uncommitted pool appends through the pull.
git pull --rebase --autostash -q origin main || echo "pull failed; continuing on local state"
BEFORE=$(grep -c '^0x' extra_addrs.txt 2>/dev/null || true); BEFORE=${BEFORE:-0}
python3 /root/copyscan/ghscan/gh_enum_ingest.py "$DEST/enum_all.jsonl"
RC=$?
if [ "$RC" -ne 0 ]; then
  echo "ingest failed rc=$RC (run NOT ledgered; retries next cycle)"
  curl -s -H "Title: ghscan enum merge FAILED" -d "run $RUN: gh_enum_ingest.py rc=$RC" "$NTFY" >/dev/null
  exit 1
fi
AFTER=$(grep -c '^0x' extra_addrs.txt 2>/dev/null || true); AFTER=${AFTER:-0}
NEW=$((AFTER - BEFORE))
echo "$RUN" >> "$LEDGER"
# push the grown pool so the sweep runners (which read the repo copy) see it next cycle; this
# also carries any sampler appends that were still local-only.
if ! git diff --quiet -- extra_addrs.txt || ! git diff --cached --quiet -- extra_addrs.txt; then
  git add extra_addrs.txt
  git commit -q -m "enum merge run $RUN: +$NEW net-new addrs (pool $AFTER)" || true
  git push -q origin HEAD:main || echo "push raced; the next merge run carries it"
fi
echo "run $RUN: net-new appended=$NEW pool_now=$AFTER"
curl -s -H "Title: ghscan enum merge" -d "run $RUN: +$NEW net-new enum addrs -> extra_addrs.txt (pool $AFTER)" "$NTFY" >/dev/null
