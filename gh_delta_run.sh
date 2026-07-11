#!/bin/bash
# gh_delta_run.sh — one-shot delta ingest of a completed GH-Actions hl-sweep run.
# WHY: the collect job's aggregated results/all.jsonl commit is too big for a git push
# (HTTP 500 on every retry, job still exits green) and the trycloudflare sink is dead,
# so the ONLY copy of the sweep is the 20 per-shard artifacts (~200MB zip each).
# This downloads them ONE AT A TIME (peak disk ~1.5GB), runs the canonical net-stubbed
# gh_ingest_safe.py per shard (GH_INGEST_OUT part files), then atomically assembles
# probes/scan_hl_gh.jsonl — a SEPARATE probe file so the hourly scan.py hl lane can
# never overwrite it before consolidate.py folds it (coord note 2026-06-25).
# Usage: gh_delta_run.sh [RUN_ID]   (default = run 29142099727, the fresher full sweep)
set -u
RUN="${1:-29142099727}"
REPO=Baf-io/copyscan-ghscan
SCRATCH=/root/copyscan/scratch_expand
TMP=$SCRATCH/art_tmp
FINAL=/root/copyscan/probes/scan_hl_gh.jsonl
mkdir -p "$SCRATCH" "$TMP"
cd /root/copyscan/ghscan || exit 1

echo "=== gh_delta_run start $(date -u +%FT%TZ) run=$RUN ==="
gh api "repos/$REPO/actions/runs/$RUN/artifacts" --paginate \
  -q '.artifacts[] | "\(.id) \(.name)"' | sort -k2 -V > "$SCRATCH/artifact_ids.txt"
N=$(wc -l < "$SCRATCH/artifact_ids.txt")
echo "artifacts: $N"
[ "$N" -eq 0 ] && { echo "FATAL: no artifacts listed"; exit 1; }

while read -r id name; do
  part="$SCRATCH/gh_out_${name}.jsonl"
  if [ -s "$part" ]; then echo "skip $name (part exists)"; continue; fi
  echo "--- $name (id=$id) $(date -u +%T) ---"
  rm -rf "$TMP"; mkdir -p "$TMP/x"
  if ! gh api "repos/$REPO/actions/artifacts/$id/zip" > "$TMP/a.zip"; then
    echo "DL_FAIL $name"; continue
  fi
  ls -la "$TMP/a.zip"
  if ! python3 -m zipfile -e "$TMP/a.zip" "$TMP/x"; then echo "UNZIP_FAIL $name"; continue; fi
  f=$(ls "$TMP/x"/*.jsonl 2>/dev/null | head -1)
  [ -z "$f" ] && { echo "NO_JSONL $name"; continue; }
  du -h "$f"
  if ! GH_INGEST_OUT="$part" python3 gh_ingest_safe.py "$f"; then
    echo "INGEST_FAIL $name"; rm -f "$part"
  fi
  rm -rf "$TMP"
done < "$SCRATCH/artifact_ids.txt"

parts=$(ls "$SCRATCH"/gh_out_shard-*.jsonl 2>/dev/null | wc -l)
echo "parts complete: $parts / $N"
if [ "$parts" -eq 0 ]; then echo "FATAL: no parts produced"; exit 1; fi
# atomic assemble (tmp+mv, same fs) — consolidate either sees the full file or none
cat "$SCRATCH"/gh_out_shard-*.jsonl > "$SCRATCH/.scan_hl_gh.assemble"
mv "$SCRATCH/.scan_hl_gh.assemble" "$FINAL"
echo "assembled: $(wc -l < "$FINAL") probe rows -> $FINAL"
echo "=== gh_delta_run done $(date -u +%FT%TZ) ==="
