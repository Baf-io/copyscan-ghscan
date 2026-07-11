#!/bin/bash
# gh_delta_run1_fill.sh — completeness gap-fill from the OLDER sweep run (29127256058).
# Both 2026-07-10/11 runs used the same extra_addrs.txt pool, but each also prepends the LIVE
# leaderboard at trigger time; addrs on the 22:16Z board that dropped off by 06:00Z exist ONLY
# in run 1. This downloads run 1's shards, filters each to addrs NOT already vetted today
# (swept_ledger since run 2 ingest start), ingests only those, and APPENDS the parts into
# probes/scan_hl_gh.jsonl atomically.
set -u
RUN="${1:-29127256058}"
SINCE_TS="${2:?usage: gh_delta_run1_fill.sh RUN_ID SINCE_EPOCH}"
REPO=Baf-io/copyscan-ghscan
SCRATCH=/root/copyscan/scratch_expand
TMP=$SCRATCH/art_tmp_r1
FINAL=/root/copyscan/probes/scan_hl_gh.jsonl
LEDGER=/root/copyscan/ghscan/out/swept_ledger.jsonl
mkdir -p "$SCRATCH" "$TMP"
cd /root/copyscan/ghscan || exit 1

echo "=== gh_delta_run1_fill start $(date -u +%FT%TZ) run=$RUN since=$SINCE_TS ==="
python3 - "$LEDGER" "$SINCE_TS" "$SCRATCH/seen_today.txt" <<'PYEOF'
import sys, json
ledger, since, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
seen = set()
for line in open(ledger):
    line = line.strip()
    if not line: continue
    try: j = json.loads(line)
    except Exception: continue
    if j.get("ts", 0) >= since and j.get("addr"):
        seen.add(j["addr"].lower())
open(out, "w").write("\n".join(sorted(seen)) + "\n")
print("seen-today addrs:", len(seen))
PYEOF

gh api "repos/$REPO/actions/runs/$RUN/artifacts" --paginate \
  -q '.artifacts[] | "\(.id) \(.name)"' | sort -k2 -V > "$SCRATCH/artifact_ids_r1.txt"
N=$(wc -l < "$SCRATCH/artifact_ids_r1.txt")
echo "artifacts: $N"
[ "$N" -eq 0 ] && { echo "FATAL: no artifacts listed"; exit 1; }

while read -r id name; do
  part="$SCRATCH/gh_out_r1_${name}.jsonl"
  marker="$SCRATCH/.done_r1_${name}"
  if [ -e "$marker" ]; then echo "skip $name (done)"; continue; fi
  echo "--- r1 $name (id=$id) $(date -u +%T) ---"
  rm -rf "$TMP"; mkdir -p "$TMP/x"
  if ! gh api "repos/$REPO/actions/artifacts/$id/zip" > "$TMP/a.zip"; then
    echo "DL_FAIL $name"; continue
  fi
  if ! python3 -m zipfile -e "$TMP/a.zip" "$TMP/x"; then echo "UNZIP_FAIL $name"; continue; fi
  f=$(ls "$TMP/x"/*.jsonl 2>/dev/null | head -1)
  [ -z "$f" ] && { echo "NO_JSONL $name"; continue; }
  python3 - "$f" "$SCRATCH/seen_today.txt" "$TMP/filtered.jsonl" <<'PYEOF'
import sys, json
src, seenf, dst = sys.argv[1], sys.argv[2], sys.argv[3]
seen = set(l.strip() for l in open(seenf) if l.strip())
kept = tot = 0
with open(dst, "w") as o:
    for line in open(src):
        line = line.strip()
        if not line: continue
        try: j = json.loads(line)
        except Exception: continue
        tot += 1
        if (j.get("addr") or "").lower() in seen: continue
        o.write(line + "\n"); kept += 1
print("filtered: %d/%d unseen" % (kept, tot))
PYEOF
  if [ -s "$TMP/filtered.jsonl" ]; then
    if GH_INGEST_OUT="$part" python3 gh_ingest_safe.py "$TMP/filtered.jsonl"; then
      touch "$marker"
    else
      echo "INGEST_FAIL $name"; rm -f "$part"
    fi
  else
    echo "no unseen addrs in $name"; touch "$marker"
  fi
  rm -rf "$TMP"
done < "$SCRATCH/artifact_ids_r1.txt"

# append r1 parts into the final probes file atomically
if ls "$SCRATCH"/gh_out_r1_shard-*.jsonl >/dev/null 2>&1; then
  cat "$FINAL" "$SCRATCH"/gh_out_r1_shard-*.jsonl > "$SCRATCH/.scan_hl_gh.assemble" 2>/dev/null \
    || cat "$SCRATCH"/gh_out_r1_shard-*.jsonl > "$SCRATCH/.scan_hl_gh.assemble"
  mv "$SCRATCH/.scan_hl_gh.assemble" "$FINAL"
  echo "assembled(with r1): $(wc -l < "$FINAL") probe rows -> $FINAL"
else
  echo "no r1 parts to append"
fi
echo "=== gh_delta_run1_fill done $(date -u +%FT%TZ) ==="
