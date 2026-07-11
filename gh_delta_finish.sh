#!/bin/bash
# gh_delta_finish.sh — chained finisher for the gh-sweep delta ingest (newedge-verify lane).
# 1) wait for gh-delta-ingest (run-2) to finish  2) run-1 gap-fill (unseen addrs only)
# 3) wait for the next hourly consolidate to fold probes into out/roster.jsonl
# 4) diff vs scratch_expand/roster_before.txt + exclusions -> delta_new_addrs.jsonl
# 5) append coord/CHANNEL.md line + DELTA_DONE.txt status
set -u
S=/root/copyscan/scratch_expand
LOG=$S/ingest.log
echo "=== finisher start $(date -u +%FT%TZ) ===" >> "$LOG"

# 1: wait for run-2 assemble (max 40min)
for i in $(seq 1 80); do
  grep -q "gh_delta_run done" "$LOG" && break
  sleep 30
done
if ! grep -q "gh_delta_run done" "$LOG"; then
  echo "FINISH_ABORT: run-2 ingest never finished" >> "$LOG"; exit 1
fi

# 2: run-1 gap-fill (addrs unseen since run-2 ingest start 12:40Z)
SINCE=$(date -d "2026-07-11T12:40:00Z" +%s)
bash /root/copyscan/ghscan/gh_delta_run1_fill.sh 29127256058 "$SINCE" >> "$LOG" 2>&1

# 3: wait for the NEXT consolidate fold (roster mtime newer than now; max 100min)
T0=$(date +%s)
touch "$S/.wait_marker"
for i in $(seq 1 200); do
  [ /root/copyscan/out/roster.jsonl -nt "$S/.wait_marker" ] && break
  sleep 30
done
sleep 60   # let consolidate/copysim settle

# 4: delta diff + exclusions
python3 - <<'PYEOF' >> "$LOG" 2>&1
import json, os
S = "/root/copyscan/scratch_expand"
O = "/root/copyscan/out"
def addrs_of(path, key="addr"):
    s = set()
    try:
        for line in open(path):
            line = line.strip()
            if not line: continue
            try: j = json.loads(line)
            except Exception: continue
            a = (j.get(key) or j.get("address") or "")
            if isinstance(a, str) and a: s.add(a.lower())
    except FileNotFoundError:
        pass
    return s
before = set(l.strip().lower() for l in open(f"{S}/roster_before.txt") if l.strip())
after = addrs_of(f"{O}/roster.jsonl")
new = after - before
excl = set()
for f in ["megadb.jsonl", "lead_bench.jsonl", "losshider_block.jsonl",
          "drainer_block.jsonl", "elite_p1.jsonl", "elite_p2.jsonl", "elite_p3.jsonl"]:
    excl |= addrs_of(f"{O}/{f}")
excl.add("0xb33040b2618ffb4afafbd1afdfeff29c3d08d3c8")
final = sorted(new - excl)
with open(f"{S}/delta_new_addrs.jsonl", "w") as f:
    for a in final:
        f.write(json.dumps({"addr": a, "venue": "hl", "src": "gh-delta"}) + "\n")
msg = (f"roster before={len(before)} after={len(after)} rawnew={len(new)} "
       f"excluded={len(new)-len(final)} delta_new={len(final)}")
print("DELTA:", msg)
open(f"{S}/DELTA_DONE.txt", "w").write(msg + "\n")
PYEOF

# 5: CHANNEL.md line
D=$(cat "$S/DELTA_DONE.txt" 2>/dev/null || echo "delta diff failed - see scratch_expand/ingest.log")
echo "- $(date -u +"%Y-%m-%d %H:%M:%S")  **newedge-verify**  gh-sweep delta ingest: collect-job push to results/all.jsonl fails (HTTP 500, pack too big) + trycloudflare sink dead -> pulled run 29142099727 (+29127256058 gap-fill) shard artifacts one-at-a-time, gh_ingest_safe per shard -> probes/scan_hl_gh.jsonl (separate file per coord 2026-06-25 note, no scan.py collision). $D. Candidates: scratch_expand/delta_new_addrs.jsonl" >> /root/copyscan/coord/CHANNEL.md
echo "=== finisher done $(date -u +%FT%TZ) ===" >> "$LOG"
