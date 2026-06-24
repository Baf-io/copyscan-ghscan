#!/usr/bin/env python3
"""gh_enum_ingest.py — LXC side of the GH-distributed on-chain enumeration. Reads the aggregated
shard output (results/enum_all.jsonl, committed by the workflow's collect job after a `git pull`),
folds the per-shard counts into a single per-address view, applies the cross-shard MM throttle,
dedups vs every known/committed pool, and APPENDS net-new lowercase addrs to ghscan/extra_addrs.txt
— the prefilter-FREE injection point the fills sweep + downstream pipeline already consume unchanged.

Reuses the canonical helpers from `discover_onchain_hl.py` (known_addrs / POOL / PROBE / MM_FRAC),
so the GH-distributed path and the single-IP daily path dedup + append IDENTICALLY. Venue-agnostic:
the shard rows carry `venue`, so the same ingest serves any chain enumerated by gh_enum.py.

usage:  python3 gh_enum_ingest.py [results/enum_all.jsonl]    (default path shown)
        DISC_APPEND=0  -> report only, don't touch extra_addrs.txt
"""
import sys, os, json, time

HERE   = os.path.dirname(os.path.abspath(__file__))           # .../copyscan/ghscan
PARENT = os.path.dirname(HERE)                                 # .../copyscan
sys.path.insert(0, PARENT)
import coordlib as C
import discover_onchain_hl as D                                # canonical known/append/meta logic

APPEND  = os.environ.get("DISC_APPEND", "1") == "1"
LB_POOL = 4966                                                 # leaderboard pool (headline denom)


def main():
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "results", "enum_all.jsonl")
    if not os.path.exists(src):
        print("no shard output at %s — run the workflow + `git pull` first" % src); sys.exit(1)

    # fold per-shard rows into one per-address view; total blocks sampled = sum of shard_blocks
    # over DISTINCT shards (each shard walks a disjoint height slice).
    seen = {}                       # addr -> [blocks_seen, last_block_time]
    shard_ok = {}                   # shard index -> blocks that shard actually read (counted once)
    rows = 0
    for ln in open(src):
        try:
            r = json.loads(ln)
        except Exception:
            continue
        rows += 1
        a = str(r.get("addr", "")).lower()
        if not a.startswith("0x"):
            continue
        cnt = int(r.get("blocks_seen", 0)); bt = int(r.get("last_block_time", 0) or 0)
        e = seen.get(a)
        if e is None:
            seen[a] = [cnt, bt]
        else:
            e[0] += cnt
            if bt > e[1]:
                e[1] = bt
        # shard_blocks is repeated on every row from that shard; key by shard index -> sum once.
        shard_ok[r.get("shard")] = int(r.get("shard_blocks", 0) or 0)
    total_blocks = sum(shard_ok.values()) or 1

    # cross-shard MM throttle: drop always-on quoters (in > MM_FRAC of all sampled blocks).
    traders = {a: e for a, e in seen.items() if (e[0] / total_blocks) <= D.MM_FRAC}
    n_mm = len(seen) - len(traders)
    print("rows=%d | distinct order-placing addrs=%d | total blocks sampled=%d | dropped MM=%d | "
          "candidate traders=%d" % (rows, len(seen), total_blocks, n_mm, len(traders)), flush=True)

    known, kc = D.known_addrs()
    print("known/committed pools: %s (total %d)" % (kc, len(known)), flush=True)

    net = [a for a in traders if a not in known]
    net.sort(key=lambda a: (-traders[a][1], -traders[a][0]))
    print("NET-NEW (on-chain, not in any known/committed pool): %d" % len(net), flush=True)
    print("HEADLINE: net-new on-chain addrs vs leaderboard pool (%d) = +%d (%.1fx the board)" % (
        LB_POOL, len(net), (len(net) + LB_POOL) / LB_POOL), flush=True)

    now_ms = max((traders[a][1] for a in net), default=int(time.time() * 1000))
    os.makedirs(os.path.dirname(D.PROBE), exist_ok=True)
    with open(D.PROBE, "w") as f:
        for a in net:
            cnt, bt = traders[a]
            f.write(json.dumps({"addr": a, "blocks_seen": cnt,
                                "mm_frac": round(cnt / total_blocks, 4),
                                "last_block_time": bt,
                                "recency_d": round((now_ms - bt) / 86400000.0, 2)}) + "\n")
    print("wrote %s (%d net-new w/ metadata)" % (D.PROBE, len(net)), flush=True)

    if not APPEND:
        print("DISC_APPEND=0 -> report only, extra_addrs.txt untouched."); return
    if not net:
        print("no net-new to append."); return
    with C.lock("onchain.hl"):
        existing = []
        if os.path.exists(D.POOL):
            existing = [l.strip().lower() for l in open(D.POOL) if l.strip().startswith("0x")]
        eset = set(existing)
        added = [a for a in net if a not in eset]
        C.atomic_write_lines(D.POOL, existing + added)
    print("appended %d net-new addrs to %s (pool now %d)" % (
        len(added), os.path.basename(D.POOL), len(existing) + len(added)), flush=True)
    C.stamp("onchain", "gh-enum-ingest", venue="hl", net_new=len(net), appended=len(added),
            pool=len(existing) + len(added), blocks=total_blocks, mm_dropped=n_mm)


if __name__ == "__main__":
    main()
