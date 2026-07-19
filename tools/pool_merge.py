#!/usr/bin/env python3
"""pool_merge.py — GH-side pool grower. Replaces the LXC gh_enum_ingest step so the address pool
grows entirely inside Actions (no home box in the loop).

Faithful port of gh_enum_ingest.main(): folds per-shard enum rows into one per-address view, applies
the cross-shard MM throttle (drop always-on quoters seen in more than DISC_MM_FRAC of ALL sampled
blocks), dedups against the committed pools, and appends net-new addrs to extra_addrs.txt.
Drops only the LXC-only bits (probe file + coordlib stamp/lock — single runner, no lane contention).

Usage: pool_merge.py <artifacts_dir> [--pool extra_addrs.txt] [--known pool_full.txt ...]
"""
import json, os, glob, argparse

MM_FRAC = float(os.environ.get("DISC_MM_FRAC", "0.6"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("artifacts")
    ap.add_argument("--pool", default="extra_addrs.txt")
    ap.add_argument("--known", nargs="*", default=["pool_full.txt"])
    args = ap.parse_args()

    seen, shard_blocks, rows = {}, {}, 0
    for p in glob.glob(os.path.join(args.artifacts, "**", "*.jsonl"), recursive=True):
        for ln in open(p):
            try:
                r = json.loads(ln)
            except Exception:
                continue
            rows += 1
            a = str(r.get("addr", "")).lower()
            if not a.startswith("0x"):
                continue
            cnt = int(r.get("blocks_seen", 0) or 0)
            bt = int(r.get("last_block_time", 0) or 0)
            e = seen.get(a)
            if e is None:
                seen[a] = [cnt, bt]
            else:
                e[0] += cnt
                if bt > e[1]:
                    e[1] = bt
            # shard_blocks repeats on every row of a shard -> key by shard index, counted once
            shard_blocks[r.get("shard")] = int(r.get("shard_blocks", 0) or 0)
    total_blocks = sum(shard_blocks.values()) or 1

    traders = {a: e for a, e in seen.items() if (e[0] / total_blocks) <= MM_FRAC}
    n_mm = len(seen) - len(traders)
    print("rows=%d distinct=%d blocks_sampled=%d MM_dropped=%d candidate_traders=%d"
          % (rows, len(seen), total_blocks, n_mm, len(traders)), flush=True)

    known = set()
    for kp in [args.pool] + list(args.known or []):
        if os.path.exists(kp):
            for l in open(kp):
                l = l.strip().lower()
                if l.startswith("0x"):
                    known.add(l)

    existing = []
    if os.path.exists(args.pool):
        existing = [l.strip().lower() for l in open(args.pool) if l.strip().startswith("0x")]
    eset = set(existing)

    net = [a for a in traders if a not in known]
    net.sort(key=lambda a: (-traders[a][1], -traders[a][0]))   # freshest, then most active
    added = [a for a in net if a not in eset]
    if added:
        with open(args.pool, "w") as f:
            f.write("\n".join(existing + added) + "\n")
    print("appended %d net-new to %s (pool %d -> %d)"
          % (len(added), args.pool, len(existing), len(existing) + len(added)), flush=True)


if __name__ == "__main__":
    main()
