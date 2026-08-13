#!/usr/bin/env python3
"""pool_merge.py — GH-side pool grower + SELF-CLEANER. Replaces the LXC gh_enum_ingest step so the
address pool grows (and prunes) entirely inside Actions (no home box in the loop).

Faithful port of gh_enum_ingest.main(): folds per-shard enum rows into one per-address view, applies
the cross-shard MM throttle (drop always-on quoters seen in more than DISC_MM_FRAC of ALL sampled
blocks), dedups against the committed pools, and appends net-new addrs to extra_addrs.txt.

SELF-CLEANING (added 2026-08-13 — the pool had ballooned to 67.5k addrs and blew the hl-sweep 180-min
job timeout every day). The pool is now a BOUNDED, freshness-ordered window instead of an append-only
pile:
  * pool_meta.jsonl  — freshness ledger, one row per ACTIVE addr {addr,last_seen,first_seen,hits}.
                       last_seen = most recent on-chain block time we've observed the addr placing an
                       order. Refreshed every enum run; an addr that keeps trading keeps its slot.
  * PRUNE policy (env-tunable):
      POOL_STALE_DAYS (45)  drop addrs not seen on-chain in this many days  -> they went quiet.
      POOL_MAX       (12000) hard cap; if still over, keep the freshest POOL_MAX by last_seen.
  * archive/pool_retired.jsonl — append-only RESEARCH STORE. Every evicted addr is preserved here
                       with its last-known meta + reason. Nothing is deleted; it just stops being
                       swept. Analyze / survivorship-mine it later; sweep it to the LXC if it grows.
  * BOOTSTRAP: on the first run (no pool_meta.jsonl yet) the existing flat pool is seeded with
                       last_seen=now (grace, so a one-off sample gap can't nuke a live trader), then
                       the cap is applied keeping the file's tail (= most-recently-DISCOVERED addrs,
                       since every prior run appended net-new at the bottom). Genuinely-active addrs
                       that fall outside the bootstrap window get re-surfaced by ongoing enum within
                       days, so the bound is self-healing.

Runs fine with an EMPTY artifacts dir (no enum rows) — it then just applies prune/cap/bootstrap to
the current pool. That's how the one-time reseed is kicked off.

Usage: pool_merge.py <artifacts_dir> [--pool extra_addrs.txt] [--known pool_full.txt ...]
env:   DISC_MM_FRAC(0.6) POOL_MAX(12000) POOL_STALE_DAYS(45) POOL_META(pool_meta.jsonl)
       POOL_ARCHIVE(archive/pool_retired.jsonl)
"""
import json, os, glob, argparse, time

MM_FRAC     = float(os.environ.get("DISC_MM_FRAC", "0.6"))
POOL_MAX    = int(os.environ.get("POOL_MAX", "12000"))
STALE_DAYS  = float(os.environ.get("POOL_STALE_DAYS", "45"))
META_PATH   = os.environ.get("POOL_META", "pool_meta.jsonl")
ARCH_PATH   = os.environ.get("POOL_ARCHIVE", "archive/pool_retired.jsonl")

NOW_MS = int(time.time() * 1000)
DAY_MS = 86400000.0


def _load_pool(path):
    if not os.path.exists(path):
        return []
    return [l.strip().lower() for l in open(path) if l.strip().lower().startswith("0x")]


def _load_meta(path):
    """addr -> {last_seen, first_seen, hits}. Tolerant of a missing/partial file."""
    meta = {}
    if os.path.exists(path):
        for ln in open(path):
            try:
                r = json.loads(ln)
            except Exception:
                continue
            a = str(r.get("addr", "")).lower()
            if a.startswith("0x"):
                meta[a] = {"last_seen": int(r.get("last_seen", 0) or 0),
                           "first_seen": int(r.get("first_seen", 0) or 0),
                           "hits": int(r.get("hits", 0) or 0)}
    return meta


def _atomic_write(path, text):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


def _archive(evicted, reason):
    """Append evicted {addr: meta} to the research store, deduped on addr (latest wins)."""
    if not evicted:
        return
    os.makedirs(os.path.dirname(ARCH_PATH) or ".", exist_ok=True)
    have = set()
    if os.path.exists(ARCH_PATH):
        for ln in open(ARCH_PATH):
            try:
                have.add(json.loads(ln).get("addr"))
            except Exception:
                pass
    with open(ARCH_PATH, "a") as f:
        for a, m in evicted.items():
            if a in have:
                continue
            f.write(json.dumps({"addr": a, "reason": reason,
                                "last_seen": m.get("last_seen", 0),
                                "first_seen": m.get("first_seen", 0),
                                "hits": m.get("hits", 0),
                                "retired_at": NOW_MS}) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("artifacts")
    ap.add_argument("--pool", default="extra_addrs.txt")
    ap.add_argument("--known", nargs="*", default=["pool_full.txt"])
    args = ap.parse_args()

    # ---- 1. fold this run's enum rows into a per-address view (unchanged logic) ----
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
            shard_blocks[r.get("shard")] = int(r.get("shard_blocks", 0) or 0)
    total_blocks = sum(shard_blocks.values()) or 1

    traders = {a: e for a, e in seen.items() if (e[0] / total_blocks) <= MM_FRAC}
    n_mm = len(seen) - len(traders)
    print("rows=%d distinct=%d blocks_sampled=%d MM_dropped=%d candidate_traders=%d"
          % (rows, len(seen), total_blocks, n_mm, len(traders)), flush=True)

    # ---- 2. load current pool + freshness meta ----
    known = set()
    for kp in list(args.known or []):
        for l in _load_pool(kp):
            known.add(l)

    pool = _load_pool(args.pool)          # ordered oldest-discovered -> newest (tail = freshest)
    meta = _load_meta(META_PATH)
    bootstrap = not os.path.exists(META_PATH)

    # bootstrap: seed meta for every legacy pool addr with a graced last_seen so the FIRST prune
    # can't wipe a live trader on a one-off sample gap. Tail-of-file = most recently discovered, so
    # give later lines a fractionally newer stamp to make the cap keep the freshest end.
    if bootstrap:
        n = len(pool)
        for i, a in enumerate(pool):
            meta[a] = {"last_seen": NOW_MS - (n - i), "first_seen": NOW_MS - (n - i), "hits": 1}
        print("BOOTSTRAP: seeded meta for %d legacy pool addrs (last_seen=now)" % n, flush=True)

    # ---- 3. fold this run's on-chain sightings into the freshness ledger ----
    for a, (cnt, bt) in traders.items():
        ts = bt if bt > 0 else NOW_MS
        m = meta.get(a)
        if m is None:
            meta[a] = {"last_seen": ts, "first_seen": ts, "hits": 1}
        else:
            m["last_seen"] = max(m["last_seen"], ts)
            m["first_seen"] = min(m["first_seen"] or ts, ts)
            m["hits"] = m.get("hits", 0) + 1

    # ---- 4. net-new addrs (this run, not in any known pool, not already in pool) ----
    pset = set(pool)
    net = [a for a in traders if a not in known and a not in pset]
    net.sort(key=lambda a: (-traders[a][1], -traders[a][0]))   # freshest, then most active
    pool = pool + net
    pset = set(pool)
    if net:
        print("appended %d net-new candidates (pool %d -> %d)"
              % (len(net), len(pool) - len(net), len(pool)), flush=True)

    # drop any meta rows no longer referenced by the pool (keeps the ledger bounded)
    meta = {a: meta[a] for a in pset if a in meta}
    # any pool addr still lacking meta (shouldn't happen post-bootstrap) -> grace-stamp it
    for a in pset:
        meta.setdefault(a, {"last_seen": NOW_MS, "first_seen": NOW_MS, "hits": 1})

    # ---- 5. PRUNE: stale-out, then hard-cap. Evicted -> research archive. ----
    stale_cut = NOW_MS - STALE_DAYS * DAY_MS
    evicted = {}
    fresh = []
    for a in pool:
        if meta[a]["last_seen"] < stale_cut:
            evicted[a] = meta[a]
        else:
            fresh.append(a)
    if evicted:
        _archive(evicted, "stale>%.0fd" % STALE_DAYS)
        print("pruned %d STALE addrs (last_seen > %.0fd ago) -> %s"
              % (len(evicted), STALE_DAYS, ARCH_PATH), flush=True)

    # order freshest-LAST (preserve the tail=freshest convention downstream expects)
    fresh.sort(key=lambda a: meta[a]["last_seen"])
    capped = {}
    if len(fresh) > POOL_MAX:
        overflow = fresh[:len(fresh) - POOL_MAX]     # oldest end
        fresh = fresh[len(fresh) - POOL_MAX:]
        capped = {a: meta[a] for a in overflow}
        _archive(capped, "cap>%d" % POOL_MAX)
        print("capped pool to POOL_MAX=%d (archived %d oldest) -> %s"
              % (POOL_MAX, len(capped), ARCH_PATH), flush=True)

    keep = set(fresh)
    meta = {a: meta[a] for a in keep}

    # ---- 6. atomic write pool + meta ----
    _atomic_write(args.pool, "\n".join(fresh) + ("\n" if fresh else ""))
    _atomic_write(META_PATH, "".join(
        json.dumps({"addr": a, **meta[a]}) + "\n" for a in fresh))
    print("pool now %d active (cap %d, stale %.0fd); meta %s; archive %s"
          % (len(fresh), POOL_MAX, STALE_DAYS, META_PATH, ARCH_PATH), flush=True)


if __name__ == "__main__":
    main()
