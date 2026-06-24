#!/usr/bin/env python3
"""gh_enum.py — venue-pluggable ON-CHAIN ADDRESS ENUMERATION worker for the GitHub-Actions
multi-IP harness. STDLIB ONLY (runs on a bare runner, no pip).

WHY THIS EXISTS: enumerating the full on-chain trader universe from a single IP is rate-walled
(HL's explorer caps `blockDetails` at 100 ARCHIVED blocks/day/IP). Each GitHub-Actions runner is a
DISTINCT IP with its OWN budget, so N shards = N× the per-IP cap. 20 shards × ~90 blocks ≈ 1,800
blocks/sweep → the active universe in one pass instead of accumulating ~85/day on the LXC.

The runner ONLY enumerates addresses (+ light per-addr metadata: blocks_seen, last_block_time). The
LXC `gh_enum_ingest.py` does the cross-shard MM-filter, dedup vs known, and append to
`extra_addrs.txt`. THE FRAMEWORK: add a chain = add ONE `enum_<venue>(...)` function + register it
in ENUMERATORS. Everything else (sharding, artifacting, ingest, dedup, the downstream pipeline) is
venue-agnostic and already built.

env: VENUE (default hl) · SHARD_INDEX · SHARD_TOTAL · ENUM_DAYS · ENUM_SAMPLES (GLOBAL count across
     all shards) · ENUM_DELAY · OUT
"""
import os, json, time, urllib.request, urllib.error


def _post(url, body, tries=3):
    """JSON POST with gentle, capped backoff. Returns parsed JSON or None."""
    data = json.dumps(body).encode()
    for a in range(tries):
        try:
            req = urllib.request.Request(url, data=data,
                                         headers={"Content-Type": "application/json",
                                                  "User-Agent": "ghenum/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if a == tries - 1:
                return None
            time.sleep(3.0 if e.code == 429 else 1.5)
        except Exception:
            if a == tries - 1:
                return None
            time.sleep(1.5)
    return None


def _shard_heights(tip, start, samples, shard, total):
    """The GLOBAL list of `samples` heights spread across [start, tip], then THIS shard's disjoint
    interleaved slice — so the 20 runners cover 20 distinct sub-grids with no wasted overlap."""
    span = tip - start
    g = [start + (span * i) // (samples - 1) for i in range(samples)] if samples > 1 else [tip]
    return sorted(set(g))[shard::total]


# ---------------------------------------------------------------------------
# HL enumerator — the explorer block index. Calibrates the window from FREE userDetails
# timestamps (NOT under the archived-block cap), then walks this shard's block slice.
# ---------------------------------------------------------------------------
HL_EXPLORER = "https://rpc.hyperliquid.xyz/explorer"
HL_ORDER_ACTIONS = {"order", "batchModify", "modify", "twapOrder", "batchOrder", "twapCancel"}
HL_SEEDS = [
    "0x31ca8395cf837de08b24da3f660e77761dfb974b",
    "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",   # HLP vault
    "0x010461c14e146ac35fe42271bdc1134ee31c703a",   # HL liquidator vault
]


def enum_hl(shard, total, days, samples, delay):
    tip, pts = 0, []
    for s in HL_SEEDS:
        ud = _post(HL_EXPLORER, {"type": "userDetails", "user": s})
        if ud and ud.get("txs"):
            for t in ud["txs"]:
                b, tm = t.get("block"), t.get("time")
                if b and tm:
                    pts.append((b, tm)); tip = max(tip, b)
    if not tip:
        return {}, 0, "no-tip"
    bps = 14.5
    if len(pts) >= 2:
        lo = min(pts, key=lambda p: p[0]); hi = max(pts, key=lambda p: p[0])
        if hi[0] - lo[0] > 50_000 and hi[1] - lo[1] > 0:
            bps = (hi[0] - lo[0]) / ((hi[1] - lo[1]) / 1000.0)
    start = max(tip - int(days * 86400 * bps), 1)
    heights = _shard_heights(tip, start, samples, shard, total)

    seen, ok, note = {}, 0, "done"
    for h in heights:
        j = _post(HL_EXPLORER, {"type": "blockDetails", "height": h})
        if isinstance(j, dict) and j.get("type") == "error":
            if "archived" in str(j.get("message", "")).lower():
                note = "archive-cap-hit"; break   # this IP's daily budget is spent
            time.sleep(delay); continue
        if not j:
            time.sleep(delay); continue
        bd = j.get("blockDetails", j)
        if not isinstance(bd, dict) or "txs" not in bd:
            time.sleep(delay); continue
        ok += 1
        bt = bd.get("blockTime", 0)
        placers = set()
        for tx in bd.get("txs", []):
            if (tx.get("action") or {}).get("type") in HL_ORDER_ACTIONS:
                u = tx.get("user")
                if u:
                    placers.add(u.lower())
        for u in placers:
            e = seen.get(u)
            if e is None:
                seen[u] = [1, bt]
            else:
                e[0] += 1
                if bt > e[1]:
                    e[1] = bt
        time.sleep(delay)
    return seen, ok, note


# register chains here — each is (shard,total,days,samples,delay) -> ({addr:[blocks_seen,last_bt]}, ok, note)
ENUMERATORS = {"hl": enum_hl}


def main():
    venue = os.environ.get("VENUE", "hl")
    shard = int(os.environ.get("SHARD_INDEX", "0"))
    total = int(os.environ.get("SHARD_TOTAL", "20"))
    days = float(os.environ.get("ENUM_DAYS", "14"))
    samples = int(os.environ.get("ENUM_SAMPLES", "1800"))
    delay = float(os.environ.get("ENUM_DELAY", "0.8"))
    out = os.environ.get("OUT", f"enum_{venue}_shard_{shard}.jsonl")

    fn = ENUMERATORS.get(venue)
    if not fn:
        print(f"no enumerator registered for venue '{venue}' (have: {sorted(ENUMERATORS)})")
        open(out, "w").close()
        return
    t0 = time.time()
    seen, ok, note = fn(shard, total, days, samples, delay)
    with open(out, "w") as f:
        for a, (cnt, bt) in seen.items():
            f.write(json.dumps({"addr": a, "venue": venue, "blocks_seen": cnt,
                                "last_block_time": bt, "shard": shard, "shard_blocks": ok}) + "\n")
    print("[%s shard %d/%d] blocks_ok=%d distinct_addrs=%d note=%s [%.0fs] -> %s" % (
        venue, shard, total, ok, len(seen), note, time.time() - t0, out), flush=True)


if __name__ == "__main__":
    main()
