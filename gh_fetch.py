#!/usr/bin/env python3
"""gh_fetch.py — HL fetch worker for the GitHub-Actions multi-IP sweep.

STDLIB ONLY (runs on a bare runner, no pip). Each GitHub-Actions runner gets a DISTINCT IP, so
each shard fetches against HL's REST budget independently -> ~N-way parallel throughput with no
single-IP throttling. The runner only does the rate-limited FETCH; it emits raw {addr, fills,
positions} JSON. The LXC (gh_ingest.py) runs the forensic/ruleset vetting, so none of that logic
has to be vendored onto the runner and the two stay in lock-step.

This mirrors scan.py's HL pre-filter + capped-fills EXACTLY (90d / 4 pages) so the GH sweep and the
local scan vet the same population the same way.

env:
  SHARD_INDEX, SHARD_TOTAL  shard k fetches active_pool[k::SHARD_TOTAL]   (from the Actions matrix)
  OUT                       output jsonl path (default shard_<k>.jsonl)
  MAX                       cap addresses (0 = no cap; test convenience)
  DELAY                     per-address spacing seconds (default 2.5; each IP has its own budget)
"""
import os, json, time, urllib.request, urllib.error

INFO = "https://api.hyperliquid.xyz/info"
LB   = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
DAYS, PAGES = 90, 4
MIN_ACCT, MAX_ACCT = 1_000.0, 5_000_000.0
MIN_TO, MAX_TO = 1.0, 80.0
DELAY = float(os.environ.get("DELAY", "2.5"))


def _post(typ, **kw):
    body = json.dumps({"type": typ, **kw}).encode()
    for attempt in range(4):
        try:
            req = urllib.request.Request(INFO, data=body, headers={"Content-Type": "application/json"})
            return json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        except urllib.error.HTTPError as e:
            time.sleep((6 if e.code == 429 else 2) * (attempt + 1))   # brief, bounded backoff
        except Exception:
            time.sleep(2 * (attempt + 1))
    return None


def capped_fills(addr):
    """Windowed + page-capped fills (same 90d/4pg as scan.py's hl_capped_fills)."""
    out, end = [], int(time.time() * 1000)
    cur = end - DAYS * 86400 * 1000
    for _ in range(PAGES):
        j = _post("userFillsByTime", user=addr, startTime=cur, endTime=end)
        if not isinstance(j, list) or not j:
            break
        out += j
        if len(j) < 2000:
            break
        cur = max(f["time"] for f in j) + 1
    seen, d = set(), []
    for f in out:
        k = (f.get("time"), f.get("oid"), f.get("tid"))
        if k in seen:
            continue
        seen.add(k); d.append(f)
    d.sort(key=lambda f: f["time"])
    return d


def active_pool():
    """HL leaderboard -> active/copyable-shaped addresses, in stable leaderboard order.
    Same junk filter as scan.hl_prefilter (dust/MM/inactive/buy-hold/churner dropped)."""
    d = json.loads(urllib.request.urlopen(
        urllib.request.Request(LB, headers={"User-Agent": "ghscan"}), timeout=40).read().decode())
    rows = d.get("leaderboardRows", d) if isinstance(d, dict) else d
    pool = []
    for r in rows:
        a = r.get("ethAddress")
        if not a:
            continue
        try:
            acct = float(r.get("accountValue") or 0)
        except (ValueError, TypeError):
            acct = 0.0
        wp = {w: m for w, m in (r.get("windowPerformances") or [])}
        def _vlm(w):
            try:
                return float((wp.get(w) or {}).get("vlm") or 0)
            except (ValueError, TypeError):
                return 0.0
        mv, wv = _vlm("month"), _vlm("week")
        to = mv / acct if acct > 0 else 0.0
        if MIN_ACCT <= acct <= MAX_ACCT and wv > 0 and MIN_TO <= to <= MAX_TO:
            pool.append(a)
    return pool


def main():
    shard = int(os.environ.get("SHARD_INDEX", "0"))
    total = int(os.environ.get("SHARD_TOTAL", "1"))
    out = os.environ.get("OUT", f"shard_{shard}.jsonl")
    cap = int(os.environ.get("MAX", "0"))
    pool = active_pool()
    mine = pool[shard::total]
    if cap:
        mine = mine[:cap]
    print(f"[shard {shard}/{total}] active_pool={len(pool)} mine={len(mine)} delay={DELAY}s", flush=True)
    n = len(mine)
    with open(out, "w") as f:
        for i, a in enumerate(mine, 1):
            if i > 1:
                time.sleep(DELAY)
            rec = {"addr": a, "fills": capped_fills(a), "positions": _post("clearinghouseState", user=a) or {}}
            f.write(json.dumps(rec) + "\n"); f.flush()
            if i % 25 == 0 or i == n:
                print(f"  [shard {shard}] {i}/{n}", flush=True)
    print(f"[shard {shard}] wrote {out} ({n} addrs)", flush=True)


if __name__ == "__main__":
    main()
