#!/usr/bin/env python3
"""gh_pac_fetch.py — OFF-network Pacifica per-account fill fetcher (GitHub-Actions cloud runner).

Runs on a GH-Actions runner (a DISTINCT public IP with its own Pacifica rate budget), one of
SHARD_TOTAL shards. Each shard independently:
  1. GETs the full public leaderboard (api.pacifica.fi/api/v1/leaderboard — one call, ~6.8k rows,
     no auth), applies the SAME deterministic scan-set filter (equity>=MIN_EQUITY & all_time_pnl>0
     & vol_30d>MIN_VOL30), sorts by address (stable), takes its disjoint slice addrs[shard::total].
  2. For each addr in the slice: cursor-paginates /api/v1/positions/history (max_pages=MAX_PAGES,
     matching PacificaAdapter._history so the LXC replay normalizes the SAME tape the on-network
     vet would have) + one GET /api/v1/positions (live open book, for the refute stage).
  3. Emits one RAW record per addr to $OUT (ndjson):
        {"addr": <sol_pubkey>, "raw": [<positions/history rows>], "positions": [<live rows>]}

RAW ONLY — no vetting here. The LXC offline driver (megadb_pac_offline.py) seeds the canonical
PacificaAdapter cache with `raw` and calls the adapter's get_fills() to normalize -> HL-shaped
fills -> megadb_vet.vet(addr,"pacifica",...,fills=). ONE normalization authority (the adapter);
the cloud is a dumb fetcher. Solana base58 addrs are CASE-SENSITIVE — never lowercase them.
"""
import json, os, sys, time, urllib.request, urllib.error

BASE = "https://api.pacifica.fi/api/v1"
HDRS = {"User-Agent": "Mozilla/5.0"}

SHARD = int(os.environ.get("SHARD_INDEX", "0"))
TOTAL = int(os.environ.get("SHARD_TOTAL", "20"))
OUT = os.environ.get("OUT", f"pac_shard_{SHARD}.jsonl")
DELAY = float(os.environ.get("DELAY", "1.5"))        # between accounts
PAGE_DELAY = float(os.environ.get("PAGE_DELAY", "0.35"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "15"))    # == PacificaAdapter._history default
MIN_EQUITY = float(os.environ.get("MIN_EQUITY", "1000"))
MIN_VOL30 = float(os.environ.get("MIN_VOL30", "0"))   # strict directive gate = vol_30d > 0


def _get(url, timeout=30, retries=7):
    """GET with UA + robust 429 backoff (Pacifica = credit-based 60s rolling window)."""
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                ra = e.headers.get("Retry-After") if e.headers else None
                try:
                    wait = float(ra) if ra else min(2.0 * (2 ** i), 30.0)
                except (ValueError, TypeError):
                    wait = min(2.0 * (2 ** i), 30.0)
                time.sleep(wait)
                continue
            if e.code in (400, 404):
                return {"success": False, "data": []}
            time.sleep(0.6 * (i + 1))
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    if last:
        raise last
    return {"data": []}


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def scan_set():
    d = _get(f"{BASE}/leaderboard").get("data") or []
    keep = []
    for r in d:
        a = r.get("address")
        if not a:
            continue
        if _f(r.get("equity_current")) < MIN_EQUITY:
            continue
        if _f(r.get("pnl_all_time")) <= 0:
            continue
        if _f(r.get("volume_30d")) <= MIN_VOL30:
            continue
        keep.append(a)
    keep = sorted(set(keep))                 # stable, disjoint sharding
    mine = keep[SHARD::TOTAL]
    return keep, mine


def history(addr):
    rows, cursor = [], None
    for _ in range(MAX_PAGES):
        url = f"{BASE}/positions/history?account={addr}&limit=200"
        if cursor:
            url += f"&cursor={cursor}"
        d = _get(url)
        page = d.get("data") or []
        rows += page
        cursor = d.get("next_cursor")
        if not d.get("has_more") or not cursor or not page:
            break
        time.sleep(PAGE_DELAY)
    return rows


def live_positions(addr):
    return _get(f"{BASE}/positions?account={addr}").get("data") or []


def main():
    full, mine = scan_set()
    sys.stderr.write(f"shard {SHARD}/{TOTAL}: scan_set={len(full)} mine={len(mine)}\n")
    n = 0
    with open(OUT, "w") as f:
        for addr in mine:
            try:
                raw = history(addr)
            except Exception as e:
                sys.stderr.write(f"HIST-FAIL {addr}: {repr(e)[:120]}\n")
                raw = []
            try:
                pos = live_positions(addr)
            except Exception:
                pos = []
            f.write(json.dumps({"addr": addr, "raw": raw, "positions": pos}) + "\n")
            f.flush()
            n += 1
            if n % 10 == 0:
                sys.stderr.write(f"shard {SHARD}: {n}/{len(mine)} done\n")
            time.sleep(DELAY)
    sys.stderr.write(f"shard {SHARD}: wrote {n} records -> {OUT}\n")


if __name__ == "__main__":
    main()
