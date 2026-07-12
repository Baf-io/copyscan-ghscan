#!/usr/bin/env python3
"""gh_lighter_book.py -- cloud-runner Lighter LIVE-BOOK snapshot fetcher (one shard).

Runs on a GitHub-Actions runner (a DISTINCT cloud IP, OFF Roel's home network).
Reads lighter_ids.txt (the committed active-account id list), takes this shard's
stride slice (ids[SHARD_INDEX::SHARD_TOTAL]), and for each active account_id GETs
the PUBLIC, no-auth account snapshot
    GET /api/v1/account?by=index&value=<id>
distilling it into a COMPACT live-book record for the offline discipline /
loss-hider gate (lighter_book_vet.py on the LXC).

WHY snapshot-only: Lighter's per-account fill HISTORY is auth-gated (own-account
only), so there is NO realized backfill. What IS public is the live open book
(positions carry sign, avg_entry_price, position_value=notional, unrealized_pnl,
realized_pnl, liquidation_price). This sweep is the preventive 'catch live
drainers / bag-holders NOW' layer; realized-edge vetting stays FORWARD-only via
the LXC lighter-collect tape logger.

Raw per-shard output stays in the run ARTIFACT (never committed -- the hl-sweep
transport lesson). Stdlib only. Gentle: DELAY>=1.05s keeps <60 req/min (Lighter's
per-IP rate limit).

Env: SHARD_INDEX, SHARD_TOTAL, IDS_FILE, OUT, DELAY.
"""
import os, sys, json, time, urllib.request, urllib.error

BASE     = "https://mainnet.zklighter.elliot.ai/api/v1"
IDS_FILE = os.environ.get("IDS_FILE", "lighter_ids.txt")
SHARD    = int(os.environ.get("SHARD_INDEX", "0"))
NSHARD   = int(os.environ.get("SHARD_TOTAL", "20"))
OUT      = os.environ.get("OUT", "lighter_book_shard_%d.jsonl" % SHARD)
DELAY    = float(os.environ.get("DELAY", "1.1"))
HTTP_TO  = 15


def _f(d, k):
    try:
        return float(d.get(k) or 0)
    except (ValueError, TypeError):
        return 0.0


def get(path, tries=3):
    url = BASE + path
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "copyscan-lighter/1.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TO) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(6 + 5 * i)          # rate limited -> cool off hard
            elif 500 <= e.code < 600:
                time.sleep(2 + 2 * i)
            else:
                return None                     # 4xx bad params -> don't retry
        except Exception:
            time.sleep(2 + 2 * i)
    return None


def distill(aid, a):
    """Snapshot -> compact live-book record. Keeps only NON-flat legs."""
    poss = a.get("positions") or []
    open_pos = []
    for p in poss:
        pv = abs(_f(p, "position_value"))       # notional (flat legs report ~0 / negative dust)
        size = abs(_f(p, "position"))
        if pv < 1.0 or size <= 0:
            continue
        up = _f(p, "unrealized_pnl")
        rp = _f(p, "realized_pnl")
        roe = up / pv if pv > 0 else 0.0         # notional-based ROE (matches lossvet_open.py)
        open_pos.append({
            "coin": str(p.get("symbol", "")).upper(),
            "sign": int(p.get("sign") or 0),     # +1 long / -1 short
            "notional": round(pv, 2),
            "avg_entry": _f(p, "avg_entry_price"),
            "upnl": round(up, 2),
            "rpnl": round(rp, 2),
            "roe": round(roe, 4),
            "liq_px": _f(p, "liquidation_price"),
            "imf": _f(p, "initial_margin_fraction"),
        })
    return {
        "account_id": aid,
        "l1_address": (a.get("l1_address") or "").lower(),
        "account_type": a.get("account_type"),
        "collateral": round(_f(a, "collateral"), 2),
        "acct_value": round(_f(a, "total_asset_value"), 2),
        "avail_balance": round(_f(a, "available_balance"), 2),
        "n_open": len(open_pos),
        "open_upnl": round(sum(pp["upnl"] for pp in open_pos), 2),
        "open_rpnl": round(sum(pp["rpnl"] for pp in open_pos), 2),
        "positions": open_pos,
        "snap_ts": int(time.time()),
    }


def main():
    ids = []
    with open(IDS_FILE) as f:
        for ln in f:
            ln = ln.strip()
            if ln.isdigit():
                ids.append(int(ln))
    mine = ids[SHARD::NSHARD]
    print("shard %d/%d: %d of %d ids, delay=%.2fs" %
          (SHARD, NSHARD, len(mine), len(ids), DELAY), flush=True)
    n_ok = n_fail = n_flat = 0
    with open(OUT, "w") as out:
        for k, aid in enumerate(mine):
            d = get("/account?by=index&value=%d" % aid)
            time.sleep(DELAY)
            if not d or not d.get("accounts"):
                n_fail += 1
                out.write(json.dumps({"account_id": aid, "err": "no_account"}) + "\n")
                continue
            rec = distill(aid, d["accounts"][0])
            if rec["n_open"] == 0:
                n_flat += 1
            n_ok += 1
            out.write(json.dumps(rec, separators=(",", ":")) + "\n")
            out.flush()
            if (k + 1) % 25 == 0:
                print("  %d/%d ok=%d fail=%d flat=%d" %
                      (k + 1, len(mine), n_ok, n_fail, n_flat), flush=True)
    print("DONE shard %d: ok=%d fail=%d flat=%d -> %s" %
          (SHARD, n_ok, n_fail, n_flat, OUT), flush=True)


if __name__ == "__main__":
    main()
