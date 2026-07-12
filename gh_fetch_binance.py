#!/usr/bin/env python3
"""gh_fetch_binance.py — Binance copy-trade LEAD fetch worker for the GitHub-Actions sweep.

STDLIB ONLY (bare runner, no pip). Runs OFF Roel's home network — each GH runner is a DISTINCT
cloud IP with its own Binance budget, so this is where ALL per-lead Binance fetching happens
(the LXC shares one home IP with a live trading bot; on-network mass fetch is banned).

Self-contained clone of /root/copyscan/venues/binance_copy.py's request + normalization logic so
the runner needs neither the copyscan repo nor var_universe.json. It only FETCHES + normalizes to
the canonical HL fill schema; the fee-true vetting (megadb_vet gauntlet) runs OFFLINE on the LXC
over the artifacts this emits (platform stats = DISCOVERY ranking only, per doctrine).

Two modes (env MODE):
  discover : sweep query-list (30D+90D x PNL+ROI), dedup by leadPortfolioId, filter aum>=AUM_FLOOR
             & roi>0 -> write leads_pool.jsonl (one row/lead + platform stats). One runner. The
             query-list is discovery metadata (not per-lead history); it is small (~a few hundred
             paginated calls). Prints a clear GEO/BLOCK diagnostic from the first response so a
             US-datacenter geoblock of www.binance.com is visible in the job log.
  fetch    : read leads_pool.jsonl, take slice [SHARD_INDEX::SHARD_TOTAL], and for each lead pull
             DETAIL (closeLeadCount serial-cycler flag, positionShow, profitSharingRate, ...) +
             the raw FILL tape (trade-history) -> write shard_<k>.jsonl records:
               {addr, venue:"binance", src:"binance-copy", fills:[<canonical>], windowed,
                fill_total, span_est_d, disc:{...platform+detail...}}
             Detail is fetched here (not in discover) so the per-lead calls distribute across the
             matrix. Every lead gets a record even with zero fills (offline vet -> NOFILLS), so
             the whole pool receives a verdict.

env:
  MODE                 discover | fetch                              (default fetch)
  SHARD_INDEX,SHARD_TOTAL  fetch: this runner handles pool[k::total] (from the Actions matrix)
  AUM_FLOOR            discover filter, kill tiny-capital ROI% gaming (default 5000)
  DISC_MAX_PAGES       discover: page cap per (timeRange x dataType) combo (default 100; probe=3)
  MAX_LEADS            fetch: cap leads this shard processes, 0=all (default 0; probe convenience)
  MAXREC               fetch: per-lead fill cap (default 3000; bounds scalper cost, span is the
                       real FULLY limiter anyway)
  DELAY                fetch: extra seconds between LEADS (throttle already spaces requests ~1.05s)
  POOL_FILE            leads_pool.jsonl path (default leads_pool.jsonl)
  OUT                  output path (discover default leads_pool.jsonl; fetch default shard_<k>.jsonl)
"""
import json, os, time, urllib.request, urllib.error

BASE = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade"
HDRS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Content-Type": "application/json", "Accept": "application/json",
        "Origin": "https://www.binance.com", "Referer": "https://www.binance.com/en/copy-trading"}

PAGE_CAP = 200
WINDOW_CAP = 6000
_MIN_INTERVAL = 1.05
_QUOTES = ("USDT", "USDC", "FDUSD", "BUSD", "USD")
_ALIAS = {"XBT": "BTC", "WETH": "ETH", "WBTC": "BTC"}

_last_req = [0.0]


def _throttle():
    dt = time.time() - _last_req[0]
    if dt < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - dt)
    _last_req[0] = time.time()


def _req(path, body=None, retries=6, timeout=30):
    url = BASE + path
    last = None
    for i in range(retries):
        _throttle()
        try:
            if body is not None:
                req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                             headers=HDRS, method="POST")
            else:
                req = urllib.request.Request(url, headers=HDRS)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 418):
                ra = e.headers.get("Retry-After") if e.headers else None
                try:
                    wait = float(ra) if ra else min(2.0 * (2 ** i), 30.0)
                except (ValueError, TypeError):
                    wait = min(2.0 * (2 ** i), 30.0)
                time.sleep(wait)
                continue
            # 451 = geo-restricted (US datacenter block); 403 = challenge. Surface, do not retry.
            if e.code in (400, 404, 451, 403):
                return {"code": str(e.code), "success": False, "data": None, "_http": e.code}
            time.sleep(1.0 * (i + 1))
        except Exception as e:
            last = e
            time.sleep(1.0 * (i + 1))
    if last:
        return {"code": "ERR", "success": False, "data": None, "_err": repr(last)[:160]}
    return {"data": None}


def _base_ticker(symbol, base_asset=None):
    """Base ticker, KEEPING the numeric multiplier prefix (1000PEPEUSDT -> 1000PEPE). No VARU
    filtering on the runner — the LXC offline vet maps to the Variational-executable set."""
    s = (symbol or "").upper()
    for q in _QUOTES:
        if s.endswith(q) and len(s) > len(q):
            s = s[:-len(q)]
            break
    s = _ALIAS.get(s, s)
    if s:
        return s
    if base_asset:
        return _ALIAS.get(base_asset.upper(), base_asset.upper())
    return s


# ---- discovery -------------------------------------------------------------
def get_leads(time_ranges, data_types, page_size=100, max_pages=100, order="DESC"):
    out = {}
    first_diag = None
    for tr in time_ranges:
        for dt in data_types:
            for pn in range(1, max_pages + 1):
                d = _req("/home-page/query-list",
                         {"pageNumber": pn, "pageSize": page_size,
                          "timeRange": tr, "dataType": dt, "order": order})
                if first_diag is None:
                    first_diag = {"code": d.get("code"), "success": d.get("success"),
                                  "http": d.get("_http"), "err": d.get("_err"),
                                  "has_data": bool(d.get("data"))}
                data = d.get("data") or {}
                lst = data.get("list") if isinstance(data, dict) else data
                lst = lst or []
                if not lst:
                    break
                for r in lst:
                    pid = str(r.get("leadPortfolioId") or "")
                    if not pid:
                        continue
                    r = dict(r)
                    r["_seen"] = r.get("_seen", []) + ["%s/%s" % (tr, dt)]
                    if pid in out:
                        out[pid]["_seen"] = out[pid].get("_seen", []) + r["_seen"]
                    else:
                        out[pid] = r
                total = data.get("total") if isinstance(data, dict) else None
                if total and pn * page_size >= total:
                    break
    return out, first_diag


def get_detail(pid):
    d = _req("/lead-portfolio/detail?portfolioId=%s" % pid)
    return d.get("data") or {}


# ---- fills -----------------------------------------------------------------
def _fill_key(r):
    return (r.get("time"), r.get("symbol"), r.get("side"), r.get("price"),
            r.get("qty"), r.get("realizedProfit"), r.get("fee"), r.get("positionSide"))


def _trade_pages(pid, page_size=200, max_records=WINDOW_CAP, max_pages=60):
    page_size = min(page_size, PAGE_CAP)
    seen, rows, total = set(), [], None
    stalls = 0
    for pn in range(1, max_pages + 1):
        page = []
        for att in range(4):
            d = _req("/lead-portfolio/trade-history",
                     {"portfolioId": pid, "pageNumber": pn, "pageSize": page_size})
            data = d.get("data") or {}
            page = (data.get("list") if isinstance(data, dict) else data) or []
            if total is None and isinstance(data, dict) and data.get("total") is not None:
                total = data.get("total")
            if page:
                break
            time.sleep(2.0)
        new = 0
        for r in page:
            k = _fill_key(r)
            if k not in seen:
                seen.add(k); rows.append(r); new += 1
        if new == 0:
            stalls += 1
            if stalls >= 2:
                break
        else:
            stalls = 0
        if len(rows) >= max_records or (total and len(rows) >= total):
            break
    return rows, total


def get_fills(pid, max_records=WINDOW_CAP):
    raw, total = _trade_pages(pid, max_records=max_records)
    last_total = total or len(raw)
    windowed = bool(total and total >= WINDOW_CAP)
    out = []
    for r in raw:
        try:
            t = int(r.get("time") or 0)
            px = float(r.get("price") or 0)
            sz = float(r.get("qty") or 0)
            cp = float(r.get("realizedProfit") or 0)
            fraw = r.get("fee")
            fee = -float(fraw) if fraw not in (None, "") else 0.0
        except (TypeError, ValueError):
            continue
        if t <= 0 or px <= 0 or sz <= 0:
            continue
        side = "B" if str(r.get("side", "")).upper() == "BUY" else "A"
        coin = _base_ticker(r.get("symbol"), r.get("baseAsset"))
        out.append({"coin": coin, "side": side, "sz": sz, "px": px, "time": t,
                    "closedPnl": cp, "fee": fee, "crossed": bool(r.get("activeBuy"))})
    out.sort(key=lambda f: (f["time"], 1 if f["closedPnl"] != 0 else 0))
    span = round((out[-1]["time"] - out[0]["time"]) / 86400000.0, 1) if out else None
    return out, windowed, last_total, span


# ---- modes -----------------------------------------------------------------
def mode_discover():
    aum_floor = float(os.environ.get("AUM_FLOOR", "5000"))
    max_pages = int(os.environ.get("DISC_MAX_PAGES", "100"))
    out_path = os.environ.get("OUT", "leads_pool.jsonl")
    print("[discover] sweeping query-list (30D+90D x PNL+ROI, max_pages=%d)..." % max_pages, flush=True)
    leads, diag = get_leads(("30D", "90D"), ("PNL", "ROI"), max_pages=max_pages)
    print("[discover] first-response diag:", json.dumps(diag), flush=True)
    print("[discover] raw unique portfolios:", len(leads), flush=True)
    if not leads:
        print("[discover] !!! ZERO leads — likely geo-block (451) or challenge (403) from this "
              "runner IP, OR endpoint moved. See diag above. Writing empty pool.", flush=True)
    kept = []
    for pid, r in leads.items():
        try:
            aum = float(r.get("aum") or 0); roi = float(r.get("roi") or 0)
        except (TypeError, ValueError):
            continue
        if aum < aum_floor or roi <= 0:
            continue
        kept.append((pid, aum, r))
    kept.sort(key=lambda x: x[1], reverse=True)
    print("[discover] after aum>=$%.0f & roi>0: %d" % (aum_floor, len(kept)), flush=True)
    with open(out_path, "w") as f:
        for pid, aum, r in kept:
            row = {"addr": str(pid), "venue": "binance", "aum": aum,
                   "roi": r.get("roi"), "pnl": r.get("pnl"), "mdd": r.get("mdd"),
                   "winRate": r.get("winRate"), "sharpe": r.get("sharpRatio"),
                   "copierPnl": r.get("copierPnl"),
                   "currentCopyCount": r.get("currentCopyCount"),
                   "tradFiTag": r.get("tradFiTag"), "portfolioType": r.get("portfolioType"),
                   "nickname": r.get("nickname"), "startTime": r.get("startTime"),
                   "seen": sorted(set(r.get("_seen") or []))}
            f.write(json.dumps(row) + "\n")
    print("[discover] wrote %d leads -> %s" % (len(kept), out_path), flush=True)


def mode_fetch():
    shard = int(os.environ.get("SHARD_INDEX", "0"))
    total = int(os.environ.get("SHARD_TOTAL", "1"))
    pool_file = os.environ.get("POOL_FILE", "leads_pool.jsonl")
    out_path = os.environ.get("OUT", "shard_%d.jsonl" % shard)
    maxrec = int(os.environ.get("MAXREC", "3000"))
    max_leads = int(os.environ.get("MAX_LEADS", "0"))
    extra_delay = float(os.environ.get("DELAY", "0"))
    SERIAL_CYCLER_MIN = 3
    try:
        pool = [json.loads(l) for l in open(pool_file) if l.strip()]
    except OSError:
        print("[fetch] no pool file %s — run discover first" % pool_file, flush=True)
        open(out_path, "w").close(); return
    pool.sort(key=lambda d: float(d.get("aum") or 0), reverse=True)
    mine = pool[shard::total]
    if max_leads:
        mine = mine[:max_leads]
    n = len(mine)
    print("[fetch shard %d/%d] pool=%d mine=%d maxrec=%d" % (shard, total, len(pool), n, maxrec), flush=True)
    with open(out_path, "w") as f:
        for i, ld in enumerate(mine, 1):
            pid = str(ld.get("addr"))
            det = {}
            try:
                det = get_detail(pid)
            except Exception as e:
                det = {"_detail_err": repr(e)[:80]}
            clc = det.get("closeLeadCount")
            try:
                clc = int(clc) if clc is not None else None
            except (TypeError, ValueError):
                clc = None
            serial = bool(clc is not None and clc >= SERIAL_CYCLER_MIN)
            try:
                fills, windowed, ftotal, span = get_fills(pid, max_records=maxrec)
            except Exception as e:
                fills, windowed, ftotal, span = [], False, 0, None
                print("  [shard %d] %s FILLS-ERR %s" % (shard, pid, repr(e)[:80]), flush=True)
            rec = {"addr": pid, "venue": "binance", "src": "binance-copy",
                   "fills": fills, "windowed": windowed, "fill_total": ftotal,
                   "span_est_d": span,
                   "disc": {"aum": ld.get("aum"), "roi": ld.get("roi"),
                            "sharpe": ld.get("sharpe"), "winRate": ld.get("winRate"),
                            "pnl": ld.get("pnl"), "mdd": ld.get("mdd"),
                            "copierPnl": ld.get("copierPnl"),
                            "tradFiTag": ld.get("tradFiTag"), "nickname": ld.get("nickname"),
                            "serial_cycler": serial, "close_lead_count": clc,
                            "profit_sharing_rate": det.get("profitSharingRate"),
                            "position_show": det.get("positionShow"),
                            "margin_balance": det.get("marginBalance"),
                            "total_copy_count": det.get("totalCopyCount"),
                            "status": det.get("status"),
                            "last_trade_time": det.get("lastTradeTime"),
                            "start_time": det.get("startTime")}}
            f.write(json.dumps(rec) + "\n"); f.flush()
            if extra_delay:
                time.sleep(extra_delay)
            if i % 10 == 0 or i == n:
                print("  [shard %d] %d/%d (last pid=%s fills=%d windowed=%s span=%s)"
                      % (shard, i, n, pid, len(fills), windowed, span), flush=True)
    print("[fetch shard %d] wrote %s (%d leads)" % (shard, out_path, n), flush=True)


if __name__ == "__main__":
    mode = os.environ.get("MODE", "fetch").strip().lower()
    if mode == "discover":
        mode_discover()
    else:
        mode_fetch()
