#!/usr/bin/env python3
"""okx_gh_fetch.py -- OKX copy-lead RAW fetch worker for the GitHub-Actions multi-IP sweep.

OFF-NETWORK BY DESIGN (Roel 2026-07-12): all per-trader OKX fetching happens on cloud
runners, NEVER on Roel's home NAT (shared with the live trading bot). Each GitHub-Actions
runner is a DISTINCT IP with its own OKX rate budget, so the ~259-lead pool is fetched in
parallel shards with no single-IP throttling.

STDLIB ONLY (bare runner, no pip). The runner is a DUMB FETCHER: it pulls the RAW OKX
responses and dumps them verbatim. All transform (raw round-trips -> synthetic HL-schema
fills) + vetting happens OFFLINE on the LXC (okx_offline.py replays the canonical adapter
okx_copy.OKXCopyAdapter over this raw dump), so the fill-construction logic lives in exactly
ONE place and never drifts.

Per lead it fetches BOTH layers of the canonical two-layer gate:
  (1) public-subpositions-history  -> closed round-trips (realized forensic truth), paginated
  (2) public-current-subpositions  -> live open book (loss-hider / open-bag gate)
and records the OKX response CODE/MSG so the LXC can distinguish a genuine data-null
(code 60004 "Trader doesn't exist" == lead hid their sub-positions -> permanent NOFILLS)
from a transient fetch-fail (429 / timeout -> retryable), which a naive empty-return conflates.

Emits one JSON line per lead:
  {"addr","venue":"okx","src":"okx-copy","hist":[...raw...],"pos":[...raw...],
   "n_hist","n_pos","hist_code","hist_msg","pos_code","pos_msg","err"}

env (from the Actions matrix):
  SHARD_INDEX, SHARD_TOTAL  shard k fetches pool[k::SHARD_TOTAL]
  POOL_FILE                 committed uniqueCode list (default okx_leads_pool.txt)
  OUT                       output jsonl (default okx_shard_<k>.jsonl)
  MAX                       cap leads this shard (0 = no cap; canary convenience)
  DELAY                     seconds between requests (default 0.5 -> 2/s < documented 5/2s)
"""
import os, json, time, urllib.request, urllib.error

OKX = "https://www.okx.com/api/v5/copytrading"
HIST_LIMIT = 100
HIST_MAXPAGES = int(os.environ.get("OKX_HIST_MAXPAGES", "25"))   # 25*100 = 2500 trip cap
DELAY = float(os.environ.get("DELAY", "0.5"))
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", "1"))
POOL_FILE = os.environ.get("POOL_FILE", "okx_leads_pool.txt")
OUT = os.environ.get("OUT", "okx_shard_%d.jsonl" % SHARD_INDEX)
MAX = int(os.environ.get("MAX", "0"))

_last = [0.0]
def _throttle():
    dt = time.time() - _last[0]
    if dt < DELAY:
        time.sleep(DELAY - dt)
    _last[0] = time.time()


def _call(path, params, timeout=30, retries=4):
    """Return (data_list, code, msg, http/transport-err-str). code is the OKX business code
    ('0' on success, '60004' on hidden trader, ...). A transport failure -> err set."""
    qs = "&".join("%s=%s" % (k, v) for k, v in params.items())
    url = "%s/%s?%s" % (OKX, path, qs)
    last_err = None
    for attempt in range(retries):
        _throttle()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read().decode()
            d = json.loads(body)
            code = str(d.get("code"))
            msg = d.get("msg") or ""
            if code == "0":
                return (d.get("data", []) or []), code, msg, None
            # business error (e.g. 60004 hidden trader, 50011 rate-limit). 60004 is terminal;
            # rate-limit codes are worth a bounded retry.
            if code in ("50011", "50013", "50004") and attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1)); last_err = "code=%s %s" % (code, msg); continue
            return [], code, msg, None
        except urllib.error.HTTPError as e:
            last_err = "http%d" % e.code
            time.sleep((6 if e.code == 429 else 2) * (attempt + 1))
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as e:
            last_err = repr(e)[:80]
            time.sleep(2 * (attempt + 1))
    return [], None, None, last_err or "fetch-fail"


def fetch_history(uc):
    """All closed sub-positions, paginated by after=<subPosId> (mirrors okx_copy._raw_history)."""
    rows, after = [], None
    code = msg = err = None
    for _ in range(HIST_MAXPAGES):
        params = {"instType": "SWAP", "uniqueCode": uc, "limit": HIST_LIMIT}
        if after:
            params["after"] = after
        page, code, msg, err = _call("public-subpositions-history", params)
        if err or not page:
            break
        rows += page
        if len(page) < HIST_LIMIT:
            break
        after = page[-1].get("subPosId")
        if not after:
            break
    return rows, code, msg, err


def fetch_current(uc):
    data, code, msg, err = _call("public-current-subpositions",
                                 {"instType": "SWAP", "uniqueCode": uc, "limit": 100})
    return data, code, msg, err


def load_pool():
    with open(POOL_FILE) as f:
        codes = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    # stable de-dup preserving order
    seen, out = set(), []
    for c in codes:
        if c not in seen:
            seen.add(c); out.append(c)
    return out


def main():
    pool = load_pool()
    mine = pool[SHARD_INDEX::SHARD_TOTAL]
    if MAX:
        mine = mine[:MAX]
    print("[okx-fetch] shard %d/%d: %d of %d leads (delay=%.2fs) -> %s"
          % (SHARD_INDEX, SHARD_TOTAL, len(mine), len(pool), DELAY, OUT), flush=True)
    n_ok = n_hidden = n_err = 0
    with open(OUT, "w") as fh:
        for i, uc in enumerate(mine, 1):
            try:
                hist, hcode, hmsg, herr = fetch_history(uc)
                pos, pcode, pmsg, perr = fetch_current(uc)
            except Exception as e:
                hist = pos = []
                hcode = pcode = None; hmsg = pmsg = ""; herr = perr = repr(e)[:100]
            rec = {"addr": uc, "venue": "okx", "src": "okx-copy",
                   "hist": hist, "pos": pos, "n_hist": len(hist), "n_pos": len(pos),
                   "hist_code": hcode, "hist_msg": hmsg, "pos_code": pcode, "pos_msg": pmsg,
                   "err": herr or perr}
            fh.write(json.dumps(rec) + "\n"); fh.flush()
            if hist:
                n_ok += 1
            elif hcode == "60004":
                n_hidden += 1
            else:
                n_err += 1
            if i % 20 == 0 or i == len(mine):
                print("[okx-fetch] %d/%d  ok=%d hidden=%d err=%d"
                      % (i, len(mine), n_ok, n_hidden, n_err), flush=True)
    print("[okx-fetch] shard %d DONE: leads=%d hist_ok=%d hidden(60004)=%d fetch_err=%d"
          % (SHARD_INDEX, len(mine), n_ok, n_hidden, n_err), flush=True)


if __name__ == "__main__":
    main()
