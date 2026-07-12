#!/usr/bin/env python3
"""sol_openbook.py - GH-Actions LIVE OPEN-BOOK reader for positions-only Solana venues (Jupiter Perps
+ Drift). STDLIB ONLY (bare runner, no pip). OFF Roel's home network by construction: runs on a GitHub
cloud runner IP, hits PUBLIC Solana RPC. The earlier holdings scan ran from the LXC (shared home IP,
one budget with the live bot) - this replaces it with a distributed cloud-IP read.

WHY: Solana perps expose NO fills-history API, so the fills-based megadb gauntlet cannot run. The ONE
preventive check available NOW is the LIVE OPEN-BOOK discipline / loss-hider scan: read every holder's
current open positions WITH entry price + mark + (Jup) collateral, and compute how deeply underwater the
book is. A lead sitting on deep un-cut bags = a drainer we block immediately, no forward-shadow wait.

Per holder this worker emits the RAW per-position metrics; the LXC offline driver (sol_openbook_offline.py)
applies the gate thresholds (reuses the megadb live-book framework: bag roe<=-0.10, deep bag<=-0.25,
hider bags>=N) so the vetting logic stays in ONE place.

  jup  reader: getProgramAccounts(JPP, [dataSize 216, memcmp owner@8]) per holder -> that owner's open
       Position PDAs. Decode (validated vs feeds/jup_live.py): side@152, entry@153/1e6, size_usd@161/1e6,
       custody@72->market, collateral@169/1e6. Jupiter margin is ISOLATED per position -> roe = unrl/collat
       is an EXACT leverage-inclusive returnOnEquity (same semantics as HL's returnOnEquity).
  drift reader: getProgramAccounts(DRIFT, [memcmp User-disc@0, memcmp authority@8]) per holder -> that
       authority's subaccounts. Decode (validated vs driftpy.decode_user, exact match): perp region abs
       424, 96B/slot: base_asset_amount@+8 (i64,1e9), quote_entry_amount@+32 (i64,1e6), market_index@+92.
       entry = |qe/1e6| / |base/1e9|. Drift is cross-margined -> per-position isolated ROE is not
       recoverable cheaply, so the leverage-FREE price-move return pmret=dir*(mark-entry)/entry is the
       authoritative Drift bag signal (exact, needs no margin). roe left null (roe_basis=price_move).

  marks: one HL allMids call per shard (cloud IP) -> {COIN: price}. Coins HL doesn't price -> roe/pmret
       null, flagged, NOT counted as a bag (fail-open on unknowns, never false-block).

env: VENUE=jup|drift  SHARD_INDEX  SHARD_TOTAL  ADDR_FILE  OUT  DELAY(0.35)  SOLANA_RPC(optional override)
"""
import os, sys, json, time, base64, hashlib, urllib.request, urllib.error

VENUE       = os.environ.get("VENUE", "jup").strip()
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", "1"))
ADDR_FILE   = os.environ.get("ADDR_FILE", "")
OUT         = os.environ.get("OUT", "sol_openbook_out.jsonl")
DELAY       = float(os.environ.get("DELAY", "0.35"))

# public, no-auth Solana RPC endpoints; rotate per request to spread the per-IP filtered-gPA budget.
RPCS = [os.environ["SOLANA_RPC"]] if os.environ.get("SOLANA_RPC") else [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
]

JPP  = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
JUP_DISC = bytes.fromhex("aabc8fe47a40f7d0")
JUP_POS_SIZE = 216
JUP_CUSTODY = {
    "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz": "SOL",
    "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn": "ETH",
    "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm": "BTC",
}
JUP_SANE = 10**15

DRIFT = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
DRIFT_USER_DISC = hashlib.sha256(b"account:User").digest()[:8]
D_PERP_ABS = 424
D_SLOT = 96
# Drift market_index -> base symbol (denomination-multiplier prefixes already stripped; from driftpy
# mainnet_perp_market_configs @ 2026-07-12). Direction copies 1:1; symbol only used to price the mark.
DRIFT_SYM = {0:"SOL",1:"BTC",2:"ETH",3:"APT",4:"BONK",5:"POL",6:"ARB",7:"DOGE",8:"BNB",9:"SUI",10:"PEPE",
 11:"OP",12:"RENDER",13:"XRP",14:"HNT",15:"INJ",16:"LINK",17:"RLB",18:"PYTH",19:"TIA",20:"JTO",21:"SEI",
 22:"AVAX",23:"WIF",24:"JUP",25:"DYM",26:"TAO",27:"W",28:"KMNO",29:"TNSR",30:"DRIFT",31:"CLOUD",32:"IO",
 33:"ZEX",34:"POPCAT",35:"WEN",36:"TRUMP",37:"KAMALA",38:"FED",39:"REPUBLICAN",40:"BREAKPOINT",
 41:"DEMOCRATS",42:"TON",43:"LANDO",44:"MOTHER",45:"MOODENG",46:"WARWICK",47:"DBR",48:"WLF",49:"VRSTPN",
 50:"LNDO",51:"MEW",52:"MICHI",53:"GOAT",54:"FWOG",55:"PNUT",56:"RAY",57:"SUPERBOWL",58:"SUPERBOWL",
 59:"HYPE",60:"LTC",61:"ME",62:"PENGU",63:"AI16Z",64:"TRUMP",65:"MELANIA",66:"BERA",67:"NBAFINALS25",
 68:"NBAFINALS25",69:"KAITO",70:"IP",71:"FARTCOIN",72:"ADA",73:"PAXG",74:"LAUNCHCOIN",75:"PUMP",
 76:"ASTER",77:"XPL",78:"2Z",79:"ZEC",80:"MNT",81:"PUMP",82:"MET",83:"MON",84:"LIT"}

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
def b58e(b):
    n = int.from_bytes(b, "big"); s = ""
    while n:
        n, r = divmod(n, 58); s = _B58[r] + s
    return "1" * (len(b) - len(b.lstrip(b"\0"))) + s

def _u64(b, o): return int.from_bytes(b[o:o+8], "little")
def _i(b, o, n, s=True): return int.from_bytes(b[o:o+n], "little", signed=s)

_rr = 0
def rpc(method, params, timeout=40, tries=5):
    """rotate endpoint per call; capped backoff on 429/err. Returns result or raises."""
    global _rr
    last = None
    for k in range(tries):
        url = RPCS[(_rr) % len(RPCS)]; _rr += 1
        body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
        try:
            req = urllib.request.Request(url, data=body,
                    headers={"Content-Type":"application/json","User-Agent":"sol-openbook/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                res = json.loads(r.read())
            if "error" in res:
                raise RuntimeError(str(res["error"])[:120])
            return res.get("result")
        except urllib.error.HTTPError as e:
            last = e; time.sleep(min(2.0*(k+1), 8.0) if e.code == 429 else 1.5*(k+1))
        except Exception as e:
            last = e; time.sleep(1.5*(k+1))
    raise RuntimeError("rpc %s failed x%d: %s" % (method, tries, str(last)[:120]))

def hl_allmids():
    try:
        req = urllib.request.Request("https://api.hyperliquid.xyz/info",
                data=json.dumps({"type":"allMids"}).encode(),
                headers={"Content-Type":"application/json","User-Agent":"sol-openbook/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return {k.upper(): float(v) for k, v in json.loads(r.read()).items()}
    except Exception as e:
        print("allMids FAILED: %s" % str(e)[:100], flush=True)
        return {}

# ---------- readers ----------
def read_jup(owner, marks):
    res = rpc("getProgramAccounts", [JPP, {"encoding":"base64",
              "filters":[{"dataSize":JUP_POS_SIZE},{"memcmp":{"offset":8,"bytes":owner}}]}])
    positions, notional, equity = [], 0.0, 0.0
    for acc in (res or []):
        raw = base64.b64decode(acc["account"]["data"][0])
        if len(raw) != JUP_POS_SIZE or raw[0:8] != JUP_DISC: continue
        if b58e(raw[8:40]) != owner: continue
        side_b = raw[152]; size_raw = _u64(raw, 161)
        if side_b not in (1, 2) or not (0 < size_raw < JUP_SANE): continue
        sym = JUP_CUSTODY.get(b58e(raw[72:104]))
        if not sym: continue
        side = "long" if side_b == 1 else "short"
        size_usd = size_raw / 1e6
        entry = _u64(raw, 153) / 1e6
        collat = _u64(raw, 169) / 1e6
        mark = marks.get(sym)
        pmret = roe = unrl = None
        if mark and entry > 0:
            pmret = (mark - entry) / entry * (1 if side == "long" else -1)
            unrl = size_usd * pmret
            roe = (unrl / collat) if collat > 0 else None
        notional += size_usd; equity += collat + (unrl or 0.0)
        positions.append({"coin":sym,"side":side,"entry":round(entry,6),"mark":mark,
                          "notional":round(size_usd,2),"collat":round(collat,2),
                          "pmret":(round(pmret,4) if pmret is not None else None),
                          "roe":(round(roe,4) if roe is not None else None),
                          "unrl":(round(unrl,2) if unrl is not None else None)})
    return positions, round(notional,2), round(equity,2)

def read_drift(auth, marks):
    res = rpc("getProgramAccounts", [DRIFT, {"encoding":"base64",
              "filters":[{"memcmp":{"offset":0,"bytes":b58e(DRIFT_USER_DISC)}},
                         {"memcmp":{"offset":8,"bytes":auth}}]}])
    net = {}   # market_index -> [base_sum, qe_sum]
    for acc in (res or []):
        d = base64.b64decode(acc["account"]["data"][0])
        if len(d) < D_PERP_ABS + D_SLOT: continue
        for pi in range(8):
            o = D_PERP_ABS + pi * D_SLOT
            if o + D_SLOT > len(d): break
            base = _i(d, o+8, 8)
            if base == 0: continue
            qe = _i(d, o+32, 8); mi = _i(d, o+92, 2, s=False)
            e = net.setdefault(mi, [0, 0]); e[0] += base; e[1] += qe
    positions, notional = [], 0.0
    for mi, (base, qe) in net.items():
        if base == 0: continue
        sym = DRIFT_SYM.get(mi, "idx%d" % mi)
        side = "long" if base > 0 else "short"
        entry = (abs(qe) / 1e6) / (abs(base) / 1e9) if base != 0 else 0.0
        mark = marks.get(sym)
        pmret = unrl = ntl = None
        if mark and entry > 0:
            pmret = (mark - entry) / entry * (1 if side == "long" else -1)
            ntl = abs(base) / 1e9 * mark
            unrl = (base / 1e9) * (mark - entry)
            notional += ntl
        positions.append({"coin":sym,"side":side,"entry":round(entry,6),"mark":mark,
                          "notional":(round(ntl,2) if ntl is not None else None),
                          "pmret":(round(pmret,4) if pmret is not None else None),
                          "roe":None,  # cross-margin: leverage-inclusive ROE not recoverable cheaply
                          "unrl":(round(unrl,2) if unrl is not None else None)})
    return positions, round(notional,2), None

def main():
    addrs = [l.strip() for l in open(ADDR_FILE) if l.strip()]
    mine = addrs[SHARD_INDEX::SHARD_TOTAL]
    marks = hl_allmids()
    reader = read_jup if VENUE == "jup" else read_drift
    print("shard %d/%d venue=%s holders=%d marks=%d" %
          (SHARD_INDEX, SHARD_TOTAL, VENUE, len(mine), len(marks)), flush=True)
    n_open = n_flat = n_err = 0
    with open(OUT, "w") as f:
        for i, a in enumerate(mine):
            try:
                positions, notional, equity = reader(a, marks)
            except Exception as e:
                n_err += 1
                f.write(json.dumps({"addr":a,"venue":VENUE,"read_ok":False,
                                    "err":str(e)[:80]}) + "\n")
                time.sleep(DELAY); continue
            if not positions:
                n_flat += 1
            else:
                n_open += 1
                row = {"addr":a,"venue":VENUE,"read_ok":True,"n_pos":len(positions),
                       "notional_usd":notional,"positions":positions}
                if equity is not None: row["equity"] = equity
                f.write(json.dumps(row) + "\n")
            if i % 100 == 0:
                print("  %d/%d open=%d flat=%d err=%d" %
                      (i, len(mine), n_open, n_flat, n_err), flush=True)
            time.sleep(DELAY)
    print("DONE shard %d venue=%s: open=%d flat=%d err=%d" %
          (SHARD_INDEX, VENUE, n_open, n_flat, n_err), flush=True)

if __name__ == "__main__":
    main()
