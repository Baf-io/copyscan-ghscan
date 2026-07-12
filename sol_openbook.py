#!/usr/bin/env python3
"""sol_openbook.py - GH-Actions LIVE OPEN-BOOK reader for positions-only Solana venues (Jupiter Perps
+ Drift). STDLIB ONLY (bare runner, no pip). OFF Roel's home network by construction: runs on GitHub
cloud runner IPs, hits PUBLIC Solana RPC. The earlier holdings scan ran from the LXC (shared home IP,
one budget with the live bot) - this replaces it with a distributed cloud-IP read.

WHY: Solana perps expose NO fills-history API, so the fills-based megadb gauntlet cannot run. The ONE
preventive check available NOW is the LIVE OPEN-BOOK discipline / loss-hider scan: read every holder's
current open positions WITH entry price + mark + (Jup) collateral, and compute how deeply underwater the
book is. A lead sitting on deep un-cut bags = a drainer we block immediately, no forward-shadow wait.

SCALABLE RPC PATTERN (v2 - the per-holder gPA v1 throttled to death on free RPC): use BULK reads, not
one gPA per holder.
  jup  : 3 per-custody full-program getProgramAccounts (SOL/ETH/BTC), dataSlice 8..169 -> owner + side +
         entry@153 + size_usd@161 + collateral@169 for EVERY open Position PDA in one call/custody; keep
         owners in the committed holder set. Jup margin is ISOLATED -> roe = unrl/collat is an EXACT
         leverage-inclusive returnOnEquity (validated vs feeds/jup_live.py). VENUE=jup is a SINGLE job.
  drift: getMultipleAccounts (CHEAP, not gPA-throttled) on PRECOMPUTED user-account PDAs
         (ghscan/sol_drift_pdas.jsonl, derived offline via driftpy - no runner-side crypto), dataSlice
         424..1192 = the perp_positions region only. Decode base@+8 (i64,1e9) + quote_entry@+32 (i64,1e6)
         + market_index@+92 (validated exact vs driftpy.decode_user), net per authority across subaccounts.
         Drift is cross-margined -> isolated ROE not cheap; the leverage-FREE price-move return
         pmret=dir*(mark-entry)/entry is the authoritative Drift bag signal (exact). roe left null.
         VENUE=drift is SHARDED over authorities.

  marks: one HL allMids call per job (cloud IP) -> {COIN: price}. Coins HL doesn't price -> roe/pmret
         null, flagged, NOT counted as a bag (fail-open on unknowns, never false-block).

env: VENUE=jup|drift  SHARD_INDEX  SHARD_TOTAL  ADDR_FILE(jup)  PDA_FILE(drift)  OUT  DELAY(0.15)
     SOLANA_RPC(optional override)
"""
import os, sys, json, time, base64, urllib.request, urllib.error

VENUE       = os.environ.get("VENUE", "jup").strip()
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", "0"))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", "1"))
ADDR_FILE   = os.environ.get("ADDR_FILE", "sol_jup_addrs.txt")
PDA_FILE    = os.environ.get("PDA_FILE", "sol_drift_pdas.jsonl")
OUT         = os.environ.get("OUT", "sol_openbook_out.jsonl")
DELAY       = float(os.environ.get("DELAY", "0.15"))

RPCS = [os.environ["SOLANA_RPC"]] if os.environ.get("SOLANA_RPC") else [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://solana.drpc.org",
    "https://endpoints.omniatech.io/v1/sol/mainnet/public",
]

JPP  = "PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu"
JUP_DISC_B58 = "VZMoMoKgZQb"            # b58 of the Position discriminator aabc8fe47a40f7d0 (validated)
JUP_POS_SIZE = 216
JUP_CUSTODY = {  # custody pubkey -> (market, custody is memcmp'd at abs offset 72)
    "SOL": "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz",
    "ETH": "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn",
    "BTC": "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm",
}
JUP_CUST_REV = {v: k for k, v in JUP_CUSTODY.items()}
JUP_SANE = 10**15

D_PERP_ABS = 424
D_SLOT = 96
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
def rpc(method, params, timeout=120, tries=6):
    global _rr
    last = None
    for k in range(tries):
        url = RPCS[_rr % len(RPCS)]; _rr += 1
        body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
        try:
            req = urllib.request.Request(url, data=body,
                    headers={"Content-Type":"application/json","User-Agent":"sol-openbook/2.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                res = json.loads(r.read())
            if "error" in res:
                raise RuntimeError(str(res["error"])[:140])
            return res.get("result")
        except urllib.error.HTTPError as e:
            last = e; time.sleep(min(2.0*(k+1), 10.0) if e.code == 429 else 1.5*(k+1))
        except Exception as e:
            last = e; time.sleep(1.5*(k+1))
    raise RuntimeError("rpc %s failed x%d: %s" % (method, tries, str(last)[:140]))

def hl_allmids():
    try:
        req = urllib.request.Request("https://api.hyperliquid.xyz/info",
                data=json.dumps({"type":"allMids"}).encode(),
                headers={"Content-Type":"application/json","User-Agent":"sol-openbook/2.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return {k.upper(): float(v) for k, v in json.loads(r.read()).items()}
    except Exception as e:
        print("allMids FAILED: %s" % str(e)[:100], flush=True); return {}

# ---------- JUPITER: 3 per-custody full-program gPA (bulk) ----------
def scan_jup(holder_set, marks):
    agg = {}   # owner -> {positions, notional, equity}
    for sym, cust in JUP_CUSTODY.items():
        res = rpc("getProgramAccounts", [JPP, {"encoding":"base64",
                  "filters":[{"dataSize":JUP_POS_SIZE},
                             {"memcmp":{"offset":0,"bytes":JUP_DISC_B58}},
                             {"memcmp":{"offset":72,"bytes":cust}}],
                  "dataSlice":{"offset":8,"length":169}}], timeout=300)
        kept = 0
        for acc in (res or []):
            d = base64.b64decode(acc["account"]["data"][0])
            if len(d) < 169: continue
            owner = b58e(d[0:32])
            if owner not in holder_set: continue
            side_b = d[144]; size_raw = _u64(d, 153)
            if side_b not in (1, 2) or not (0 < size_raw < JUP_SANE): continue
            side = "long" if side_b == 1 else "short"
            size_usd = size_raw / 1e6; entry = _u64(d, 145) / 1e6; collat = _u64(d, 161) / 1e6
            mark = marks.get(sym); pmret = roe = unrl = None
            if mark and entry > 0:
                pmret = (mark - entry) / entry * (1 if side == "long" else -1)
                unrl = size_usd * pmret
                roe = (unrl / collat) if collat > 0 else None
            e = agg.setdefault(owner, {"positions":[], "notional":0.0, "equity":0.0})
            e["notional"] += size_usd; e["equity"] += collat + (unrl or 0.0)
            e["positions"].append({"coin":sym,"side":side,"entry":round(entry,6),"mark":mark,
                "notional":round(size_usd,2),"collat":round(collat,2),
                "pmret":(round(pmret,4) if pmret is not None else None),
                "roe":(round(roe,4) if roe is not None else None),
                "unrl":(round(unrl,2) if unrl is not None else None)})
            kept += 1
        print("  custody %s: kept %d holder-positions" % (sym, kept), flush=True)
        time.sleep(DELAY)
    with open(OUT, "w", buffering=1) as f:
        for owner, e in agg.items():
            f.write(json.dumps({"addr":owner,"venue":"jup","read_ok":True,"n_pos":len(e["positions"]),
                "notional_usd":round(e["notional"],2),"equity":round(e["equity"],2),
                "positions":e["positions"]}) + "\n")
    print("DONE jup: %d holders w/ open positions" % len(agg), flush=True)

# ---------- DRIFT: getMultipleAccounts on precomputed PDAs (bulk, sharded) ----------
def scan_drift(marks):
    rows = [json.loads(l) for l in open(PDA_FILE) if l.strip()]
    mine = rows[SHARD_INDEX::SHARD_TOTAL]
    # flat list of (auth, pda); getMultipleAccounts in batches of 100; attribute by index
    flat = [(r["auth"], p) for r in mine for p in r["pdas"]]
    net = {}   # auth -> {market_index: [base_sum, qe_sum]}
    n_open = n_err = 0
    for ci in range(0, len(flat), 100):
        chunk = flat[ci:ci+100]
        keys = [p for _, p in chunk]
        try:
            res = rpc("getMultipleAccounts", [keys, {"encoding":"base64",
                      "dataSlice":{"offset":D_PERP_ABS,"length":8*D_SLOT}}], timeout=60)
        except Exception as e:
            n_err += 1; print("  batch %d err: %s" % (ci//100, str(e)[:80]), flush=True); continue
        for (auth, _pda), acc in zip(chunk, (res or {}).get("value", []) or []):
            if not acc: continue
            d = base64.b64decode(acc["data"][0])
            if len(d) < 8*D_SLOT: continue
            for pi in range(8):
                o = pi * D_SLOT
                base = _i(d, o+8, 8)
                if base == 0: continue
                qe = _i(d, o+32, 8); mi = _i(d, o+92, 2, s=False)
                e = net.setdefault(auth, {}).setdefault(mi, [0, 0]); e[0] += base; e[1] += qe
        if (ci//100) % 20 == 0:
            print("  batch %d/%d auths=%d" % (ci//100, (len(flat)+99)//100, len(net)), flush=True)
        time.sleep(DELAY)
    with open(OUT, "w", buffering=1) as f:
        for auth, mkts in net.items():
            positions, notional = [], 0.0
            for mi, (base, qe) in mkts.items():
                if base == 0: continue
                sym = DRIFT_SYM.get(mi, "idx%d" % mi)
                side = "long" if base > 0 else "short"
                entry = (abs(qe)/1e6) / (abs(base)/1e9) if base != 0 else 0.0
                mark = marks.get(sym); pmret = unrl = ntl = None
                if mark and entry > 0:
                    pmret = (mark - entry) / entry * (1 if side == "long" else -1)
                    ntl = abs(base)/1e9 * mark; unrl = (base/1e9) * (mark - entry); notional += ntl
                positions.append({"coin":sym,"side":side,"entry":round(entry,6),"mark":mark,
                    "notional":(round(ntl,2) if ntl is not None else None),
                    "pmret":(round(pmret,4) if pmret is not None else None), "roe":None,
                    "unrl":(round(unrl,2) if unrl is not None else None)})
            if not positions: continue
            n_open += 1
            f.write(json.dumps({"addr":auth,"venue":"drift","read_ok":True,"n_pos":len(positions),
                "notional_usd":round(notional,2),"positions":positions}) + "\n")
    print("DONE drift shard %d/%d: auths_read=%d open=%d batch_err=%d" %
          (SHARD_INDEX, SHARD_TOTAL, len(mine), n_open, n_err), flush=True)

def main():
    marks = hl_allmids()
    print("venue=%s shard=%d/%d marks=%d" % (VENUE, SHARD_INDEX, SHARD_TOTAL, len(marks)), flush=True)
    if VENUE == "jup":
        holder_set = set(l.strip() for l in open(ADDR_FILE) if l.strip())
        print("jup holder set: %d" % len(holder_set), flush=True)
        scan_jup(holder_set, marks)
    else:
        scan_drift(marks)

if __name__ == "__main__":
    main()
