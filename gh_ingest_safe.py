import sys, os, json, glob, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import scan as S
S._load_adapters(); assets = S.load_assets(); now_ms = int(time.time()*1000); now_s = int(time.time())
ad = S.ADAPTERS["hl"]()
files = sorted(f for p in (sys.argv[1:] or ["artifacts"])
               for f in (glob.glob(os.path.join(p, "**", "*.jsonl"), recursive=True) if os.path.isdir(p) else [p]))
out = os.path.join(os.path.dirname(HERE), "probes", "scan_hl.jsonl")
# SWEPT-LEDGER: record EVERY vetted addr (any verdict) so build_sweep_targets can skip it next run
# (RECHECK_DAYS). This is what turns the 3h full re-sweep into a ~15min incremental slice.
ledger = os.path.join(os.path.dirname(HERE), "out", "swept_ledger.jsonl")
os.makedirs(os.path.dirname(ledger), exist_ok=True)
lf = open(ledger, "a")
print("START: %d shard files -> %s (+ledger %s)" % (len(files), out, os.path.basename(ledger)), flush=True)
_cur = {"fills": [], "pos": {}}
S.hl_capped_fills = lambda a: _cur["fills"]
ad.get_positions = lambda a: _cur["pos"]
S.hl_spot_balances = lambda a: {}
ad.count_liquidations = lambda a: None
kept = n = 0; counts = {}
f = open(out, "w")
for fp in files:
    for line in open(fp):
        line = line.strip()
        if not line: continue
        try: r = json.loads(line)
        except Exception: continue
        n += 1
        _cur["fills"] = r.get("fills") or []
        _cur["pos"]   = r.get("positions") or {}
        try: cand, v = S.scan_addr(ad, r["addr"], assets, now_ms)
        except Exception: cand, v = None, "ERR"
        counts[v] = counts.get(v, 0) + 1
        a = (r.get("addr") or "").lower()
        if a: lf.write(json.dumps({"addr": a, "ts": now_s, "verdict": v}) + chr(10))
        if cand:
            f.write(json.dumps(cand) + chr(10)); f.flush()
            if v in ("CLEAN", "WATCH"): kept += 1
    print("  shard %s done; total %d, survivors %d" % (os.path.basename(fp), n, kept), flush=True)
f.close(); lf.close()
print("INGEST DONE: %d records -> %s | survivors(CLEAN/WATCH)=%d | ledger+=%d" % (n, counts, kept, n), flush=True)
