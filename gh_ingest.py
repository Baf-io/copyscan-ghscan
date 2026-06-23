#!/usr/bin/env python3
"""gh_ingest.py — LXC side of the GH multi-IP sweep.

Reads the raw {addr, fills, positions} shard files the runners produced and vets each through
scan.scan_addr UNCHANGED — by patching the two fetch hooks (hl_capped_fills + adapter.get_positions)
to return the pre-fetched data instead of hitting HL. So the GH sweep and a local `scan.py hl` run
vet with byte-identical logic (no duplicated forensic/ruleset code, no drift). Writes CLEAN/WATCH to
probes/scan_hl.jsonl and records the vetted cache; then run consolidate.py to fold into the roster.

  python3 gh_ingest.py <dir|file ...>     e.g.  python3 gh_ingest.py artifacts/
"""
import sys, os, json, glob, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))     # copyscan/ (for scan.py)
import scan as S


def main():
    args = sys.argv[1:] or ["."]
    files = []
    for p in args:
        files += glob.glob(os.path.join(p, "**", "*.jsonl"), recursive=True) if os.path.isdir(p) else [p]
    recs = []
    for fp in files:
        for line in open(fp):
            if line.strip():
                recs.append(json.loads(line))
    print(f"ingesting {len(recs)} fetched records from {len(files)} shard file(s)")
    if not recs:
        print("nothing to ingest."); return

    S._load_adapters()
    ad = S.ADAPTERS["hl"]()
    FILLS = {r["addr"].lower(): (r.get("fills") or []) for r in recs}
    POS   = {r["addr"].lower(): (r.get("positions") or {}) for r in recs}
    # patch the fetch hooks -> serve pre-fetched data; scan_addr runs otherwise unchanged
    S.hl_capped_fills = lambda a: FILLS.get(a.lower(), [])
    ad.get_positions = lambda a: POS.get(a.lower(), {})

    assets = S.load_assets()
    now_ms = int(time.time() * 1000)
    out = os.path.join(os.path.dirname(HERE), "probes", "scan_hl.jsonl")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    counts, kept = {}, 0
    with open(out, "w") as f:
        for r in recs:
            try:
                cand, v = S.scan_addr(ad, r["addr"], assets, now_ms)
            except Exception as e:
                cand, v = None, f"ERROR:{type(e).__name__}"
            counts[v] = counts.get(v, 0) + 1
            if cand:
                f.write(json.dumps(cand) + "\n")
                if v in ("CLEAN", "WATCH"):
                    kept += 1
                    print(f"  ** {v:<5} {cand['addr'][:12]} hold={cand['median_hold_h']}h "
                          f"wr={cand['wr']} pf={cand['payoff']} mkts={cand['variational'][:5]}")
    S.mark_vetted([r["addr"] for r in recs])
    print(f"\ningested {len(recs)} -> verdicts={counts} survivors(CLEAN/WATCH)={kept}")
    print(f"wrote {out}\nnext: python3 consolidate.py   (folds survivors into out/roster.jsonl)")


if __name__ == "__main__":
    main()
