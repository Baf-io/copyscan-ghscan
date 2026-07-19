#!/usr/bin/env python3
"""make_scoreboard.py — render the public shadow-board -> docs/index.html for GitHub Pages.

Reads the committed PUBLIC outputs only (no engine IP): results/scoreboard.json (vet + open-book
verdicts) overlaid with shadow/summary.jsonl (forward paper-track: days / trips / cum our_E / promote).
Self-contained dark-terminal HTML, no external assets. Runs in the PUBLIC repo; needs no deploy key.

Usage: make_scoreboard.py [--root .] [--out docs/index.html]
"""
import json, os, sys, time, argparse, html

ORDER = {"BOARD": 0, "WATCH": 1, "BOARDED": 1, "REJECT": 3}


def load_jsonl(p):
    return [json.loads(l) for l in open(p)] if os.path.exists(p) else []


def load_json(p):
    return json.load(open(p)) if os.path.exists(p) else {}


def fnum(v, pfx="", suf=""):
    if v is None:
        return "—"
    try:
        return "%s%s%s" % (pfx, ("{:,}".format(int(round(float(v))))), suf)
    except Exception:
        return html.escape(str(v))


def render(root):
    sb = load_json(os.path.join(root, "results", "scoreboard.json"))
    summ = {s["addr"]: s for s in load_jsonl(os.path.join(root, "shadow", "summary.jsonl"))}
    rows = sb.get("rows", [])
    ts = sb.get("ts") or int(time.time())

    merged = []
    for r in rows:
        a = r.get("addr")
        s = summ.get(a, {})
        merged.append({**r, "fwd_days": s.get("days_tracked"), "fwd_trips": s.get("n_fwd_trips"),
                       "cum_our_E": s.get("cum_our_E"), "promote": bool(s.get("promote"))})
    # promotions first, then BOARD, WATCH, REJECT; within, by forward cum then vet score
    merged.sort(key=lambda x: (0 if x["promote"] else 1, ORDER.get(x.get("verdict"), 4),
                               -(x.get("cum_our_E") or -9e9), -(x.get("score") or 0)))

    from collections import Counter
    vc = Counter(r.get("verdict") for r in rows)
    n_prom = sum(1 for m in merged if m["promote"])
    updated = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ts))

    def pill(v, promote):
        cls = "promote" if promote else {"BOARD": "board", "WATCH": "watch",
                                         "BOARDED": "watch"}.get(v, "reject")
        lab = "PROMOTE" if promote else v
        return '<span class="pill %s">%s</span>' % (cls, html.escape(str(lab)))

    trs = []
    for i, m in enumerate(merged, 1):
        a = m.get("addr", "")
        short = html.escape(a[:6] + "…" + a[-4:]) if a else "—"
        coins = ", ".join(html.escape(str(c)) for c in (m.get("copyable_coins") or [])) or "—"
        oe = m.get("cum_our_E")
        oe_cls = "pos" if (oe or 0) > 0 else ("neg" if (oe or 0) < 0 else "")
        why = html.escape(str(m.get("why") or ""))
        trs.append(
            "<tr>"
            f'<td class="dim">{i}</td>'
            f"<td>{pill(m.get('verdict'), m['promote'])}</td>"
            f'<td class="mono"><a href="https://hyperdash.info/trader/{html.escape(a)}" target="_blank" rel="noopener">{short}</a></td>'
            f'<td class="coins">{coins}</td>'
            f'<td class="num">{fnum(m.get("av"), "$")}</td>'
            f'<td class="num">{fnum(m.get("r30"), "$")}</td>'
            f'<td class="num">{("%.1f" % m["fwd_days"]) if m.get("fwd_days") is not None else "—"}</td>'
            f'<td class="num">{m.get("fwd_trips") if m.get("fwd_trips") is not None else "—"}</td>'
            f'<td class="num {oe_cls}">{("%+.2f%%" % oe) if oe is not None else "—"}</td>'
            f'<td class="why">{why}</td>'
            "</tr>")

    body = "\n".join(trs) or '<tr><td colspan="10" class="dim" style="text-align:center;padding:32px">no candidates yet — awaiting first full vet</td></tr>'

    return TEMPLATE.format(
        updated=updated, n=len(rows), board=vc.get("BOARD", 0), watch=vc.get("WATCH", 0) + vc.get("BOARDED", 0),
        reject=vc.get("REJECT", 0), prom=n_prom, rows=body)


TEMPLATE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>copyscan · shadow board</title>
<style>
 :root {{ --bg:#0b0e14; --panel:#111722; --line:#1e2735; --txt:#c9d4e5; --dim:#5c6b82; --acc:#6ea8fe;
         --pos:#3fb950; --neg:#f85149; --board:#2f81f7; --watch:#9e6a03; --prom:#238636; --rej:#30363d; }}
 * {{ box-sizing:border-box; }}
 body {{ margin:0; background:var(--bg); color:var(--txt); font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }}
 .wrap {{ max-width:1100px; margin:0 auto; padding:28px 18px 64px; }}
 h1 {{ font-size:20px; margin:0 0 2px; letter-spacing:.5px; }}
 .sub {{ color:var(--dim); font-size:12.5px; margin-bottom:18px; }}
 .sub b {{ color:var(--acc); font-weight:600; }}
 .stats {{ display:flex; gap:10px; flex-wrap:wrap; margin:0 0 18px; }}
 .stat {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:9px 14px; }}
 .stat .k {{ color:var(--dim); font-size:11px; text-transform:uppercase; letter-spacing:.6px; }}
 .stat .v {{ font-size:19px; font-weight:600; }}
 .tblwrap {{ overflow-x:auto; border:1px solid var(--line); border-radius:10px; background:var(--panel); }}
 table {{ border-collapse:collapse; width:100%; min-width:760px; }}
 th,td {{ padding:9px 12px; text-align:left; border-bottom:1px solid var(--line); white-space:nowrap; }}
 th {{ color:var(--dim); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:.5px; position:sticky; top:0; background:var(--panel); }}
 tr:last-child td {{ border-bottom:0; }}
 tr:hover td {{ background:#0d131d; }}
 td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
 td.dim {{ color:var(--dim); text-align:right; }}
 td.mono a {{ color:var(--acc); text-decoration:none; }}
 td.mono a:hover {{ text-decoration:underline; }}
 td.coins {{ color:#d7c58a; }} td.why {{ color:var(--dim); font-size:12px; white-space:normal; min-width:200px; }}
 .pos {{ color:var(--pos); }} .neg {{ color:var(--neg); }}
 .pill {{ display:inline-block; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:700; letter-spacing:.4px; }}
 .pill.board {{ background:rgba(47,129,247,.16); color:#5ba3ff; border:1px solid rgba(47,129,247,.4); }}
 .pill.watch {{ background:rgba(158,106,3,.16); color:#e3b341; border:1px solid rgba(158,106,3,.4); }}
 .pill.promote {{ background:rgba(35,134,54,.2); color:#4ac05f; border:1px solid rgba(35,134,54,.5); }}
 .pill.reject {{ background:rgba(120,130,145,.1); color:var(--dim); border:1px solid var(--rej); }}
 .foot {{ color:var(--dim); font-size:11.5px; margin-top:20px; line-height:1.7; }}
 .foot code {{ color:var(--txt); background:#0d131d; padding:1px 5px; border-radius:4px; }}
</style></head>
<body><div class="wrap">
 <h1>copyscan · shadow board</h1>
 <div class="sub">forward paper-track of the top copyable leads · <b>pf2 parked, shadow-only</b> · 100% GitHub-hosted, off the home line · updated {updated}</div>
 <div class="stats">
  <div class="stat"><div class="k">candidates</div><div class="v">{n}</div></div>
  <div class="stat"><div class="k">promoted</div><div class="v pos">{prom}</div></div>
  <div class="stat"><div class="k">board</div><div class="v">{board}</div></div>
  <div class="stat"><div class="k">watch</div><div class="v">{watch}</div></div>
  <div class="stat"><div class="k">rejected</div><div class="v">{reject}</div></div>
 </div>
 <div class="tblwrap"><table>
  <thead><tr><th>#</th><th>verdict</th><th>lead</th><th>copyable</th><th>acct $</th><th>30d $</th>
   <th>fwd days</th><th>fwd trips</th><th>cum our&nbsp;E</th><th>note</th></tr></thead>
  <tbody>
{rows}
  </tbody>
 </table></div>
 <div class="foot">
  <b>Method:</b> off-IP GitHub-Actions runners fetch lead fills (never the home HL budget) → offline forensic vet →
  live open-book loss-hider filter → top picks forward-tracked with the pipeline's own copy-sim. <br>
  <b>PROMOTE</b> = ≥14 days forward · ≥10 forward trips · positive cumulative our&nbsp;E (paper). Vetting engine is private;
  only the board is published. <code>cum our E</code> = per-trip first-entry return − entry-lag − spread, summed.
 </div>
</div></body></html>
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--out", default=os.path.join("docs", "index.html"))
    args = ap.parse_args()
    html_doc = render(args.root)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    open(args.out, "w").write(html_doc)
    print("wrote %s (%d bytes)" % (args.out, len(html_doc)))


if __name__ == "__main__":
    main()
