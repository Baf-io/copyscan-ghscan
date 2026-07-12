#!/usr/bin/env python3
"""gh_dydx_enum_ingest.py — LXC merge lane for the GH-distributed dYdX v4 enumeration (dydx-enum.yml).
Reads the concatenated per-shard artifact jsonl (argv[1]), extracts dydx1 owner addrs + their light
activity/co-fill meta, and MERGES them into probes/onchain_dydx_addrs.txt (dedup vs out/roster.jsonl
and vs the file's own prior content, so the pool ACCUMULATES across daily runs). Appends/merges the
per-addr meta into probes/onchain_dydx_meta.jsonl.

NEVER writes out/roster.jsonl. This only produces the ONCHAIN-DYDX probe pool — the same file the
now-neutralized LXC crawler discover_dydx.py used to write. Runs ENTIRELY off the downloaded GitHub
artifacts; it does NOT crawl the chain (that is the whole point — all crawling happens on GH runners).

SYBIL GUARD: every dydx candidate REQUIRES the entity-dedup check at VET time (Jaccard ticker overlap
+ same-millisecond co-fills). A prior dYdX cohort was ONE sybil fund. cp_top_frac ~1.0 = fills mostly
against one counterparty = wash/sybil smell. The header of onchain_dydx_addrs.txt carries this note.
"""
import sys, os, json, time, re

ROOT = '/root/copyscan'
ADDR_OUT = f'{ROOT}/probes/onchain_dydx_addrs.txt'
META_OUT = f'{ROOT}/probes/onchain_dydx_meta.jsonl'
ROSTER = f'{ROOT}/out/roster.jsonl'
BECH32 = re.compile(r'^dydx1[a-z0-9]{38}$')


def load_roster_dydx():
    s = set()
    if os.path.exists(ROSTER):
        for line in open(ROSTER):
            try:
                a = json.loads(line).get('addr', '')
            except Exception:
                continue
            if isinstance(a, str) and a.startswith('dydx1'):
                s.add(a)
    return s


def load_existing_addrs():
    s = set()
    if os.path.exists(ADDR_OUT):
        for line in open(ADDR_OUT):
            line = line.strip()
            if BECH32.match(line):
                s.add(line)
    return s


def load_existing_meta():
    d = {}
    if os.path.exists(META_OUT):
        for line in open(META_OUT):
            try:
                r = json.loads(line)
            except Exception:
                continue
            a = r.get('addr')
            if a:
                d[a] = r
    return d


def main():
    if len(sys.argv) < 2 or not os.path.exists(sys.argv[1]):
        print('usage: gh_dydx_enum_ingest.py <merged_shard_jsonl>')
        sys.exit(2)
    src = sys.argv[1]

    roster = load_roster_dydx()
    existing = load_existing_addrs()
    meta = load_existing_meta()

    new_addrs = set()
    for line in open(src):
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        a = r.get('addr', '')
        if not (isinstance(a, str) and BECH32.match(a)):
            continue
        new_addrs.add(a)
        # keep the richer meta row per addr (prefer the one with more fills)
        prev = meta.get(a)
        if prev is None or (r.get('taker_fills', 0) + r.get('maker_fills', 0)) > \
                (prev.get('taker_fills', 0) + prev.get('maker_fills', 0)):
            r['sybil_check_required'] = True
            meta[a] = r

    # union of all-time discovered, minus anything already promoted into the roster
    pool = (existing | new_addrs) - roster
    net_new = len([a for a in new_addrs if a not in existing and a not in roster])

    # .bak the addr file before rewriting
    if os.path.exists(ADDR_OUT):
        bak = f'{ADDR_OUT}.bak-{time.strftime("%Y%m%d-%H%M%S")}'
        try:
            os.replace(ADDR_OUT, bak)
        except Exception:
            pass

    header = [
        '# onchain_dydx_addrs.txt — ACTIVE dYdX v4 traders harvested from decoded blocks.',
        '# SOURCE: GitHub-Actions multi-IP crawler (dydx-enum.yml -> dydx_enum.py), merged by',
        '#   gh_dydx_enum_ingest.py. The LXC crawler discover_dydx.py is NEUTRALIZED (it hammered',
        '#   home-router NAT). All chain crawling now happens on cloud runners.',
        '# (r) SYBIL GUARD: dYdX candidates REQUIRE entity-dedup at VET time — Jaccard ticker overlap',
        '#   + same-millisecond co-fills across addrs. A prior dYdX cohort was ONE sybil fund with',
        '#   identical-ms fills. See probes/onchain_dydx_meta.jsonl: cp_top_frac ~1.0 = fills mostly',
        '#   one counterparty (wash/sybil smell). Nothing here reaches a board without the full megadb',
        '#   funnel + Roel. dydx addrs are bech32 dydx1..., NOT 0x.',
        f'# excluded_in_roster={len(roster)} pool_total={len(pool)} net_new_this_run={net_new} '
        f'updated={time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}',
    ]
    tmp = ADDR_OUT + '.tmp'
    with open(tmp, 'w') as f:
        f.write('\n'.join(header) + '\n')
        f.write('\n'.join(sorted(pool)) + '\n')
    os.replace(tmp, ADDR_OUT)

    # rewrite merged meta (bounded to distinct addrs)
    tmpm = META_OUT + '.tmp'
    with open(tmpm, 'w') as f:
        for a in sorted(meta):
            f.write(json.dumps(meta[a]) + '\n')
    os.replace(tmpm, META_OUT)

    print(f'[dydx-ingest] new_seen={len(new_addrs)} net_new={net_new} pool_total={len(pool)} '
          f'meta_rows={len(meta)}')


if __name__ == '__main__':
    main()
