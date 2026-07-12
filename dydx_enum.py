#!/usr/bin/env python3
"""dydx_enum.py — GitHub-Actions SHARD CRAWLER: dYdX v4 ACTIVE-trader enumeration via Cosmos
LCD block decode. STDLIB ONLY (bare runner, no pip).

WHY THIS RUNS ON GH RUNNERS, NOT THE LXC: a chain block-crawl fans out one HTTP flow per block;
run from the home LXC it spiked router NAT conntrack (~78/s) and starved Roel's connection
(2026-07-12 incident — the LXC crawler /root/copyscan/discover_dydx.py was KILLED and neutralized
for exactly this). Each GitHub-Actions runner is a DISTINCT cloud IP with its OWN budget and ZERO
home-network impact, so N shards crawl N disjoint block slices in parallel with no home concern.

DECODE LOGIC copied verbatim from discover_dydx.py (the LXC crawler that proved it): the dYdX v4
indexer's public per-market surfaces are ANONYMOUS, but the LCD decodes every block's txs to JSON
and the clob trading messages carry the subaccount owner in-band:
  - clob.MsgPlaceOrder (stateful):   order.order_id.subaccount_id.owner
  - clob.MsgProposedOperations.operations_queue[]:
      match.match_orders.taker_order_id.subaccount_id.owner + every fill's maker_order_id...owner
      match.match_perpetual_liquidation -> liquidated subaccount owner
      short_term_order_placement = base64 protobuf blob; protobuf strings are raw bytes so the
        bech32 owner is a plain substring -> regex the decoded bytes, no protobuf lib needed.
VERIFIED 2026-07-12 on live LCD: block with a real BTC trade -> 4 distinct dydx1 owners via the
place/taker/maker/short_term_b64 paths. Most 1.3s blocks are EMPTY (dYdX v4 is low-activity in
2026 — BTC-USD trades ~every 30-60s), so the union of shards crawls a full recent window and the
active traders (who repeat) get harvested across it.

SHARDING: all shards floor the tip to the nearest 1000 (drift-immune alignment as shards start a
few seconds apart), take the window [base-BLOCKS, base), and each shard owns the interleaved class
h % SHARD_TOTAL == SHARD_INDEX. Union of all shards = every block in the window (full coverage);
a dead shard costs 1/N evenly spread, never a whole time-chunk.

Addresses are bech32 'dydx1...' (NOT 0x). Output = one jsonl row per distinct owner with light
activity stats + top co-fill counterparties (the SYBIL meta — see below).

SYBIL GUARD: a prior dYdX cohort was ONE sybil fund with identical-millisecond fills. Every addr
here REQUIRES the entity-dedup check at VET time (Jaccard ticker overlap + same-millisecond
co-fills). cp_top_frac ~1.0 = fills mostly against one counterparty = wash/sybil smell.

env: SHARD_INDEX · SHARD_TOTAL · DYDX_BLOCKS (GLOBAL window across all shards) · DYDX_DELAY
     (per-request throttle) · DYDX_TIP (optional tip override) · DYDX_MAX_MIN (soft wall-clock) · OUT
"""
import os, json, base64, re, time, urllib.request, urllib.error

LCDS = [
    'https://dydx-lcd.publicnode.com',
    'https://dydx-api.polkachu.com',
    'https://rest-dydx.ecostake.com',
]
HDR = {'User-Agent': 'copyscan-ghenum/1.0', 'Accept': 'application/json'}
BECH32_RE_S = re.compile(r'dydx1[a-z0-9]{38}')
BECH32_RE_B = re.compile(rb'dydx1[a-z0-9]{38}')


def get(path, start_lcd=0, timeout=15, tries=2):
    """GET path from the LCDs, starting at start_lcd, rotating on failure. Returns JSON or None."""
    n = len(LCDS)
    for a in range(tries):
        for k in range(n):
            base = LCDS[(start_lcd + k) % n]
            try:
                req = urllib.request.Request(base + path, headers=HDR)
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    time.sleep(2.0)
                continue
            except Exception:
                continue
        time.sleep(1.0)
    return None


def tip_height(start_lcd=0):
    h = get('/cosmos/base/tendermint/v1beta1/blocks/latest', start_lcd=start_lcd)
    if not h:
        return 0
    try:
        return int(h['block']['header']['height'])
    except Exception:
        return 0


def bump(addrs, addr, field, height):
    a = addrs.get(addr)
    if a is None:
        a = {'fs': height, 'ls': height, 'blk': 0, 'tk': 0, 'mk': 0,
             'sp': 0, 'st': 0, 'cx': 0, 'liq': 0, 'cp': {}}
        addrs[addr] = a
    a['fs'] = min(a['fs'], height)
    a['ls'] = max(a['ls'], height)
    a[field] += 1
    return a


def cofill(addrs, taker, maker):
    """Record taker<->maker co-fills (sybil signal). Keep the heaviest counterparties only."""
    for x, y in ((taker, maker), (maker, taker)):
        a = addrs.get(x)
        if a is None:
            continue
        cp = a['cp']
        cp[y] = cp.get(y, 0) + 1
        if len(cp) > 12:
            for kk in sorted(cp, key=cp.get)[:len(cp) - 8]:
                del cp[kk]


def scan_block(addrs, d, height):
    """Decode one block's txs, harvest clob owners. Returns the set of owners seen in this block."""
    seen = set()
    for tx in d.get('txs') or []:
        for m in tx.get('body', {}).get('messages', []):
            t = m.get('@type', '')
            if t.endswith('clob.MsgPlaceOrder'):
                o = (m.get('order') or {}).get('order_id', {}).get('subaccount_id', {}).get('owner')
                if o:
                    bump(addrs, o, 'sp', height); seen.add(o)
            elif t.endswith('clob.MsgCancelOrder') or t.endswith('clob.MsgBatchCancel'):
                o = (m.get('order_id') or m.get('subaccount_id') or {})
                o = o.get('subaccount_id', o).get('owner') if isinstance(o, dict) else None
                if o:
                    bump(addrs, o, 'cx', height); seen.add(o)
            elif t.endswith('clob.MsgProposedOperations'):
                for op in m.get('operations_queue') or []:
                    stp = op.get('short_term_order_placement')
                    if stp:
                        try:
                            for mo in BECH32_RE_B.findall(base64.b64decode(stp)):
                                o = mo.decode()
                                bump(addrs, o, 'st', height); seen.add(o)
                        except Exception:
                            pass
                        continue
                    match = op.get('match') or {}
                    mo_ = match.get('match_orders')
                    if mo_:
                        taker = (mo_.get('taker_order_id') or {}).get('subaccount_id', {}).get('owner')
                        if taker:
                            bump(addrs, taker, 'tk', height); seen.add(taker)
                        for fill in mo_.get('fills') or []:
                            maker = (fill.get('maker_order_id') or {}).get('subaccount_id', {}).get('owner')
                            if maker:
                                bump(addrs, maker, 'mk', height); seen.add(maker)
                                if taker:
                                    cofill(addrs, taker, maker)
                        continue
                    liq = match.get('match_perpetual_liquidation')
                    if liq:
                        liqd = liq.get('liquidated')
                        o = liqd.get('owner') if isinstance(liqd, dict) else None
                        if not o:
                            mm = BECH32_RE_S.search(json.dumps(liq))
                            o = mm.group(0) if mm else None
                        if o:
                            bump(addrs, o, 'liq', height); seen.add(o)
    for o in seen:
        addrs[o]['blk'] += 1
    return seen


def main():
    shard = int(os.environ.get('SHARD_INDEX', '0'))
    total = int(os.environ.get('SHARD_TOTAL', '20'))
    blocks = int(os.environ.get('DYDX_BLOCKS', '12000'))   # GLOBAL window across all shards
    delay = float(os.environ.get('DYDX_DELAY', '0.6'))
    tip_override = int(os.environ.get('DYDX_TIP', '0') or '0')
    max_min = float(os.environ.get('DYDX_MAX_MIN', '18'))
    out = os.environ.get('OUT', f'enum_dydx_shard_{shard}.jsonl')

    start_lcd = shard % len(LCDS)   # spread load across the public nodes
    tip = tip_override or tip_height(start_lcd)
    if not tip:
        print(f'[dydx shard {shard}/{total}] NO TIP (all LCDs failed) -> empty', flush=True)
        open(out, 'w').close()
        return
    base = (tip // 1000) * 1000     # align across shards (drift-immune)
    start = max(base - blocks, 1)
    heights = [h for h in range(start, base) if h % total == shard]

    addrs = {}
    ok = 0
    deadline = time.time() + max_min * 60
    for h in heights:
        if time.time() > deadline:
            print(f'[dydx shard {shard}] soft deadline hit at block {h}', flush=True)
            break
        d = get(f'/cosmos/tx/v1beta1/txs/block/{h}', start_lcd=start_lcd)
        if not isinstance(d, dict):
            time.sleep(delay); continue
        try:
            scan_block(addrs, d, h)
            ok += 1
        except Exception:
            pass
        time.sleep(delay)

    with open(out, 'w') as f:
        for a, s in addrs.items():
            cp = sorted(s['cp'].items(), key=lambda kv: -kv[1])[:5]
            cp_total = sum(s['cp'].values())
            f.write(json.dumps({
                'addr': a, 'venue': 'dydx_v4',
                'first_seen_h': s['fs'], 'last_seen_h': s['ls'], 'blocks_active': s['blk'],
                'taker_fills': s['tk'], 'maker_fills': s['mk'], 'liq_events': s['liq'],
                'stateful_places': s['sp'], 'short_term_places': s['st'], 'cancels': s['cx'],
                'cp_top': cp, 'cp_top_frac': round(cp[0][1] / cp_total, 3) if cp and cp_total else None,
                'sybil_check_required': True,
                'shard': shard, 'shard_blocks_ok': ok,
            }) + '\n')
    print(f'[dydx shard {shard}/{total}] window=[{start},{base}) blocks_ok={ok} '
          f'distinct_owners={len(addrs)} -> {out}', flush=True)


if __name__ == '__main__':
    main()
