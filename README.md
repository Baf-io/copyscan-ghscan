# ghscan — GitHub-Actions multi-IP HL sweep

Free, fast HL scanning by fanning out across GitHub-Actions runners — **each runner is a distinct
IP with its own HL REST budget**, so 20 shards ≈ 20× throughput with no single-IP throttling.
The full ~5,000-address active pool sweeps in **~10–12 min** vs ~13 hr single-IP on the LXC.

**Why this is safe:** the live copy bot executes on **Variational** and only *watches* HL leads over
**WebSocket** — a separate channel from the REST `/info` API this sweep hits. Hammering HL REST has
zero effect on the live bot.

## Split design (no logic vendored onto runners)
- **Runner (`gh_fetch.py`, stdlib only):** leaderboard → same pre-filter as `scan.py` → its shard
  slice (`pool[k::20]`) → capped fills (90d/4pg) + positions per address → uploads raw
  `{addr, fills, positions}` as an artifact. Pure fetching.
- **LXC (`gh_ingest.py`):** downloads the artifacts and vets every record through
  `scan.scan_addr` **unchanged** (by patching the fetch hooks to serve the pre-fetched data), so the
  GH sweep and a local `scan.py hl` run vet with byte-identical forensic/ruleset logic. Writes
  `probes/scan_hl.jsonl` + the vetted cache.

## One-time deploy (needs your GitHub account)
This `ghscan/` directory IS the repo root.
```bash
cd /root/copyscan/ghscan
git init && git add . && git commit -m "ghscan: multi-IP HL sweep"
gh repo create copyscan-ghscan --private --source=. --push      # or push to a repo you create
```
(Install gh first if needed: `apt-get install gh`, then `gh auth login`. HL's API is public — no
secrets required.)

## Run a sweep
```bash
gh workflow run hl-sweep --repo <you>/copyscan-ghscan     # or click "Run workflow" in the Actions tab
# wait ~10-12 min, then on the LXC:
cd /root/copyscan/ghscan
gh run download --repo <you>/copyscan-ghscan -D artifacts/    # pulls all 20 shard artifacts
python3 gh_ingest.py artifacts/                               # vet -> probes/scan_hl.jsonl
cd /root/copyscan && python3 consolidate.py                  # fold survivors into roster.jsonl
# bot hot-reloads roster within 600s; then apply the conviction/lag bar as usual
```

## Tuning
- **More/fewer IPs:** edit the `matrix.shard` list + `SHARD_TOTAL` in `.github/workflows/hl-sweep.yml`
  (GitHub free tier caps ~20 concurrent jobs).
- **Per-IP pace:** `DELAY` env in the workflow (2.5s default; each IP has its own budget, so it can be
  tighter than the LXC's 5s single-IP pace).
- **Pool filter / fill cap:** keep in sync with `scan.py` (`HL_PF_*`, `HL_FILLS_DAYS/PAGES`).
