import sys
sys.path.insert(0,".")
import gh_fetch
pool = gh_fetch.active_pool()
print("full active pool from LXC leaderboard:", len(pool))
# canary: write first 100 to test the rig + info-API reachability from cloud IPs
open("extra_addrs.txt","w").write("\n".join(pool[:100]) + "\n")
# stash the full pool for the real sweep
open("pool_full.txt","w").write("\n".join(pool) + "\n")
print("wrote extra_addrs.txt (100 canary) + pool_full.txt (%d)"%len(pool))
