# Minicloud Provision Test Speedup

## Problem

The first successful end-to-end minicloud provision test (3 DB + 1 loader + 1 monitor) takes **12.5 minutes**. The theoretical floor is ~5 minutes (3min stress workload + irreducible SCT overhead). Sequential provisioning, unnecessary package installs, slow cloud-init steps, and under-resourced VMs account for ~6 minutes of waste.

Target: **~6.5 minutes** (halved from 12.5).

## Approach

Six independent speedups ranked by impact:

### 1. Provision monitor in parallel with loader (~2min saved)

Currently SCT provisions sequentially: DB → wait → loader → wait → monitor → wait. The monitor has no dependency on the loader — it only needs DB node IPs. Provision monitor concurrently with loader.

**Options:**
- (a) SCT code change: launch monitor cluster init in a thread alongside loader init
- (b) Script-level: not feasible since `sct.py run-test` controls the sequence internally

This requires an SCT code change in `tester.py` setup flow.

### 2. Skip SSM agent start in cloud-init (~10-30s per VM, ~50-150s total)

The user-data script runs `systemctl start amazon-ssm-agent` which always fails on minicloud (SSM not implemented). Add a minicloud detection guard:

```bash
# Skip SSM on minicloud (no Systems Manager endpoint)
if curl -s --connect-timeout 1 http://169.254.169.254/latest/meta-data/services/domain | grep -q "minicloud"; then
    echo "Minicloud detected, skipping SSM agent"
else
    systemctl unmask amazon-ssm-agent && systemctl enable amazon-ssm-agent && systemctl start amazon-ssm-agent
fi
```

Alternative: set an env var `SCT_SKIP_SSM_AGENT=true` in the minicloud script and check it in the user-data template.

### 3. Pre-install lsof, net-tools in AMI snapshots (~30-60s saved)

After cloud-init, SCT runs `apt-get update && apt-get install lsof net-tools` on every node. These are small packages but `apt-get update` hits remote mirrors.

**Fix:** Run once on each base AMI instance, then re-snapshot. One-time operation.

### 4. Bump VM RAM to 4GiB / Scylla memory to 2G (~30-60s on Raft bootstrap)

Current: `--lightweight-memory 3.5GiB`, Scylla `--memory 1200M`. Raft group0 bootstrap across 3 nodes is memory-bound.

**Fix:** In the minicloud script:
```bash
export MINICLOUD_LIGHTWEIGHT_MEMORY="4GiB"
export SCT_APPEND_SCYLLA_ARGS="--memory 2G"
```

Requires host machine has sufficient RAM (5 VMs × 4GiB = 20GiB).

### 5. Use 2 DB nodes instead of 3 for faster Raft bootstrap (~30s saved)

Raft consensus with 2 nodes bootstraps faster than 3. For a basic provision smoke test, RF=2 is sufficient.

**Fix:** `SCT_N_DB_NODES=2` in the minicloud script. Trade-off: less realistic test.

### 6. Fix S3 ACL upload error (correctness, not speed)

`set_public_access()` in `sdcm/utils/common.py:316` crashes when `acl_obj.grants` is `None` (minicloud S3 passthrough doesn't return grants).

**Fix:** `grants = copy.deepcopy(acl_obj.grants or [])`

## Files to Modify

| File | Change |
|------|--------|
| `sdcm/utils/common.py:316` | Handle `None` grants in `set_public_access()` |
| `sdcm/provision/common/utils.py` | Add SSM skip logic based on env var |
| `sdcm/tester.py` (setup flow) | Parallelize monitor + loader provisioning (item #1) |
| `scripts/run-minicloud-provision-test.sh` | Bump RAM, optionally reduce DB nodes, set `SCT_SKIP_SSM_AGENT` |
| AMI snapshots (one-time manual) | Pre-install lsof, net-tools, re-snapshot |

## Verification

- [ ] `uv run sct.py pre-commit` passes
- [ ] Unit tests pass: `uv run sct.py unit-tests`
- [ ] End-to-end minicloud provision test completes in <7 minutes
- [ ] No new errors in `events_log/error.log`
- [ ] S3 upload warning gone (grants fix)
- [ ] All 5 nodes provision successfully with unique IPs
- [ ] Scylla cluster forms and stress workload completes
