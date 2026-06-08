# MinicloudManager Full Lifecycle Module

## Problem

The current `sdcm/utils/minicloud.py` (231 lines) has the basic `MinicloudManager` with start/stop/health-check and networking setup. The shell script (`scripts/run-minicloud-provision-test.sh`, 77 lines) still handles: preflight checks, CLI flags (`--s3-passthrough-buckets`, `--aws-region`, `--lightweight-memory`), log file redirection, QEMU orphan cleanup, region preparation, and all env var overrides.

The first successful end-to-end run (Jun 2, 2026) proved the integration works: 3 DB + 1 loader + 1 monitor, scylla-bench write+read, 12.5 minutes total. Now the Python module needs to absorb the shell script's responsibilities so users just set config and run.

## Approach

Expand `sdcm/utils/minicloud.py` to handle everything the shell script does today:

**`MinicloudConfig` dataclass** — all parameters in one place:
- `binary_path` (env: `SCT_MINICLOUD_BINARY`, default: `/usr/local/bin/minicloud`)
- `setup_script_path` (env: `SCT_MINICLOUD_SETUP_SCRIPT`, auto-discovered next to binary)
- `port` (default: 5000)
- `lightweight` (SCT param: `minicloud_lightweight`, default: True)
- `lightweight_memory` (SCT param: `minicloud_lightweight_memory`, default: `"3.5GiB"`)
- `s3_passthrough_buckets` (SCT param, default: `["scylla-qa-keystore", "cloudius-jenkins-test"]`)
- `region` (from `SCT_REGION_NAME`, default: `eu-west-1`)
- `state_dir` (default: `~/.cache/minicloud`)
- `log_file` (default: `~/sct-results/minicloud.log` — proven location from test runs)

**Expand `MinicloudManager` class** — absorb shell script logic:

1. `preflight_check()` → hard-fail with actionable messages if:
   - `/dev/kvm` missing
   - binary not found or not executable
   - setup script not found
   - AWS creds invalid (`aws sts get-caller-identity` fails)
   - sudo unavailable (needed for networking)

2. `start()` → current implementation PLUS:
   - `_kill_orphans()` — kill stale minicloud and qemu-system processes (the `pkill -f "qemu-system"` from shell)
   - Pass `--s3-passthrough-buckets`, `--aws-region`, `--lightweight-memory` CLI flags (currently only `--lightweight` and `--port` are passed)
   - Redirect stdout/stderr to `log_file` (currently uses `subprocess.PIPE` — should write to file for `tail -f` debugging)
   - Write PID file to `state_dir/minicloud.pid`
   - Register `atexit` handler for emergency cleanup

3. `prepare_region()` → NEW: runs `AwsRegion(region_name).configure()` with `AWS_ENDPOINT_URL` set. Currently done by `uv run sct.py prepare-regions` in shell. Should call the Python API directly.

4. `stop()` → current implementation PLUS:
   - Kill orphan QEMU processes (`pkill -f "qemu-system"`)
   - Remove PID file
   - Note: `TerminateInstances` not yet implemented in minicloud — QEMU kill is the only cleanup path

5. `ensure_running()` → NEW: for `collect-logs` / `clean-resources` commands that run after the test. Check PID file, restart if needed, reattach to state.

6. `set_env_overrides()` → NEW: set all the env vars the shell script currently exports:
   ```python
   os.environ["AWS_ENDPOINT_URL"] = endpoint
   os.environ["SCT_MINICLOUD_ENDPOINT_URL"] = endpoint
   os.environ["SCT_IP_SSH_CONNECTIONS"] = "private"
   os.environ["SCT_INSTANCE_PROVISION"] = "on_demand"
   os.environ["SCT_ENTERPRISE_DISABLE_KMS"] = "true"
   os.environ["SCT_FORCE_RUN_IOTUNE"] = "false"
   ```
   Note: `SCT_N_DB_NODES`, `SCT_N_LOADERS`, `SCT_N_MONITOR_NODES`, `SCT_APPEND_SCYLLA_ARGS` are test-specific and stay in the script/user's env.

**`MinicloudError` exception** — includes fix instructions in the message (pattern from existing `FileNotFoundError` messages in current code).

**New SCT config params** (in `sct_config.py` + `defaults/test_default.yaml`):
- `minicloud_lightweight: true` (Boolean)
- `minicloud_lightweight_memory: "3.5GiB"` (String)
- `minicloud_s3_passthrough_buckets: "scylla-qa-keystore,cloudius-jenkins-test"` (StringOrList)

**Integration in `sdcm/tester.py`:**
- In `get_cluster_aws()`: if `is_minicloud_active()` and no minicloud process already running, create `MinicloudManager` from config, call `.start()` then `.prepare_region()`
- In `tearDown()` / `destroy()`: call `.stop()`
- Store as `self.minicloud` on the tester instance

**Shell script becomes a thin wrapper** that just:
1. Sets test-specific env vars (`SCT_N_DB_NODES=3`, `SCT_APPEND_SCYLLA_ARGS`, etc.)
2. Calls `uv run sct.py run-test ...`
3. MinicloudManager handles everything else via tester.py integration

**Lessons from the successful test run (Jun 2):**
- `--lightweight-memory 3.5GiB` works; Scylla `--memory 1200M` is tight but functional
- `cloudius-jenkins-test` bucket must be in S3 passthrough (for EBS snapshots)
- Monitor node gets IP 10.4.0.14 (5th VM) — no collisions with consecutive DHCP
- Cloud-init fast path works for DB AMI (vector pre-installed, 20s vs 110s)
- Loader cloud-init is the bottleneck (110s for vector install) — tracked in SCT-436
- `TerminateInstances` not implemented — cleanup must kill QEMU processes directly
- S3 `GetObjectAcl` returns `None` grants — `set_public_access()` needs defensive fix (separate PR)
- SSM agent start always fails (harmless but wastes ~10s per VM)

## Files to Modify

| File | Change |
|------|--------|
| `sdcm/utils/minicloud.py` | Add `MinicloudConfig` dataclass, expand `MinicloudManager` with `preflight_check()`, `prepare_region()`, `ensure_running()`, `set_env_overrides()`, `_kill_orphans()`, log file redirect, PID file, `--s3-passthrough-buckets`/`--aws-region`/`--lightweight-memory` CLI flags |
| `sdcm/sct_config.py` | Add `minicloud_lightweight`, `minicloud_lightweight_memory`, `minicloud_s3_passthrough_buckets` params |
| `defaults/test_default.yaml` | Add defaults for new params |
| `sdcm/tester.py` | Hook `MinicloudManager.start()` + `.prepare_region()` into `get_cluster_aws()`, `.stop()` into teardown |
| `unit_tests/unit/test_minicloud.py` | Expand: preflight checks, start with full CLI flags, prepare_region mock, orphan kill, ensure_running, set_env_overrides |
| `scripts/run-minicloud-provision-test.sh` | Simplify to env var overrides + `uv run sct.py run-test` (remove preflight, networking, health wait, cleanup trap — all handled by Python) |

## Verification

- [ ] `uv run python -m pytest unit_tests/unit/test_minicloud.py -x` — all tests pass
- [ ] `uv run sct.py pre-commit` passes
- [ ] `MinicloudConfig.from_env()` correctly reads all env vars and SCT params
- [ ] `MinicloudManager.start()` passes `--s3-passthrough-buckets scylla-qa-keystore,cloudius-jenkins-test --aws-region eu-west-1 --lightweight --lightweight-memory 3.5GiB --port 5000`
- [ ] minicloud stdout/stderr written to `~/sct-results/minicloud.log` (not PIPE)
- [ ] `prepare_region()` creates VPC/subnet/SG/keypair via `AwsRegion.configure()`
- [ ] PID file written to `~/.cache/minicloud/minicloud.pid`
- [ ] `stop()` kills QEMU processes and removes PID file
- [ ] `atexit` handler fires if Python crashes mid-test
- [ ] Simplified shell script still runs the full provision test end-to-end
- [ ] Normal AWS path (no minicloud env vars) has zero behavioral change
