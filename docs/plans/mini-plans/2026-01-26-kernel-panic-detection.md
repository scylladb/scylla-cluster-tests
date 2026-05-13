# Mini-Plan: Kernel Panic Detection for Cloud Providers

**Date:** 2026-01-26
**Estimated LOC:** ~600
**Related PR:** #13354
**Owner:** fruch

## Problem

Kernel panics during testing are critical failures that can cause nodes to become unresponsive, leading to test hangs and wasted time. Currently, SCT has no automated mechanism to detect kernel panics, requiring manual log inspection after failures.

## Approach

- Add `KernelPanicEvent` with CRITICAL severity to the event system
- Create `sdcm/kernel_panic_checker.py` with a base class and 4 provider-specific checkers (AWS, GCE, Azure, OCI)
- Each checker runs as a daemon thread with 30-second polling, reading cloud console output for panic signatures
- SSH connectivity check (socket port 22, threshold=3) as secondary signal
- Console output saved to `{logdir}/console_output.log` on stop and on panic detection
- `FileLog` entry added to `ScyllaLogCollector` to collect console logs
- Constructor accepts `node_name`, `host`, `logdir` — no node object passed

## Files to Modify

- `sdcm/sct_events/system.py` — Add `KernelPanicEvent`
- `defaults/severities.yaml` — Register CRITICAL severity
- `sdcm/kernel_panic_checker.py` — New: base class + AWS/GCE/Azure/OCI checkers
- `sdcm/cluster_aws.py` — Integrate checker into node init/destroy
- `sdcm/cluster_gce.py` — Same
- `sdcm/cluster_azure.py` — Same
- `sdcm/cluster_oci.py` — Same
- `sdcm/logcollector.py` — Add `FileLog` for `console_output.log`
- `unit_tests/test_kernel_panic.py` — Pytest-style unit tests

## Verification

- All unit tests pass: `uv run sct.py unit-tests -t test_kernel_panic.py`
- Pre-commit clean: `uv run sct.py pre-commit`
- CI green on AWS, GCE, Azure backends
- No false positives in testing
