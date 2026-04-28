# Mini-Plan: Use backtraces.scylladb.com for Backtrace Decoding (SCT-275)

**Date:** 2026-04-28
**Estimated LOC:** ~250
**Related Issue:** SCT-275

## Problem

During longevity tests (e.g. `longevity-10gb-3h-azure-test`), the `addr2line` process on the
monitor node consumes ~6.6 GB RSS when decoding Scylla backtraces from 1+ GB debug binaries.
This OOM-kills the monitor node (8 GB VM), loses monitoring connectivity, and fails the test.

The root cause is `decode_raw_backtrace()` in `sdcm/cluster.py` (line ~2066) which runs
`addr2line -Cpife <debug_file> <addresses>` locally on the monitor node, loading full DWARF
debug info into memory for every backtrace.

## Approach

**Strategy:** Try the external `backtraces.scylladb.com` service first, fall back to local
`addr2line` only when the external service fails.

### Step 1: Add external service helper method

Add `_decode_via_external_service(self, build_id, raw_backtrace)` to the monitor node class
in `sdcm/cluster.py`:
- POST to `https://api.backtrace.scylladb.com/api/backtrace` with JSON body
  `{"build_id": "<hex>", "input": "Backtrace:\n<addresses>"}`
- Timeout: 120 seconds (decoding large binaries can take 30+ seconds on cache miss)
- Return `output.stdout` on success (`response["success"] == True`)
- Raise on failure (404 build not found, network error, `success == false`)

### Step 2: Modify `decode_backtrace()` to try external first

In the `decode_backtrace()` method (line ~1966), which already has `build_id` available from
`obj["build_id"]`:
1. If `build_id` is available, try `_decode_via_external_service(build_id, raw_backtrace)`
2. On success → use the result, **skip** `copy_scylla_debug_info()` and local `addr2line` entirely
3. On failure → log warning, fall back to existing flow (copy debug file + local `addr2line`)
4. If `build_id` is `None` → go directly to local fallback

This avoids copying the 1+ GB debug file to the monitor node when external decoding succeeds,
which is the majority case for official Scylla builds.

### Step 3: Refactor `decode_raw_backtrace()` as local-only fallback

Rename or keep `decode_raw_backtrace()` as the local-only path. The external service path
bypasses it entirely — no signature change needed for the local method.

### Step 4: Add unit tests

Test the new external service path with mocked HTTP responses:
- External success → no local `addr2line` called
- External failure (404) → falls back to local `addr2line`
- External timeout → falls back to local `addr2line`
- No `build_id` → goes directly to local `addr2line`

### Design Notes

- **Build ID source:** Already available in the decoding queue object (`obj["build_id"]`),
  extracted from logs by `DbLogReader` (see `sdcm/db_log_reader.py` line ~157).
- **Input format:** The API requires `"Backtrace:\n"` prefix before raw addresses.
  Addresses can be raw hex (`0x12f34`) or path+offset format.
- **Response format:** `{"success": true, "stdout": "<decoded lines>", "stderr": "", "duration": 5.2}`
- **Existing API usage:** `sdcm/utils/version_utils.py` already calls the same service
  (`/api/search/build_id`) — reuse similar HTTP patterns.
- **No new config params needed initially.** The external service is always tried first;
  local fallback is automatic. Can add a config toggle later if needed.

## Files to Modify

- `sdcm/cluster.py` — Add `_decode_via_external_service()`, modify `decode_backtrace()` to try external first
- `unit_tests/unit/test_decode_backtrace.py` — Add tests for external service path and fallback logic

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_decode_backtrace.py -v`
- [ ] External service success path skips `copy_scylla_debug_info` and local `addr2line`
- [ ] Fallback to local `addr2line` works when external service returns 404
- [ ] Fallback to local `addr2line` works when external service times out
- [ ] No `build_id` case goes directly to local path without attempting external call
- [ ] `uv run sct.py pre-commit` passes
