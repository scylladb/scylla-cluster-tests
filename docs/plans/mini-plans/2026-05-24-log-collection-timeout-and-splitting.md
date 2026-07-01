---
status: draft
domain: infrastructure
created: 2026-05-24
last_updated: 2026-05-24
owner: fruch
jira: SCT-398
---

# Log Collection Timeout and Large Log Splitting Optimization

## 1. Problem Statement

The Jenkins pipeline's "Collect log data" stage has a **hardcoded 90-minute timeout** that is insufficient for long-running longevity tests. When log collection exceeds this timeout, the entire build is marked as `ABORTED`, masking actual test results.

**Pain points:**

1. **Static timeout**: 90-minute limit regardless of test duration (some tests run 3+ days) or cluster size (38+ nodes)
2. **Slow log splitting**: The `sct-runner-python-log` collection alone takes 60+ minutes for large `sct.log` files (2-3 GB), using an inefficient bash script that reads the file multiple times
3. **Sequential uploads**: After splitting, chunks are uploaded to S3 one at a time
4. **Lost test results**: Build marked ABORTED masks whether the actual test passed or failed

**Evidence**: [longevity-50gb-3days-test #5](https://jenkins.scylladb.com/job/scylla-2026.2/job/vnodes/job/tier1/job/longevity-50gb-3days-test/5/) — timeout hit at exactly 90 minutes, `sct-runner-python-log` was still collecting.

Related: [SCT-284](https://scylladb.atlassian.net/browse/SCT-284), [scylla-cluster-tests#4130](https://github.com/scylladb/scylla-cluster-tests/issues/4130)

## 2. Current State

### Timeout Calculation

**File**: `vars/getJobTimeouts.groovy` (lines 40-65)

```groovy
Integer collectLogsTimeout = 90  // Line 54 — STATIC, the root cause
Integer testRunTimeout = testStartupTimeout + testDuration + testTeardownTimeout
Integer runnerTimeout = testRunTimeout + collectLogsTimeout + resourceCleanupTimeout + sendEmailTimeout
```

The function already computes `testDuration` dynamically from config (line 40-51) but does NOT use it to scale `collectLogsTimeout`.

**Consumers** (apply `timeout(time: collectLogsTimeout, unit: 'MINUTES')`):
- `vars/longevityPipeline.groovy` (line ~240, 280, 310)
- `vars/managerPipeline.groovy`
- `vars/jepsenPipeline.groovy`

### Log Collection — Python Side

**File**: `sdcm/logcollector.py`

- `BaseSCTLogCollector` (line 1200+): `too_big_log_size = 3 * 1024 * 1024 * 1024` (3 GB threshold)
- `PythonSCTLogCollector` (line 1269-1302): Handles `sct.log` collection
  - If file < 3 GB: compresses with zstd as single archive
  - If file >= 3 GB: calls `log_archive.sh` to split, then uploads chunks sequentially

**File**: `sdcm/log_archive.sh` (14 lines)

```bash
# For each chunk: tail -n "+$i" "$1" | head -n "$lines_part" | zstd -o ...
```

**Problems with `log_archive.sh`:**
1. `wc -lc $1` — full file read #1 (counting lines + bytes)
2. `sed -n "${i}p" $1` — random access per chunk for timestamp extraction (O(n) each time)
3. `tail -n "+$i" | head -n "$lines_part"` — reads from beginning each time (O(n*chunks))
4. Sequential compression — each chunk compressed one at a time
5. Total file reads: ~2*chunks + 1 for a file that's already 2-3 GB

### Upload Path

`PythonSCTLogCollector.create_archive_and_upload()` (line 1295-1302):
- Iterates chunks in a `for` loop, calling `upload_archive_to_s3` one at a time — no parallelism

## 3. Goals

1. **Eliminate ABORTED builds caused by log collection timeout** — collect logs timeout scales with test duration so it never hits for healthy collection
2. **Reduce `sct-runner-python-log` collection time from 60+ minutes to under 15 minutes** for 2-3 GB log files
3. **Preserve test status** — log collection failure should degrade gracefully (partial logs) rather than overriding test result with ABORTED
4. **Minimize disk space usage during splitting** — maintain the current design constraint (large SCT disks = higher costs)

## 4. Implementation Phases

### Phase 1: Adaptive Timeout in Jenkins [Importance: Critical]

**Scope**: Single file change — `vars/getJobTimeouts.groovy`

**Implementation**:
```groovy
// Scale collect logs timeout: base 90 min + 1 min per hour of test duration + buffer for large clusters
Integer collectLogsTimeout = Math.max(90, 90 + (testDuration / 60).toInteger())
```

For a 3-day test (4320 min): `max(90, 90 + 72) = 162 minutes`.

Also add an optional override parameter `collect_logs_timeout` that pipelines can pass to override the formula entirely.

**Definition of Done**:
- [ ] `collectLogsTimeout` scales with `testDuration`
- [ ] Optional override parameter respected when provided
- [ ] Formula documented with inline comment explaining rationale
- [ ] Existing test timeouts validated (no regression for short tests)

**Dependencies**: None

### Phase 2: Optimize `log_archive.sh` with `split` Command [Importance: Critical]

**Scope**: Rewrite `sdcm/log_archive.sh` to eliminate redundant file reads.

**Implementation approach**:
- Use GNU `split --line-bytes=$CHUNK_SIZE` to split file in a single pass
- Extract timestamp from first line of each chunk (one `head -1` per chunk, not `sed` on full file)
- Compress chunks in parallel using `xargs -P$(nproc)` or GNU `parallel`
- Remove original chunks after compression to conserve disk

**Expected speedup**: 4-6x (single-pass split + parallel compression)

**Definition of Done**:
- [ ] File is split in a single pass (no repeated full-file reads)
- [ ] Compression runs in parallel across available cores
- [ ] Disk usage at any point ≤ original file + one compressed chunk (space-efficient)
- [ ] Output file naming preserves timestamp convention for S3 ordering
- [ ] Unit test verifying correct splitting behavior with a sample file

**Dependencies**: None

### Phase 3: Parallel S3 Uploads [Importance: High]

**Scope**: `sdcm/logcollector.py` — `PythonSCTLogCollector.create_archive_and_upload()`

**Implementation**:
- Use `concurrent.futures.ThreadPoolExecutor` to upload chunks in parallel (max 4-8 workers)
- Delete each chunk immediately after successful upload (preserves disk space constraint)

**Definition of Done**:
- [ ] Chunks uploaded in parallel (configurable concurrency)
- [ ] Each chunk deleted after successful upload
- [ ] Error handling: if one upload fails, others continue; failures reported but don't crash
- [ ] Unit test with mocked S3 verifying parallel behavior

**Dependencies**: Phase 2 (chunks must exist before uploading)

### Phase 4: Lower `too_big_log_size` Threshold [Importance: Medium]

**Scope**: `sdcm/logcollector.py` line 1209

**Implementation**: Lower from 3 GB to 1 GB. Most problematic logs are 1-3 GB — they currently take the slow single-file compression path. By lowering the threshold, we trigger the (now-optimized) split path earlier, getting parallel compression benefits sooner.

**Definition of Done**:
- [ ] Threshold changed to 1 GB
- [ ] Verified no regression for logs < 1 GB (still use simple compression)
- [ ] Existing unit tests pass

**Dependencies**: Phase 2 (splitting must be efficient before lowering threshold)

### Phase 5: Graceful Degradation — Non-Fatal Log Collection [Importance: Medium]

**Scope**: `vars/longevityPipeline.groovy` (and other pipeline consumers)

**Implementation**: When the collect-logs stage times out, catch the timeout exception, mark logs as partial, and preserve the original test result (`SUCCESS`/`FAILURE`/`UNSTABLE`) instead of overriding to `ABORTED`.

```groovy
try {
    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
        runCollectLogs(...)
    }
} catch (org.jenkinsci.plugins.workflow.steps.FlowInterruptedException e) {
    currentBuild.description += " [PARTIAL LOGS]"
    // Do NOT set currentBuild.result = 'ABORTED'
}
```

**Definition of Done**:
- [ ] Log collection timeout does NOT override build result
- [ ] Build description annotated with `[PARTIAL LOGS]` when timeout occurs
- [ ] Actual test result preserved in Jenkins and Argus
- [ ] Tested manually by setting artificially low timeout

**Dependencies**: Phase 1 (adaptive timeout reduces how often this triggers)

### Phase 6: Documentation Update [Importance: Low]

**Scope**: Update relevant docs on timeout configuration.

**Definition of Done**:
- [ ] `docs/configuration_options.md` regenerated if new param added
- [ ] Inline comments in `getJobTimeouts.groovy` explain the formula

**Dependencies**: Phases 1-5

## 5. Testing Requirements

### Unit Tests
- **Phase 2**: Test `log_archive.sh` with a synthetic multi-GB file (or small file simulating the split logic) — verify correct chunk count, naming, compression
- **Phase 3**: Mock S3 upload, verify parallel execution and error handling
- **Phase 4**: Existing `unit_tests/unit/test_log_collector.py` must pass with new threshold

### Integration Tests
- **Phase 1**: Run a short docker-backend test and verify computed timeouts in Jenkins console output
- **End-to-end**: Run with `--backend docker` generating a >1 GB sct.log (can be simulated by appending data), verify split + upload completes

### Manual Tests
- **Phase 1**: Inspect Jenkins output for various test durations (30 min, 6h, 72h configs) — verify reasonable timeout values
- **Phase 5**: Set `collectLogsTimeout = 1` artificially, run a test, verify build is NOT marked ABORTED

## 6. Success Criteria

- No longevity test builds marked ABORTED solely due to log collection timeout (Phase 1 + 5)
- `sct-runner-python-log` collection for 2-3 GB logs completes in under 15 minutes (Phases 2 + 3 + 4)
- Disk usage during log splitting never exceeds `original_size + chunk_size` (Phase 2)
- All existing unit tests pass without modification (except threshold value change)

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `split --line-bytes` not available on all SCT runner images | Low | High | Check coreutils version in Dockerfile; fall back to current approach if unavailable |
| Parallel compression exhausts CPU on small runners | Medium | Low | Cap parallelism at `min(nproc, 4)` |
| Parallel S3 uploads hit rate limits | Low | Medium | Use exponential backoff; limit concurrency to 4 |
| Lowering threshold to 1 GB causes unnecessary splits for medium logs | Low | Low | Monitor split frequency; adjust threshold if needed |
| Graceful degradation in Phase 5 masks genuine infrastructure issues | Medium | Medium | Add Argus alert for `[PARTIAL LOGS]` annotations; track frequency |
