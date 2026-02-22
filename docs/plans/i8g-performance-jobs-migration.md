# Plan: Migration to i8g Instance Type for Performance Testing

## 1. Problem Statement

The current performance regression testing infrastructure uses i4i.4xlarge (x86_64) instances for gradual throughput tests. The AWS i8g instance family (ARM64/Graviton) offers significantly higher throughput capabilities, requiring a comprehensive migration plan to:
- Leverage improved hardware performance of ARM-based i8g instances
- Recalibrate throughput steps based on new maximum sustainable throughput
- Validate latency expectations remain within acceptable ranges
- Maintain support for releases that don't support i8g instances
- Preserve rollback capabilities to ensure infrastructure reliability

**Important:** The i8g instance family is ARM64-only (AWS Graviton processors), not x86_64. This migration represents both an instance type change and an architecture change from x86_64 to ARM64.

The migration impacts critical weekly performance testing jobs for both vnodes and tablets configurations across Scylla releases that support ARM64 architecture: 2025.3, 2025.4, and master. **Releases 2024.1, 2024.2, 2025.1, and 2025.2 do not support i8g and will remain on i4i instances.**

## 2. Current State

### Existing Infrastructure

**Primary Test Jobs:**
- **Location:** `/jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/`
- **Vnodes job:** `scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes.jenkinsfile`
  - Config: Uses `test-cases/performance/perf-regression-predefined-throughput-steps.yaml`
  - Instance type: i4i.4xlarge (line 54 of test config)
  - Test class: `PerformanceRegressionPredefinedStepsTest`
  - Throttle steps: `configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml`
  - Latency thresholds: `configurations/performance/latency-decorator-error-thresholds-steps-ent-vnodes.yaml`
  - Sub-tests: test_read_gradual_increase_load, test_mixed_gradual_increase_load, test_read_disk_only_gradual_increase_load

- **Vnodes write job:** `scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes.jenkinsfile`
  - Same config as vnodes job
  - Sub-tests: test_write_gradual_increase_load (separate job for write workload)

- **Tablets job:** `scylla-enterprise-perf-regression-predefined-throughput-steps-tablets.jenkinsfile`
  - Same config base as vnodes but with tablets enabled
  - Latency thresholds: `configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml`
  - Sub-tests: test_read_gradual_increase_load, test_mixed_gradual_increase_load, test_read_disk_only_gradual_increase_load

- **Tablets write job:** `scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets.jenkinsfile`
  - Same config as tablets job
  - Sub-tests: test_write_gradual_increase_load (separate job for write workload)

**Master Trigger:**
- **Location:** `/jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile`
- Calls: `perfRegressionParallelPipelinebyRegion()` from `vars/perfRegressionParallelPipelinebyRegion.groovy`
- Schedules: Weekly runs (Sunday 6am) with label 'master-weekly'
- Current versions: 2024.1, 2024.2, 2025.1, 2025.2, 2025.3, 2025.4, master (lines 56-70)
- **i8g migration scope:** Only 2025.3, 2025.4, and master (ARM64-supporting releases)

**Current Throughput Configuration:**
- **File:** `configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml`
- Read steps: 150K, 300K, 450K, 600K, 700K, unthrottled ops/sec
- Write steps: 200K, 300K, unthrottled ops/sec
- Mixed steps: 50K, 150K, 300K, 450K, unthrottled ops/sec
- Read disk-only: 80K, 165K, 250K, 300K, unthrottled ops/sec
- Threads: Fixed per workload (read: 620, write: 400, mixed: 1900, read_disk_only: 620)

**Current Latency Expectations (P99):**
- **Vnodes** (`latency-decorator-error-thresholds-steps-ent-vnodes.yaml`):
  - Read 150K-300K: ≤1ms, 450K: ≤5ms, 600K: ≤40ms, 700K: ≤50ms
  - Read disk-only 80K-165K: ≤5ms, 250K: ≤50ms
  - Mixed 50K-150K: ≤3ms (both r/w), 300K: ≤5ms (both r/w)
  - Write: No P99 latency limits for unthrottled write test
- **Tablets** (`latency-decorator-error-thresholds-steps-ent-tablets.yaml`):
  - Read 150K-300K: ≤1ms, 450K: ≤3ms (more strict than vnodes), 600K: ≤40ms, 700K: ≤50ms
  - Read disk-only: No P99 latency limits (all null)
  - Mixed 50K-150K: ≤3ms (both r/w), 300K: ≤5ms (both r/w)
  - Write: No P99 latency limits for unthrottled write test

**Current Throughput Expectations (Maximum Expected):**
- **Vnodes** (`latency-decorator-error-thresholds-steps-ent-vnodes.yaml`):
  - Write (unthrottled): ≥284,479 ops/sec
  - Read (unthrottled): ≥862,336 ops/sec
  - Read disk-only (unthrottled): ≥300,000 ops/sec
  - Mixed write (unthrottled): ≥252,383 ops/sec
- **Tablets** (`latency-decorator-error-thresholds-steps-ent-tablets.yaml`):
  - Write (unthrottled): ≥235,014 ops/sec
  - Read (unthrottled): ≥827,911 ops/sec
  - Read disk-only (unthrottled): No throughput limit (null)
  - Mixed write (unthrottled): ≥270,615 ops/sec

**Instance Type Configurations:**
- Existing: `configurations/aws/i4i_4xlarge.yaml` (single line: instance_type_db)
- ARM pattern: `configurations/arm_instance_types/i8g_2xlarge.yaml` (includes --no-ec2-check flag)

**Test Implementation:**
- **File:** `performance_regression_gradual_grow_throughput.py`
- **Class:** `PerformanceRegressionPredefinedStepsTest` (line 49)
- Reads throttle steps from `perf_gradual_throttle_steps` parameter
- Reads thread counts from `perf_gradual_threads` parameter
- Supports both static ops/sec and percentage-based throttling (for future use)

**i8g Current Usage:**
- **Architecture:** ARM64-only (AWS Graviton processors)
- Already used in: longevity tests (i8ge.xlarge, i8ge.2xlarge), rolling-upgrade-ami-arm, vector-search
- ARM config exists: `configurations/arm_instance_types/i8g_2xlarge.yaml`
- All i8g configs use ARM64 AMIs (see `configurations/arm/` directory)
- Note: i8g.2xlarge supported from Scylla 2025.3.3+ (comment in jenkins-pipelines/oss/rolling-upgrade/rolling-upgrade-ami-arm.jenkinsfile line 13)

**Version Support for i8g (ARM64):**
- **Confirmed unsupported:** 2024.1, 2024.2, 2025.1, 2025.2 - these releases do not support i8g/ARM64 and will remain on i4i (x86_64)
- **Supported:** 2025.3.3+, 2025.4.x, master - these releases support ARM64 architecture and i8g instances
- **Needs Investigation:** Exact minimum minor version for 2025.3.x (known: at least 2025.3.3+)

## 3. Goals

1. **Create new i8g-based performance jobs** for vnodes and tablets (including separate write jobs) with proper ARM64 configuration
2. **Validate infrastructure stability** using last week's AMIs before production use
3. **Recalibrate throughput steps** based on i8g maximum sustainable throughput using percentage-based approach
4. **Update P99 latency thresholds** to reflect i8g performance characteristics
5. **Migrate master trigger** to use new jobs while preserving rollback capability
6. **Update release-specific triggers** to use i8g jobs only for ARM64-supporting releases (2025.3+, 2025.4, master)
7. **Maintain backward compatibility** for x86_64-only releases (2024.1, 2024.2, 2025.1, 2025.2) that don't support i8g/ARM64

## 4. Implementation Phases

### Phase 1: Create Baseline i8g Jobs (Validation Ready)

**Description:** Create new Jenkins jobs and configuration files for i8g instances with existing throughput steps, ready for validation.

**Definition of Done:**
- [ ] Create `/configurations/aws/i8g_4xlarge.yaml` configuration file (or update existing ARM configuration)
- [ ] Create new Jenkins jobs for i8g:
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes-i8g.jenkinsfile`
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes-i8g.jenkinsfile`
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-tablets-i8g.jenkinsfile`
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets-i8g.jenkinsfile`
- [ ] Jobs reference i8g configuration with ARM64 AMIs instead of i4i
- [ ] Jobs use existing throughput steps from `cassandra_stress_gradual_load_steps_enterprise.yaml`
- [ ] Jobs use existing latency thresholds (vnodes/tablets)
- [ ] Jobs can be triggered manually (not yet in master trigger)
- [ ] Documentation: Add comments explaining these are validation jobs

**Dependencies:** None

**Deliverables:**
- New configuration file for i8g.4xlarge
- Four new Jenkins job files (vnodes, vnodes-write, tablets, tablets-write)

### Phase 2: Validation with Last Week's AMIs

**Description:** Execute validation runs using recent AMIs to ensure infrastructure stability and collect baseline performance data.

**Definition of Done:**
- [ ] Identify appropriate AMI IDs for validation (last week's master builds)
- [ ] Execute manual runs of both vnodes and tablets jobs
- [ ] Verify all infrastructure components provision successfully
- [ ] Confirm P99 latencies are within expected ranges per existing threshold files
- [ ] Document no infrastructure stability issues (node failures, network issues)
- [ ] Document observed maximum sustainable throughput per workload type
- [ ] Document unthrottled step performance metrics
- [ ] Create validation report with:
  - [ ] Maximum observed throughput per workload (read, write, mixed, read_disk_only)
  - [ ] P99 latency at each existing step
  - [ ] Any infrastructure anomalies
  - [ ] Recommendations for step recalibration

**Dependencies:** Phase 1

**Deliverables:**
- Validation test runs (stored in Jenkins/test results)
- Validation report document (markdown in /docs or test results comment)
- Baseline throughput measurements

### Phase 3: Finalize Version Support Matrix

**Description:** Confirm exact minor versions that support i8g/ARM64 for releases 2025.3 and 2025.4, and document the migration strategy.

**Definition of Done:**
- [ ] Confirm i8g/ARM64 support for ARM64-capable releases:
  - [ ] 2025.3.x - identify exact minimum minor version (known: at least 2025.3.3)
  - [ ] 2025.4.x - identify latest minor version and confirm i8g support
  - [ ] master - confirm continued i8g support
- [ ] Document that 2024.1, 2024.2, 2025.1, 2025.2 do NOT support i8g/ARM64 and remain on i4i
- [ ] Document findings in version support matrix table
- [ ] Finalize release migration strategy with clear version boundaries

**Dependencies:** Phase 2 (can run in parallel with Phase 4)

**Deliverables:**
- Version support matrix document with ARM64 support clarification
- Release migration strategy clearly documenting which releases stay on i4i (x86_64) vs migrate to i8g (ARM64)

### Phase 4: Recalibrate Throughput Steps

**Description:** Redefine gradual ramp steps as percentages of validated maximum throughput on i8g.

**Definition of Done:**
- [ ] Calculate percentage-based steps from validation data:
  - [ ] ~10% of max throughput
  - [ ] ~50% of max throughput
  - [ ] ~75% of max throughput
  - [ ] ~90% of max throughput
  - [ ] unthrottled (100%)
- [ ] Create new configuration file: `configurations/performance/cassandra_stress_gradual_load_steps_enterprise_i8g.yaml`
- [ ] Define steps for each workload type (read, write, mixed, read_disk_only)
- [ ] Maintain thread count strategy (static or per-step if needed)
- [ ] Document rationale for each step in configuration comments

**Dependencies:** Phase 2

**Deliverables:**
- New throughput steps configuration file
- Documentation of step calculations

### Phase 5: Validate Recalibrated Steps

**Description:** Run validation tests with new percentage-based throughput steps to ensure stability and appropriate metrics scaling.

**Definition of Done:**
- [ ] Update i8g Jenkins jobs to use new throughput steps configuration
- [ ] Execute validation runs for both vnodes and tablets
- [ ] Verify each step completes without saturation or throttling issues
- [ ] Confirm metrics scale linearly or as expected
- [ ] Verify no unexpected cluster behavior (OOM, crashes, timeouts)
- [ ] Document P99 latencies at new step levels
- [ ] Identify if new latency thresholds are needed

**Dependencies:** Phase 4

**Deliverables:**
- Validation test runs with recalibrated steps
- Performance analysis report
- Decision on latency threshold updates

### Phase 6: Update Latency Thresholds for i8g

**Description:** Create new latency threshold configurations based on i8g/ARM64 performance characteristics. Since we're changing both instance type and architecture (x86_64 to ARM64), and adding/changing throughput steps, new thresholds are required.

**Definition of Done:**
- [ ] Analyze P99 latency results from Phase 5 validation
- [ ] Create new latency threshold files for i8g:
  - [ ] Create `latency-decorator-error-thresholds-steps-ent-vnodes-i8g.yaml`
  - [ ] Create `latency-decorator-error-thresholds-steps-ent-tablets-i8g.yaml`
  - [ ] Document threshold values and rationale for each step
  - [ ] Compare with i4i thresholds and document differences
- [ ] Update Jenkins jobs to reference new i8g threshold files
- [ ] Validate that thresholds are achievable but still catch regressions

**Dependencies:** Phase 5

**Deliverables:**
- New latency threshold files for vnodes and tablets on i8g/ARM64
- Documentation of threshold rationale and comparison with i4i baselines
- Updated Jenkins jobs with correct threshold references

### Phase 7: Update Master Trigger and Rollback Plan

**Description:** Modify the master trigger to use new i8g jobs for supported versions with clear rollback capability.

**Definition of Done:**
- [ ] Update `vars/perfRegressionParallelPipelinebyRegion.groovy`:
  - [ ] For ARM64-supporting versions (2025.3+, 2025.4, master): Update job_name to point to new i8g jobs
  - [ ] For x86_64-only versions (2024.1, 2024.2, 2025.1, 2025.2): Keep existing i4i job references unchanged
  - [ ] Add comments documenting ARM64 vs x86_64 architecture routing
  - [ ] Ensure version boundaries are correct (e.g., 2025.3.3+ for 2025.3.x)
- [ ] Document rollback procedure:
  - [ ] Steps to revert to i4i jobs if issues arise
  - [ ] Monitoring metrics to watch post-migration
  - [ ] Decision criteria for rollback
- [ ] Test trigger in dry-run if possible (manual parameter testing)
- [ ] Preserve old i4i jobs (do not delete) for rollback capability

**Dependencies:** Phase 3, Phase 6

**Deliverables:**
- Updated perfRegressionParallelPipelinebyRegion.groovy
- Rollback documentation
- Pre-deployment validation

### Phase 8: Monitor and Validate Production Runs

**Description:** Monitor first production runs after trigger update to validate success and ensure no regressions.

**Definition of Done:**
- [ ] Monitor first weekly triggered run of i8g jobs
- [ ] Verify all jobs complete successfully for all targeted versions
- [ ] Verify P99 latencies meet threshold expectations
- [ ] Verify no infrastructure stability issues
- [ ] Verify Argus reporting works correctly
- [ ] Verify email reports generate properly
- [ ] Document any issues or anomalies
- [ ] If issues found: Execute rollback plan
- [ ] If successful: Document migration completion

**Dependencies:** Phase 7

**Deliverables:**
- Production monitoring report
- Migration success confirmation or rollback execution
- Post-migration documentation

## 5. Testing Requirements

### Phase 1-2: Infrastructure Validation
- **Manual Testing:** Provision test runs to verify i8g infrastructure
- **Success Criteria:** 
  - All nodes provision without errors
  - Network connectivity established correctly
  - Loaders can connect to database nodes
  - Monitoring stack deploys successfully

### Phase 4-5: Performance Validation
- **Functional Testing:** Each throughput step completes without errors
- **Performance Testing:** 
  - P99 latency captured at each step
  - Maximum throughput measured accurately
  - No cluster saturation (CPU, memory, I/O)
- **Success Criteria:**
  - All steps complete within expected duration
  - Latencies scale predictably with load
  - No test failures or infrastructure timeouts

### Phase 7-8: Integration Testing
- **Trigger Testing:** Manual execution of trigger with test parameters
- **Production Testing:** First automated weekly trigger run
- **Success Criteria:**
  - Correct jobs triggered for each version
  - All triggered jobs complete successfully
  - Results reported to Argus
  - Email reports sent correctly

### Regression Testing (All Phases)
- **Validate:** No impact on other performance test jobs
- **Validate:** Existing i4i jobs continue working for older releases
- **Validate:** Test result format compatibility maintained

## 6. Success Criteria

1. **Infrastructure Migration Complete:**
   - i8g jobs exist for vnodes and tablets configurations
   - Jobs run successfully on i8g.4xlarge instances
   - No infrastructure provisioning issues

2. **Performance Baselines Established:**
   - Maximum sustainable throughput documented for each workload
   - Percentage-based throughput steps defined and validated
   - P99 latency thresholds appropriate for i8g performance characteristics

3. **Production Deployment Successful:**
   - Master trigger updated with version-aware job routing
   - Weekly automated runs complete successfully
   - Results properly reported to Argus and email

4. **Backward Compatibility Maintained:**
   - x86_64-only releases (2024.1, 2024.2, 2025.1, 2025.2) continue using i4i jobs
   - No test result format changes breaking downstream consumers
   - Rollback capability preserved and documented

5. **Documentation Complete:**
   - Version support matrix documented
   - Throughput step calculations explained
   - Rollback procedure documented
   - Migration completion confirmed

## 7. Risk Mitigation

### Risk: i8g Instance Availability in Regions
**Impact:** Jobs may fail to provision if i8g.4xlarge unavailable in target regions
**Mitigation:**
- Verify i8g.4xlarge availability in us-east-1 before Phase 1
- Consider multi-region strategy if availability is limited
- Use capacity reservations if needed (already configured in test config)

### Risk: Throughput Significantly Higher Than Expected
**Impact:** May need to adjust step counts or distribution if throughput is 2-3x higher
**Mitigation:**
- Validation phase (Phase 2) provides early warning
- Can adjust step count before production (Phase 4)
- May need to add intermediate steps for better granularity

### Risk: ARM64 Architecture Compatibility Issues
**Impact:** May encounter ARM64-specific issues not present on x86_64
**Mitigation:**
- i8g is ARM64-only; architecture change from x86_64 to ARM64
- Releases 2024.1, 2024.2, 2025.1, 2025.2 confirmed not supporting i8g - remain on i4i
- Only migrate releases with confirmed ARM64 support (2025.3.3+, 2025.4, master)
- Validation phase tests actual ARM64 compatibility
- Existing ARM tests (rolling-upgrade-ami-arm) provide precedent

### Risk: Rollback Needed After Production Deployment
**Impact:** Weekly test runs may be disrupted during rollback
**Mitigation:**
- Keep old i4i jobs intact and functional
- Document clear rollback procedure with verification steps
- Monitor first production run closely
- Have team availability during first trigger execution

### Risk: ARM64 Performance Characteristics Differ from x86_64
**Impact:** Latency patterns may differ significantly between architectures, causing false positives/negatives
**Mitigation:**
- Validation phases (2 & 5) capture ARM64-specific latency data before production
- Phase 6 creates new thresholds based on actual i8g/ARM64 performance
- Since we're changing architecture, new thresholds are required (not optional)
- Can temporarily disable latency decorator if urgent rollback needed

### Risk: AWS Pricing Changes
**Impact:** i8g instances may be more expensive than i4i
**Mitigation:**
- Document cost implications (out of scope for this plan)
- Ensure billing_project parameter correctly configured
- Monitor costs post-migration

### Risk: Percentage-Based Throttling Not Supported
**Impact:** Current implementation may not support percentage-based step calculation
**Mitigation:**
- Phase 4 uses absolute values calculated from percentages
- No code changes needed to test framework
- Percentage approach is for documentation/maintenance clarity only

## 8. Open Questions

1. **What is the exact i8g.4xlarge (ARM64) throughput improvement over i4i.4xlarge (x86_64)?**
   - Answer will be determined in Phase 2 validation
   - Impacts step recalibration strategy
   - Note: Comparing across architectures (ARM64 vs x86_64) not just instance types

2. **What is the exact minimum minor version for 2025.3.x that supports i8g?**
   - Known: At least 2025.3.3
   - Need to verify if earlier 2025.3.x versions support i8g
   - Critical for Phase 7 trigger version boundaries

3. **Should we add additional intermediate steps if throughput is much higher?**
   - Depends on Phase 2 results
   - May need 6-7 steps instead of current 4-5 if 3x+ throughput

4. **Do tablets have different throughput characteristics on i8g/ARM64 vs i4i/x86_64?**
   - Will be measured in Phase 2
   - May need different step definitions for tablets vs vnodes
   - Architecture change may affect tablets differently than vnodes

5. **Is there a staging/test trigger environment for validation?**
   - Impacts Phase 7 testing approach
   - May need production-like manual testing if no staging

6. **Are there ARM64-specific Scylla configuration considerations?**
   - Known: `--no-ec2-check` flag required for i8g (see `configurations/arm_instance_types/i8g_2xlarge.yaml`)
   - Need to verify if this applies to i8g.4xlarge
   - Any other ARM64-specific tuning needed?
