# Plan: Migration to i8g Instance Type for Performance Testing

## 1. Problem Statement

The current performance regression testing infrastructure uses i4i.4xlarge instances for gradual throughput tests. The AWS i8g instance family offers significantly higher throughput capabilities, requiring a comprehensive migration plan to:
- Leverage improved hardware performance of i8g instances
- Recalibrate throughput steps based on new maximum sustainable throughput
- Validate latency expectations remain within acceptable ranges
- Maintain support for releases that don't support i8g instances
- Preserve rollback capabilities to ensure infrastructure reliability

The migration impacts critical weekly performance testing jobs for both vnodes and tablets configurations across multiple Scylla releases (2024.1, 2024.2, 2025.1, 2025.2, 2025.3, 2025.4, and master).

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

- **Tablets job:** `scylla-enterprise-perf-regression-predefined-throughput-steps-tablets.jenkinsfile`
  - Same config base as vnodes but with tablets enabled
  - Latency thresholds: `configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml`
  - Tablets have slightly different latency expectations (e.g., 235,014 vs 284,479 ops/sec write throughput)

**Master Trigger:**
- **Location:** `/jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile`
- Calls: `perfRegressionParallelPipelinebyRegion()` from `vars/perfRegressionParallelPipelinebyRegion.groovy`
- Schedules: Weekly runs (Sunday 6am) with label 'master-weekly'
- Supports versions: 2024.1, 2024.2, 2025.1, 2025.2, 2025.3, 2025.4, master (lines 56-70)

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
  - Mixed 50K-150K: ≤3ms (both r/w), 300K: ≤5ms (both r/w)
  - Write: 284,479 ops/sec baseline
- **Tablets** (`latency-decorator-error-thresholds-steps-ent-tablets.yaml`):
  - Read 450K: ≤3ms (more strict than vnodes), others similar
  - Write: 235,014 ops/sec baseline
  - More relaxed disk-only thresholds (many null limits)

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
- Already used in: longevity tests (i8ge.xlarge, i8ge.2xlarge), rolling-upgrade-ami-arm, vector-search
- ARM config exists: `configurations/arm_instance_types/i8g_2xlarge.yaml`
- Note: i8g.2xlarge supported from Scylla 2025.3.3+ (comment in jenkins-pipelines/oss/rolling-upgrade/rolling-upgrade-ami-arm.jenkinsfile line 13)

**Version Support for i8g:**
- **Needs Investigation:** Which exact minor versions of 2024.1, 2024.2, 2025.1, 2025.2, 2025.3, 2025.4 support i8g instances
- **Known:** master branch supports i8g
- **Known:** 2025.3.3+ explicitly supports i8g (from ARM upgrade jenkinsfile)

## 3. Goals

1. **Create new i8g-based performance jobs** for vnodes and tablets with proper instance type configuration
2. **Validate infrastructure stability** using last week's AMIs before production use
3. **Recalibrate throughput steps** based on i8g maximum sustainable throughput using percentage-based approach
4. **Update P99 latency thresholds** to reflect i8g performance characteristics
5. **Migrate master trigger** to use new jobs while preserving rollback capability
6. **Update release-specific triggers** based on i8g version support matrix
7. **Maintain backward compatibility** for older LTS releases (2024.1 and possibly 2025.1) that don't support i8g

## 4. Implementation Phases

### Phase 1: Create Baseline i8g Jobs (Validation Ready)

**Description:** Create new Jenkins jobs and configuration files for i8g instances with existing throughput steps, ready for validation.

**Definition of Done:**
- [ ] Create `/configurations/aws/i8g_4xlarge.yaml` configuration file
- [ ] Create new Jenkins jobs:
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes-i8g.jenkinsfile`
  - [ ] `scylla-enterprise-perf-regression-predefined-throughput-steps-tablets-i8g.jenkinsfile`
- [ ] Jobs reference i8g configuration instead of i4i
- [ ] Jobs use existing throughput steps from `cassandra_stress_gradual_load_steps_enterprise.yaml`
- [ ] Jobs use existing latency thresholds (vnodes/tablets)
- [ ] Jobs can be triggered manually (not yet in master trigger)
- [ ] Documentation: Add comments explaining these are validation jobs

**Dependencies:** None

**Deliverables:**
- New configuration file for i8g.4xlarge
- Two new Jenkins job files (vnodes and tablets)

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

### Phase 3: Investigate Version Support Matrix

**Description:** Determine which Scylla release versions support i8g instance type to inform trigger migration strategy.

**Definition of Done:**
- [ ] Research i8g support for each target release:
  - [ ] 2024.1.x - latest minor version identification and i8g support status
  - [ ] 2024.2.x - latest minor version identification and i8g support status  
  - [ ] 2025.1.x - latest minor version identification and i8g support status
  - [ ] 2025.2.x - latest minor version identification and i8g support status
  - [ ] 2025.3.x - latest minor version identification and i8g support status (known: 2025.3.3+ supported)
  - [ ] 2025.4.x - latest minor version identification and i8g support status
- [ ] Document findings in version support matrix table
- [ ] Identify which releases need to stay on i4i jobs
- [ ] Identify which releases can migrate to i8g jobs

**Dependencies:** Phase 2 (can run in parallel with Phase 4)

**Deliverables:**
- Version support matrix document
- Release migration strategy

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

### Phase 6: Update Latency Thresholds for i8g (If Needed)

**Description:** If validation in Phase 5 shows significantly different latency characteristics, create new latency threshold configurations.

**Definition of Done:**
- [ ] Analyze P99 latency results from Phase 5 validation
- [ ] If thresholds need adjustment:
  - [ ] Create `latency-decorator-error-thresholds-steps-ent-vnodes-i8g.yaml`
  - [ ] Create `latency-decorator-error-thresholds-steps-ent-tablets-i8g.yaml`
  - [ ] Document threshold adjustments and rationale
- [ ] If existing thresholds are appropriate:
  - [ ] Document that existing thresholds will be reused
  - [ ] Skip file creation
- [ ] Update Jenkins jobs to reference appropriate threshold files

**Dependencies:** Phase 5

**Deliverables:**
- New latency threshold files (if needed) or documentation of threshold reuse
- Updated Jenkins jobs with correct threshold references

### Phase 7: Update Master Trigger and Rollback Plan

**Description:** Modify the master trigger to use new i8g jobs for supported versions with clear rollback capability.

**Definition of Done:**
- [ ] Update `vars/perfRegressionParallelPipelinebyRegion.groovy`:
  - [ ] For supported versions: Update job_name to point to new i8g jobs
  - [ ] For unsupported versions: Keep existing i4i job references
  - [ ] Add comments documenting version-specific job routing
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
   - Older releases continue using i4i jobs
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

### Risk: Version Support Unclear
**Impact:** May migrate versions that don't properly support i8g
**Mitigation:**
- Dedicated investigation phase (Phase 3)
- Conservative approach: keep on i4i if support unclear
- Test runs on each target version during validation

### Risk: Rollback Needed After Production Deployment
**Impact:** Weekly test runs may be disrupted during rollback
**Mitigation:**
- Keep old i4i jobs intact and functional
- Document clear rollback procedure with verification steps
- Monitor first production run closely
- Have team availability during first trigger execution

### Risk: Latency Thresholds Too Strict for i8g
**Impact:** Jobs may fail latency checks despite healthy performance
**Mitigation:**
- Validation phases (2 & 5) capture latency data before production
- Phase 6 allows threshold adjustment if needed
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

1. **What is the exact i8g.4xlarge throughput improvement over i4i.4xlarge?**
   - Answer will be determined in Phase 2 validation
   - Impacts step recalibration strategy

2. **Which specific minor versions of each release support i8g?**
   - Requires investigation in Phase 3
   - Critical for Phase 7 trigger update logic

3. **Should we add additional intermediate steps if throughput is much higher?**
   - Depends on Phase 2 results
   - May need 6-7 steps instead of current 4-5 if 3x+ throughput

4. **Do tablets have different throughput characteristics on i8g vs i4i?**
   - Will be measured in Phase 2
   - May need different step definitions for tablets vs vnodes

5. **Is there a staging/test trigger environment for validation?**
   - Impacts Phase 7 testing approach
   - May need production-like manual testing if no staging

6. **What is the minimum Scylla version that supports i8g?**
   - Known: 2025.3.3+ for ARM
   - Need to verify for x86_64 and earlier versions
