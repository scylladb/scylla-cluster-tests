---
status: draft
domain: framework
created: 2026-04-06
last_updated: 2026-04-06
owner: fruch
---

# Performance Testing Strategy Implementation Plan

## 1. Problem Statement

SCT's performance regression testing relies on hardcoded `fixed_limit` thresholds in YAML files (`configurations/performance/latency-decorator-error-thresholds-*.yaml`) to detect regressions. This approach has several measurable limitations:

- **No drift detection**: A P99 latency creeping from 2ms to 5ms goes unnoticed when the threshold is 10ms. Gradual regressions accumulate across releases until they cross the fixed limit.
- **Manual threshold recalibration**: Every hardware migration (e.g., i4i to i8g) requires manually adjusting dozens of threshold values across multiple YAML files. The ongoing i8g migration (`docs/plans/i8g-performance-jobs-migration.md`) highlights this cost.
- **No root-cause attribution**: When a regression is detected, only latency and throughput are recorded. There is no system-level data (CPU utilization, disk IOPS, compaction pressure, memory) to diagnose whether the regression is compute-bound, I/O-bound, or memory-bound.
- **Coarse nemesis impact measurement**: Nemesis-latency tests (`test_latency_*_with_nemesis` in `performance_regression_test.py`) report aggregate latency without per-phase breakdown (baseline, disruption, recovery), making it impossible to distinguish slow recovery from high-impact disruption.
- **No version-over-version trend**: Upgrade tests (`LatteStressLatencyComparison` in `sdcm/argus_results.py`) run standalone without historical comparison across releases.

## 2. Current State

### Performance Test Infrastructure

- **Predefined steps test**: `PerformanceRegressionPredefinedStepsTest` in `performance_regression_gradual_grow_throughput.py` reads `perf_gradual_throttle_steps` from YAML configs and runs load at each step.
- **Step configurations**: `configurations/performance/cassandra_stress_gradual_load_steps*.yaml` and `configurations/performance/latte-perf-gradual-steps-*.yaml` define per-step throughput targets.
- **Threshold configs**: `configurations/performance/latency-decorator-error-thresholds-*.yaml` define `fixed_limit` values for latency validation.
- **Latency collection**: `sdcm/utils/latency.py` collects Prometheus-based latency metrics; `sdcm/utils/hdrhistogram.py` parses HDR histogram files for percentile data.
- **Decorator-based validation**: `sdcm/utils/decorators.py` wraps test methods with latency validation, Grafana screenshots, and reactor stall counting.
- **Max throughput discovery**: `MaximumPerformanceSearchTest` in `performance_search_max_throughput_test.py` uses binary search to find peak throughput.
- **Nemesis latency tests**: `test_latency_read_with_nemesis`, `test_latency_write_with_nemesis`, `test_latency_mixed_with_nemesis` in `performance_regression_test.py` (lines 680-699).
- **Upgrade comparison**: `LatteStressLatencyComparison` in `sdcm/argus_results.py` (line 254) reports upgrade test results to Argus.
- **Pipeline scheduling**: `vars/perfRegressionParallelPipelinebyRegion.groovy` manages weekly, 3-weekly, and monthly perf test scheduling.
- **Argus integration**: `sdcm/argus_results.py` defines result table schemas for Argus reporting.

### What Needs to Change

1. Regression detection needs statistical baselines alongside (not replacing) fixed limits.
2. System-level metrics (CPU, disk, memory, compaction) must be collected at each performance step.
3. Nemesis performance tests need structured phase measurement (baseline/disruption/recovery).
4. Upgrade results need version-over-version trend tracking in Argus.
5. Efficiency scoring needs implementation for cross-hardware and cross-cloud comparison.

## 3. Goals

1. **Statistical regression detection**: Detect latency regressions exceeding `mean + 3 * stdev` over a 10-run sliding window, partitioned by (test_name, step_name, instance_type, architecture).
2. **System metrics at each step**: Collect CPU utilization, memory, disk IOPS, compaction throughput, and cache hit rate from Prometheus at each performance step and store in Argus.
3. **Nemesis performance framework**: Measure P99 latency in three phases (baseline, disruption, recovery) for at least 4 operation types, with per-operation impact factor reporting.
4. **Upgrade trend tracking**: Store source_version and target_version metadata in Argus results, enabling version-over-version trend queries.
5. **Trend alerting**: Alert when 5 consecutive runs each show >1% degradation, or linear regression slope exceeds 0.5%/run over a 10-run window.
6. **i8g migration simplification**: Hardware-partitioned baselines auto-establish new normals for i8g, eliminating manual threshold edits for Phases 4-6 of the i8g migration plan.
7. **Efficiency scoring**: Compute Kleinrock's Power metric (throughput/latency) and cost-normalized value score for cross-hardware comparison.

## 4. Implementation Phases

### Phase 1: Statistical Baselines (3-4 PRs)

**Importance**: High -- foundation for all other phases

**Dependencies**: None

**Description**: Implement a baseline service that stores historical performance results and computes statistical thresholds. This replaces the sole reliance on `fixed_limit` with a hybrid approach: statistical detection for drift + fixed limits for SLA backstops.

**Deliverables**:
- PR 1.1: Baseline data model and storage layer (Argus or local JSON) with partition key (test_name, step_name, instance_type, architecture)
- PR 1.2: Sliding window computation (mean, stdev, N=10) with configurable k-factor
- PR 1.3: Integration into `sdcm/utils/decorators.py` validation path -- run statistical check alongside existing `fixed_limit` check
- PR 1.4: Configuration parameters in `sdcm/sct_config.py` for window size, k-factor, and enable/disable toggle

**Definition of Done**:
- [ ] Baseline data persists across runs in Argus or local storage
- [ ] Statistical regression is flagged when `observed > mean + k * stdev`
- [ ] Existing `fixed_limit` checks remain functional and unchanged
- [ ] Unit tests cover baseline computation, sliding window, and edge cases (< N runs)
- [ ] Integration test validates end-to-end flow with a Docker-based perf run

### Phase 2: System Metrics Collection (2-3 PRs, parallel with Phase 1)

**Importance**: High -- enables root-cause attribution

**Dependencies**: None (can proceed in parallel with Phase 1)

**Description**: Extend the performance decorator to query Prometheus for system-level metrics (CPU, memory, disk IOPS, compaction, cache) at the start and end of each step, and store snapshots alongside latency results.

**Deliverables**:
- PR 2.1: Prometheus query helper in `sdcm/utils/latency.py` or new `sdcm/utils/perf_metrics.py` for system metric collection
- PR 2.2: Integration into step decorator in `sdcm/utils/decorators.py` to collect metrics per step
- PR 2.3: Argus result table extension to store system metrics alongside latency/throughput

**Definition of Done**:
- [ ] CPU utilization, memory, disk IOPS (read/write), compaction throughput, and cache hit rate collected at each step
- [ ] Metrics stored in Argus alongside existing latency/throughput data
- [ ] Unit tests for Prometheus query construction and response parsing
- [ ] No regression in existing performance test execution time (< 5% overhead)

### Phase 3: Nemesis Performance Framework (3-4 PRs)

**Importance**: Medium -- new capability

**Dependencies**: Phase 2 (system metrics collection)

**Description**: Build a structured nemesis performance measurement framework that segments each nemesis test into baseline, disruption, and recovery phases with separate latency tracking.

**Deliverables**:
- PR 3.1: Phase-segmented latency collector (baseline/disruption/recovery time windows)
- PR 3.2: Impact factor computation (P99_during / P99_baseline) and recovery time measurement
- PR 3.3: Nemesis SLA matrix configuration and validation (per-operation thresholds)
- PR 3.4: At least 4 operation types implemented: rolling restart, decommission, node replace, major compaction

**Definition of Done**:
- [ ] Each nemesis perf test reports: baseline_P99, disruption_P99, recovery_P99, impact_factor, recovery_time
- [ ] Impact factor validated against configurable per-operation SLA thresholds
- [ ] Results stored in Argus with operation type metadata
- [ ] Unit tests for phase segmentation and impact factor computation

### Phase 4: Upgrade Performance Tracking (2 PRs)

**Importance**: Medium -- enhances existing capability

**Dependencies**: Phase 1 (statistical baselines)

**Description**: Extend upgrade test result reporting to include version metadata and enable version-over-version trend queries.

**Deliverables**:
- PR 4.1: Add source_version and target_version fields to `LatteStressLatencyComparison` and related Argus tables in `sdcm/argus_results.py`
- PR 4.2: Degradation percentage computation and integration with statistical baseline comparison

**Definition of Done**:
- [ ] Upgrade test results in Argus include source and target version metadata
- [ ] Degradation percentage computed automatically between versions
- [ ] Statistical baseline comparison works for upgrade results
- [ ] Unit tests for version metadata handling and degradation computation

### Phase 5: i8g Migration (Ongoing)

**Importance**: High -- active migration

**Dependencies**: Phase 1 simplifies Phases 4-6 of the existing i8g plan

**Description**: Continue the i8g migration per `docs/plans/i8g-performance-jobs-migration.md`. Phase 1 of this plan (statistical baselines) eliminates the need for manual threshold recalibration -- new baselines auto-establish after N validation runs on i8g hardware.

**Deliverables**:
- Integration of statistical baselines into i8g validation runs
- Hardware-partitioned baselines with `architecture=arm64` partition key

**Definition of Done**:
- [ ] i8g validation runs populate statistical baselines automatically
- [ ] No manual `fixed_limit` edits required for i8g threshold calibration
- [ ] Existing i4i baselines remain unaffected (partitioned by instance_type)

### Phase 6: Customer Workload Framework (1-2 PRs)

**Importance**: Low -- independent capability

**Dependencies**: None

**Description**: Create template configurations and documentation for reproducing customer-reported performance issues as SCT test cases.

**Deliverables**:
- PR 6.1: Template YAML configs in `test-cases/performance/` for common customer patterns (read-heavy, write-heavy, mixed, large-partition)
- PR 6.2: Documentation for environment mapping (customer attributes to SCT parameters)

**Definition of Done**:
- [ ] At least 4 template YAML configs for common workload patterns
- [ ] Documentation maps customer environment attributes to SCT parameters
- [ ] Templates validated with Docker backend

### Phase 7: Trend Detection & Efficiency Scoring (2-3 PRs)

**Importance**: Medium -- depends on baseline data accumulation

**Dependencies**: Phase 1 (statistical baselines)

**Description**: Implement trend detection alerts and Kleinrock's Power-based efficiency scoring for cross-hardware comparison.

**Deliverables**:
- PR 7.1: Trend detection: M consecutive degrading runs or monotonic slope alerts
- PR 7.2: Efficiency score computation (throughput/latency per step, cost-normalized value score)
- PR 7.3: Comparison report output for cross-hardware and cross-cloud results

**Definition of Done**:
- [ ] Alert generated when 5 consecutive runs each degrade >1%
- [ ] Alert generated when linear regression slope exceeds 0.5%/run over 10 runs
- [ ] Efficiency score and value score computed and stored in Argus
- [ ] Unit tests for trend detection and scoring algorithms
- [ ] Comparison report produces human-readable output matrix

## 5. Testing Requirements

### Unit Tests

| Phase | Test Focus | Location |
|-------|-----------|----------|
| 1 | Baseline computation, sliding window, k-factor thresholds, edge cases (N < window_size) | `unit_tests/test_perf_baselines.py` |
| 2 | Prometheus query construction, metric parsing, snapshot storage | `unit_tests/test_perf_metrics.py` |
| 3 | Phase segmentation, impact factor computation, SLA validation | `unit_tests/test_nemesis_perf.py` |
| 4 | Version metadata handling, degradation percentage | `unit_tests/test_upgrade_tracking.py` |
| 7 | Trend detection algorithms, efficiency scoring, value score | `unit_tests/test_trend_detection.py` |

### Integration Tests

- Phase 1: End-to-end baseline flow with Docker backend perf run
- Phase 2: Prometheus metric collection during Docker perf run
- Phase 3: Nemesis phase measurement with Docker backend

### Manual Testing

- Phase 1: Validate statistical regression detection against known regression (inject artificial latency increase)
- Phase 5: Run i8g staircase test and verify baselines populate correctly
- Phase 7: Verify comparison report output with real multi-hardware data

## 6. Success Criteria

- Statistical baselines detect a 15% latency regression within 3 runs (not waiting for fixed_limit breach)
- System metrics are available in Argus for every performance step, enabling root-cause triage without re-running tests
- Nemesis performance tests report per-phase metrics (baseline, disruption, recovery) for at least 4 operation types
- i8g threshold calibration requires zero manual YAML edits (auto-established by statistical baselines)
- Trend detection fires an alert on synthetic 5-run degradation sequence in unit tests
- Efficiency scoring produces valid comparison matrix for at least 2 hardware configurations

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Insufficient historical data for statistical baselines (< N runs) | High (initial) | Medium | Fall back to `fixed_limit` when baseline window has fewer than N runs; gradually transition as data accumulates |
| Statistical false positives from noisy environments | Medium | High | Use k=3 (3-sigma) for conservative detection; add Wilcoxon rank-sum as non-parametric alternative; allow per-test k-factor override |
| Prometheus query overhead slows test execution | Low | Medium | Batch queries per step (single request with multiple metrics); measure overhead and enforce < 5% budget |
| Argus schema changes break existing result consumers | Medium | High | Add new fields as optional columns; maintain backward compatibility; version the result table schema |
| Nemesis phase boundaries imprecise (disruption start/end timing) | Medium | Medium | Use SCT event timestamps for precise phase boundaries; add configurable tolerance window |
| i8g baseline establishment takes too long (10 runs at weekly cadence = 10 weeks) | High | Medium | Run initial baseline establishment at higher frequency (daily for 2 weeks); accept wider confidence interval with N=5 initially |
| Customer workload templates diverge from actual customer environments | Medium | Low | Templates are starting points; document explicitly that customization is expected; maintain in qa-internal for sensitive details |
