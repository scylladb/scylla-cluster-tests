# Latte Gradual Throughput Tests with Flexible Step Configuration

## Problem Statement

The gradual throughput performance tests need to support latte stress tool, which requires both `--threads` (worker threads) and `--concurrency` (concurrent requests per thread) parameters for proper parallelism control. Unlike cassandra-stress which uses only threads, latte requires both parameters to be tuned together for optimal performance.

The current implementation has limitations:
1. Latte parameters (threads, concurrency) must be hardcoded in stress commands
2. Cannot easily vary threads/concurrency across different throughput steps
3. No unified configuration structure for multiple latte parameters per step
4. Difficult to tune for different cluster configurations (vnodes vs tablets, different instance types)

This creates maintenance burden and reduces flexibility for:
- Adding new latte-based gradual throughput tests for vnodes/tablets comparison in v17
- Tuning performance parameters for different hardware (e.g., i8g.4xlarge instances)
- Running the same test with different parallelism settings

## Current State

### Configuration System (`sdcm/sct_config.py`)

**Existing Parameters:**
- **`perf_gradual_threads`** (Lines 1856-1861): Thread count per workload
  - Type: `dict_or_str`
  - Example: `{'read': 100, 'write': [200, 300], 'mixed': 300}`
  - Supports both integer (all steps) and list (per-step) values

- **`perf_gradual_throttle_steps`** (Lines 1863-1867): Rate limiting per workload
  - Type: `dict_or_str`
  - Current format: `{'read': ['100000', '150000'], 'mixed': ['300']}`
  - **Limitation**: Only supports throttle values as strings, not structured step configurations

- **`perf_gradual_step_duration`** (Lines 1869-1874): Duration per workload
  - Type: `dict_or_str`
  - Example: `{'read': '30m', 'write': None, 'mixed': '30m'}`

**Current Validation** (Lines 3476-3510):
- Validates that thread counts are int or list
- Ensures list length matches throttle steps
- Does not support structured dict format for throttle steps

### Gradual Throughput Test Implementation (`performance_regression_gradual_grow_throughput.py`)

- **Lines 24-37**: `Workload` dataclass
  - Contains: `workload_type`, `cs_cmd_tmpl`, `num_threads`, `throttle_steps`, etc.
  - `num_threads` is a simple list, cannot hold multiple parameters per step

- **Lines 314-356**: `run_step()` method
  - **Line 342**: Replaces `$threads` placeholder
  - **Line 343**: Replaces `$throttle` placeholder
  - **No support for additional placeholders** like `$concurrency`

- **Lines 398-412**: `current_throttle()` static method
  - Formats throttle based on stress tool type (cassandra-stress vs latte vs scylla-bench)

### Enterprise Predefined Throughput Tests

**Test Configuration Pattern:**
- Base test: `test-cases/performance/perf-regression-predefined-throughput-steps.yaml`
- Uses cassandra-stress with `$threads` and `$throttle` placeholders
- Configuration: `configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml`

**Jenkins Pipelines (v17/scylla-enterprise/perf-regression):**
- `scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes.jenkinsfile`
  - Backend: AWS, region: us-east-1
  - Configs: base + enterprise steps + tablets_disabled + disable_kms + latency thresholds
  - Sub-tests: test_read_gradual_increase_load, test_mixed_gradual_increase_load, test_read_disk_only_gradual_increase_load

- `scylla-enterprise-perf-regression-predefined-throughput-steps-tablets.jenkinsfile`
  - Same structure but without tablets_disabled

**Current cassandra-stress configuration example:**
```yaml
perf_gradual_threads: {"read": 620, "mixed": 1900}
perf_gradual_throttle_steps: {
  "read": ['150000', '300000', '450000', '600000', 'unthrottled'],
  "mixed": ['50000', '150000', '300000', '450000', 'unthrottled']
}
```

### Latte Test Configurations

**Existing:** `configurations/performance/latte-perf-gradual-steps-custom-d3-w1.yaml`
- Hardcoded: `--threads 7 --concurrency 256` in all stress commands
- Comment: "not used by latte" for perf_gradual_threads
- No way to vary threads/concurrency per step

## Goals

1. **Extend `perf_gradual_throttle_steps` to Support Dict Format**
   - Allow dict values with multiple parameters per step: `{'threads': int, 'concurrency': int, 'rate': str}`
   - Maintain backward compatibility with string-only format for cassandra-stress
   - Support mixed formats (some workloads use dict, others use strings)

2. **Update Gradual Throughput Test Framework**
   - Modify `Workload` dataclass to handle dict-based throttle steps
   - Update `run_step()` to extract and replace multiple placeholders from dict
   - Ensure cassandra-stress tests continue to work with string throttle values

3. **Create Latte Gradual Throughput Test Configurations**
   - Base configuration using dict format for throttle steps
   - Vnodes and tablets variants following enterprise pattern
   - Include suggested parameters for i8g.4xlarge instances

4. **Add V17 Jenkins Pipelines**
   - Create pipelines in `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/`
   - Follow naming pattern: `latte-perf-regression-predefined-throughput-steps-{tablets|vnodes}.jenkinsfile`
   - Match structure of existing cassandra-stress enterprise pipelines

5. **Document Optimal Latte Parameters for i8g.4xlarge**
   - Provide tuning guidance for thread/concurrency combinations
   - Include recommendations for vnodes vs tablets
   - Suggest starting points for finding max throughput

## Proposed Configuration Format

### New Dict-Based Format (for latte and multi-parameter tools)

```yaml
perf_gradual_throttle_steps:
  read:
    - {threads: 10, concurrency: 128, rate: '100000'}
    - {threads: 10, concurrency: 256, rate: '150000'}
    - {threads: 20, concurrency: 256, rate: '180000'}
    - {threads: 20, concurrency: 512, rate: 'unthrottled'}
  mixed:
    - {threads: 100, concurrency: 2}  # rate omitted = unthrottled
    - {threads: 200, concurrency: 2}
```

### Backward Compatible String Format (for cassandra-stress)

```yaml
perf_gradual_throttle_steps:
  read: ['150000', '300000', '450000', 'unthrottled']
  mixed: ['50000', '150000', '300000', 'unthrottled']
```

### Mixed Format (both in same config)

```yaml
perf_gradual_throttle_steps:
  # Latte workload with dict format
  read:
    - {threads: 10, concurrency: 128, rate: '100000'}
    - {threads: 10, concurrency: 256, rate: '150000'}
  # cassandra-stress workload with string format
  write: ['200000', '300000', 'unthrottled']
```

### Stress Command Placeholder Support

For latte commands:
```yaml
stress_cmd_r:
  - >-
    latte run --tag read1 --duration $duration
    --threads $threads --concurrency $concurrency
    --connections 1 --consistency ONE $throttle
    --function t1__get
    scylla-qa-internal/workload.rn
```

Placeholders replaced from dict:
- `$threads` ← `threads` from step dict
- `$concurrency` ← `concurrency` from step dict
- `$throttle` ← formatted from `rate` (or empty if omitted/unthrottled)
   - Create gradual throughput jenkinsfiles for vnodes and tablets
   - Follow existing v17 naming conventions

5. **Documentation and Testing**
   - Update configuration documentation
   - Add unit tests for concurrency parameter handling
   - Provide migration guide for existing tests

## Implementation Phases

### Phase 1: Update Configuration Parameter to Support Dict Format

**Description**: Modify `perf_gradual_throttle_steps` validation to accept dict format with multiple parameters per step

**Definition of Done**:
- [ ] Update validation in `sdcm/sct_config.py` (lines 3476-3510) to handle dict format
- [ ] Support three input formats:
  - String list: `['100000', '150000']` (backward compatible)
  - Dict list: `[{threads: 10, concurrency: 128, rate: '100000'}, ...]`
  - Mixed: Some workloads use strings, others use dicts
- [ ] Validation checks for dict format:
  - Each dict must have at least one key (threads, concurrency, or rate)
  - `threads` and `concurrency` must be positive integers if present
  - `rate` must be string if present
  - Dict length consistency across workload steps
- [ ] Maintain backward compatibility with existing string-based configs
- [ ] No changes to `defaults/test_default.yaml` (parameter remains optional)

**Dependencies**: None

**Deliverables**:
- Modified `sdcm/sct_config.py`
- Validation supports both string and dict formats

### Phase 2: Update Test Framework to Handle Dict Steps

**Description**: Modify gradual throughput test to extract and use parameters from dict-based throttle steps

**Definition of Done**:
- [ ] Update `Workload` dataclass to store step parameters as dicts
  - Convert string steps to dict format internally: `'100000'` → `{rate: '100000'}`
  - Store list of dicts: `[{threads: 10, concurrency: 128, rate: '100000'}, ...]`
- [ ] Modify `run_step()` method:
  - Accept step_params dict instead of separate num_threads/current_throttle
  - Replace all placeholders from dict: `$threads`, `$concurrency`, `$throttle`
  - Default values: threads from perf_gradual_threads if not in dict, throttle from rate
- [ ] Update `run_gradual_increase_load()`:
  - Iterate over throttle_steps extracting params dict per step
  - Pass complete dict to run_step()
- [ ] Update `warmup_cache()` to accept params dict
- [ ] Keep `perf_gradual_threads` as fallback for backward compatibility

**Dependencies**: Phase 1

**Deliverables**:
- Modified `performance_regression_gradual_grow_throughput.py`

### Phase 3: Create Latte Test Configuration (Base)

**Description**: Create base latte configuration file with dict-based throttle steps for predefined throughput test

**Definition of Done**:
- [ ] Create `configurations/performance/latte_gradual_load_steps_enterprise.yaml`:
  - Use dict format for throttle steps with threads, concurrency, rate
  - Base on cassandra_stress_gradual_load_steps_enterprise.yaml structure
  - Include read, mixed, write, read_disk_only workloads
  - Set perf_gradual_step_duration appropriately
- [ ] NO separate perf_gradual_threads needed (values in dicts)
- [ ] Configuration uses standard enterprise test patterns

**Dependencies**: Phase 2

**Deliverables**:
- New file: `configurations/performance/latte_gradual_load_steps_enterprise.yaml`

### Phase 4: Create Base Latte Test Case

**Description**: Create base test case file for latte predefined throughput steps

**Definition of Done**:
- [ ] Create `test-cases/performance/latte-perf-regression-predefined-throughput-steps.yaml`:
  - Define prepare_stress_cmd for latte schema creation
  - Define prepare_write_cmd with latte insert commands
  - Define stress_cmd_r, stress_cmd_w, stress_cmd_m, stress_cmd_read_disk with $placeholders
  - Use placeholders: $threads, $concurrency, $throttle, $duration
  - Base on cassandra-stress perf-regression-predefined-throughput-steps.yaml pattern
  - Set round_robin: true

**Dependencies**: Phase 2

**Deliverables**:
- New file: `test-cases/performance/latte-perf-regression-predefined-throughput-steps.yaml`

### Phase 5: Create V17 Enterprise Jenkins Pipelines

**Description**: Add Jenkins pipeline files for v17 latte gradual throughput tests

**Definition of Done**:
- [ ] Create `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/latte-perf-regression-predefined-throughput-steps-tablets.jenkinsfile`:
  - Use `perfRegressionParallelPipeline` function
  - Backend: "aws", region: "us-east-1", provision_type: 'on_demand'
  - test_name: "performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest"
  - test_config: base test case + latte enterprise steps + disable_kms + latency thresholds
  - sub_tests: ["test_read_gradual_increase_load", "test_mixed_gradual_increase_load", "test_read_disk_only_gradual_increase_load"]
- [ ] Create vnodes variant:
  - Same structure but add tablets_disabled.yaml to config
  - Use vnodes-specific latency thresholds if available
- [ ] Match pattern of existing cassandra-stress enterprise pipelines

**Dependencies**: Phases 3, 4

**Deliverables**:
- New file: `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/latte-perf-regression-predefined-throughput-steps-tablets.jenkinsfile`
- New file: `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/latte-perf-regression-predefined-throughput-steps-vnodes.jenkinsfile`

### Phase 6: Create Latency Threshold Configurations

**Description**: Create latency decorator threshold configurations for latte tests

**Definition of Done**:
- [ ] Create `configurations/performance/latency-decorator-error-thresholds-steps-latte-tablets.yaml`:
  - Define thresholds for P90, P99, throughput per workload type
  - Base on existing latency-decorator-error-thresholds-steps-ent-tablets.yaml
  - Adjust thresholds for latte's different performance characteristics
- [ ] Create vnodes variant with appropriate threshold adjustments

**Dependencies**: Phase 5

**Deliverables**:
- New file: `configurations/performance/latency-decorator-error-thresholds-steps-latte-tablets.yaml`
- New file: `configurations/performance/latency-decorator-error-thresholds-steps-latte-vnodes.yaml`

### Phase 7: Add Unit Tests

**Description**: Add unit tests for dict-based throttle step handling

**Definition of Done**:
- [ ] Add tests in `unit_tests/test_perf_gradual_tests.py` (create if needed):
  - Test dict format validation (valid/invalid dicts)
  - Test string to dict conversion
  - Test placeholder replacement from dicts
  - Test mixed format configs (some strings, some dicts)
  - Test backward compatibility with string-only format
- [ ] Mock necessary dependencies to avoid actual test execution

**Dependencies**: Phase 2

**Deliverables**:
- New file: `unit_tests/test_perf_gradual_tests.py` (or modified if exists)

## Recommended Latte Parameters for i8g.4xlarge

### Instance Specifications
- **Instance Type**: AWS i8g.4xlarge (Graviton3, 16 vCPUs, 128 GiB RAM)
- **Storage**: 7.5 TB NVMe SSD (4 x 1875 GB)
- **Network**: Up to 25 Gbps
- **Typical Cluster**: 3-6 nodes for performance testing

### Finding Max Throughput - Tuning Strategy

**Key Principle**: For latte, total parallelism = `threads × concurrency × num_loaders`

#### Finding Max Throughput First - Unthrottled Exploration

**NEW PRINCIPLE**: Start with unthrottled tests to find maximum achievable throughput before defining gradual steps.

**Rationale**:
- Latte is fundamentally different from cassandra-stress - it uses both threads and concurrency
- cassandra-stress uses only threads (620 for read, 400 for write, 1900 for mixed, 620 for read_disk_only)
- With latte, the same parallelism is achieved through `threads × concurrency`
- Must find optimal combination that doesn't saturate CPU while maximizing throughput

**Suggested Starting Ranges for Unthrottled Tests:**

Based on cassandra-stress enterprise config values, suggested latte equivalents:

**Read Workload** (cassandra-stress: 620 threads):
```yaml
# Test multiple combinations to find optimal, target total parallelism ~620-800
read_unthrottled_exploration:
  - {threads: 20, concurrency: 32, rate: 'unthrottled'}   # 640 total parallelism
  - {threads: 25, concurrency: 32, rate: 'unthrottled'}   # 800 total parallelism
  - {threads: 20, concurrency: 40, rate: 'unthrottled'}   # 800 total parallelism
  - {threads: 30, concurrency: 30, rate: 'unthrottled'}   # 900 total parallelism
```
- **Why lower thread count**: Latte's Rust implementation is more efficient per-thread
- **Why concurrency**: Provides request pipelining without CPU overhead of more threads
- **Target**: Find combo that achieves 500K-1M ops/sec without >90% CPU on loaders

**Write Workload** (cassandra-stress: 400 threads):
```yaml
# Test multiple combinations to find optimal, target total parallelism ~400-500
write_unthrottled_exploration:
  - {threads: 15, concurrency: 28, rate: 'unthrottled'}   # 420 total parallelism
  - {threads: 20, concurrency: 25, rate: 'unthrottled'}   # 500 total parallelism
  - {threads: 20, concurrency: 20, rate: 'unthrottled'}   # 400 total parallelism
  - {threads: 25, concurrency: 20, rate: 'unthrottled'}   # 500 total parallelism
```
- **Why even lower threads**: Writes are CPU-intensive, avoid loader saturation
- **Target**: Find combo that achieves 200K-400K ops/sec with balanced cluster CPU

**Mixed Workload** (cassandra-stress: 1900 threads):
```yaml
# Test multiple combinations to find optimal, target total parallelism ~1000-1500
mixed_unthrottled_exploration:
  - {threads: 30, concurrency: 40, rate: 'unthrottled'}   # 1200 total parallelism
  - {threads: 40, concurrency: 35, rate: 'unthrottled'}   # 1400 total parallelism
  - {threads: 35, concurrency: 40, rate: 'unthrottled'}   # 1400 total parallelism
  - {threads: 50, concurrency: 30, rate: 'unthrottled'}   # 1500 total parallelism
```
- **Why not 1900**: cassandra-stress likely oversaturated, latte more efficient
- **Target**: Find combo that achieves 300K-600K ops/sec sustained

**Read Disk Only** (cassandra-stress: 620 threads):
```yaml
# Test multiple combinations, same as read but expect lower throughput
read_disk_only_unthrottled_exploration:
  - {threads: 20, concurrency: 32, rate: 'unthrottled'}   # 640 total parallelism
  - {threads: 25, concurrency: 32, rate: 'unthrottled'}   # 800 total parallelism
  - {threads: 30, concurrency: 25, rate: 'unthrottled'}   # 750 total parallelism
```
- **Target**: Find combo that achieves 150K-300K ops/sec (disk-bound)

**Execution Strategy**:
1. Run each combination for 10-15 minutes
2. Monitor: throughput, loader CPU (should be <90%), cluster CPU balance, latency P99
3. Identify optimal threads/concurrency combination that maximizes throughput
4. Use that combination as the final "unthrottled" step in gradual test
5. Define gradual steps working backwards from optimal unthrottled values

#### Step 1: Define Gradual Steps from Unthrottled Maximum

After finding optimal unthrottled combination through exploration, create gradual steps working backwards.

**Example**: If unthrottled exploration found optimal read config is `{threads: 25, concurrency: 32}` achieving 800K ops/sec:

```yaml
perf_gradual_throttle_steps:
  read:
    - {threads: 15, concurrency: 32, rate: '200000'}   # 25% of max
    - {threads: 20, concurrency: 32, rate: '400000'}   # 50% of max
    - {threads: 22, concurrency: 32, rate: '600000'}   # 75% of max
    - {threads: 25, concurrency: 32, rate: 'unthrottled'}  # 100% - optimal found
```

**Rationale**:
- Keep concurrency constant (already optimized in unthrottled exploration)
- Gradually increase threads and rate to approach max
- Each step validates system can handle incrementally higher load
- Final unthrottled step uses optimal combination from exploration

#### Step 2: Monitor and Tune

Watch for:
1. **CPU Saturation** (>90% on loaders): Reduce concurrency or threads
2. **Network Saturation** (>20 Gbps): Capacity reached
3. **Latency Spikes** (P99 >10ms for read, >20ms for write): Reduce concurrency
4. **Throughput Plateau**: Confirmed max throughput from exploration

#### Step 3: Example Gradual Configurations by Workload

Based on unthrottled exploration results, example gradual step configurations:

**Read-Heavy Workloads:**
Assuming exploration found optimal: `{threads: 25, concurrency: 32}` at ~800K ops/sec
```yaml
read:
  - {threads: 15, concurrency: 32, rate: '200000'}   # Warmup
  - {threads: 20, concurrency: 32, rate: '400000'}   # 50% of max
  - {threads: 22, concurrency: 32, rate: '600000'}   # 75% of max
  - {threads: 25, concurrency: 32, rate: 'unthrottled'}  # Optimal from exploration
```
- Target: 500K-1M ops/sec total throughput (4 loaders)

**Write-Heavy Workloads:**
Assuming exploration found optimal: `{threads: 20, concurrency: 25}` at ~400K ops/sec
```yaml
write:
  - {threads: 12, concurrency: 25, rate: '120000'}   # Warmup
  - {threads: 16, concurrency: 25, rate: '200000'}   # 50% of max
  - {threads: 18, concurrency: 25, rate: '300000'}   # 75% of max
  - {threads: 20, concurrency: 25, rate: 'unthrottled'}  # Optimal from exploration
```
- Target: 200K-400K ops/sec total throughput

**Mixed Workloads (50/50 read/write):**
Assuming exploration found optimal: `{threads: 35, concurrency: 40}` at ~600K ops/sec
```yaml
mixed:
  - {threads: 20, concurrency: 40, rate: '200000'}   # Warmup
  - {threads: 28, concurrency: 40, rate: '350000'}   # 60% of max
  - {threads: 32, concurrency: 40, rate: '500000'}   # 85% of max
  - {threads: 35, concurrency: 40, rate: 'unthrottled'}  # Optimal from exploration
```
- Target: 300K-600K ops/sec total throughput

**Read Disk Only:**
Assuming exploration found optimal: `{threads: 25, concurrency: 30}` at ~250K ops/sec
```yaml
read_disk_only:
  - {threads: 15, concurrency: 30, rate: '80000'}    # Warmup
  - {threads: 20, concurrency: 30, rate: '150000'}   # 60% of max
  - {threads: 23, concurrency: 30, rate: '220000'}   # 88% of max
  - {threads: 25, concurrency: 30, rate: 'unthrottled'}  # Optimal from exploration
```
- Target: 150K-300K ops/sec (disk-bound workload)
Assuming exploration found optimal: `{threads: 35, concurrency: 40}` at ~600K ops/sec
```yaml
mixed:
  - {threads: 12, concurrency: 192, rate: '120000'}
  - {threads: 18, concurrency: 192, rate: '240000'}
  - {threads: 18, concurrency: 320, rate: '360000'}
  - {threads: 22, concurrency: 320, rate: 'unthrottled'}
```
- Balanced between read and write characteristics
- Moderate concurrency and thread count
- Target: 300K-600K ops/sec total throughput

### Tablets vs Vnodes Tuning

**Tablets** (generally higher performance):
- Can handle 10-20% higher throughput
- Better load distribution across nodes
- Recommended: Use higher rate steps, same concurrency/threads

**Vnodes** (more conservative):
- May show hotspots under extreme load
- Keep rate steps 10-15% lower than tablets
- Monitor individual node CPU more carefully

### Example Production Configuration

For 4 x i8g.4xlarge loaders, 6-node cluster (after unthrottled exploration):

```yaml
n_loaders: 4
n_db_nodes: 6

# These values derived from unthrottled exploration phase
perf_gradual_throttle_steps:
  read:
    # Exploration found optimal: threads=25, concurrency=32, ~800K ops/sec
    - {threads: 15, concurrency: 32, rate: '200000'}   # 200K total
    - {threads: 20, concurrency: 32, rate: '400000'}   # 400K total
    - {threads: 22, concurrency: 32, rate: '600000'}   # 600K total
    - {threads: 25, concurrency: 32, rate: 'unthrottled'}  # Max from exploration

  mixed:
    # Exploration found optimal: threads=35, concurrency=40, ~600K ops/sec
    - {threads: 20, concurrency: 40, rate: '200000'}   # 200K total
    - {threads: 28, concurrency: 40, rate: '350000'}   # 350K total
    - {threads: 32, concurrency: 40, rate: '500000'}   # 500K total
    - {threads: 35, concurrency: 40, rate: 'unthrottled'}  # Max from exploration

  write:
    # Exploration found optimal: threads=20, concurrency=25, ~400K ops/sec
    - {threads: 12, concurrency: 25, rate: '120000'}   # 120K total
    - {threads: 16, concurrency: 25, rate: '200000'}   # 200K total
    - {threads: 18, concurrency: 25, rate: '300000'}   # 300K total
    - {threads: 20, concurrency: 25, rate: 'unthrottled'}  # Max from exploration

  read_disk_only:
    # Exploration found optimal: threads=25, concurrency=30, ~250K ops/sec
    - {threads: 15, concurrency: 30, rate: '80000'}    # 80K total
    - {threads: 20, concurrency: 30, rate: '150000'}   # 150K total
    - {threads: 23, concurrency: 30, rate: '220000'}   # 220K total
    - {threads: 25, concurrency: 30, rate: 'unthrottled'}  # Max from exploration

perf_gradual_step_duration:
  read: '30m'
  mixed: '30m'
  write: None  # Run until completion
  read_disk_only: '30m'
```
    - {threads: 25, concurrency: 512, rate: 'unthrottled'}

  mixed:
    - {threads: 15, concurrency: 192, rate: '120000'}  # 480K total
    - {threads: 18, concurrency: 256, rate: '200000'}  # 800K total
    - {threads: 20, concurrency: 320, rate: '280000'}  # 1.12M total
    - {threads: 22, concurrency: 384, rate: 'unthrottled'}

  write:
    - {threads: 12, concurrency: 128, rate: '80000'}   # 320K total
    - {threads: 15, concurrency: 192, rate: '120000'}  # 480K total
    - {threads: 18, concurrency: 256, rate: '180000'}  # 720K total
    - {threads: 20, concurrency: 320, rate: 'unthrottled'}

perf_gradual_step_duration:
  read: '30m'
  mixed: '30m'
  write: None  # Run until completion
  read_disk_only: '30m'
```

### Validation Checklist

After running with recommended parameters:
- [ ] P99 latency stays below 10ms for read
- [ ] P99 latency stays below 20ms for write
- [ ] P99 latency stays below 15ms for mixed
- [ ] No loader CPU exceeds 95%
- [ ] Cluster CPU balanced across nodes (±10%)
- [ ] No network drops or retransmits
- [ ] Throughput plateaus at unthrottled step

## Testing Requirements

### Unit Tests
- Dict format validation (Phase 1)
- String to dict conversion (Phase 1)
- Placeholder replacement from dicts (Phase 2)
- Mixed format configs (Phase 2)
- Backward compatibility (Phase 2)

### Integration Tests
- Run gradual throughput test with AWS backend
- Verify dict parameters correctly applied to latte commands
- Test both tablets and vnodes configurations
- Verify test executes without errors for all workload types

### Manual Testing
- **AWS Test** (Production-like):
  ```bash
  # Trigger Jenkins pipeline
  # jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/
  #   latte-perf-regression-predefined-throughput-steps-tablets.jenkinsfile

  # Monitor logs for correct placeholder substitution:
  # - Check threads value in latte command
  # - Check concurrency value in latte command
  # - Check rate formatting (--rate=N or empty for unthrottled)

  # Verify Argus results and latency thresholds
  ```

### Configuration Validation
- Verify all YAML files parse correctly
- Check placeholder substitution for all workload types
- Test with both string and dict throttle step formats

## Success Criteria

1. **Dict Format Support**
   - `perf_gradual_throttle_steps` accepts dict format
   - Validation prevents invalid dict configurations
   - Backward compatibility maintained for string format

2. **Framework Support**
   - Latte commands correctly use threads/concurrency from dicts
   - Cassandra-stress tests still work with string format
   - Gradual throughput tests work with both vnodes and tablets

3. **Test Coverage**
   - New configurations in v17/scylla-enterprise/perf-regression folder
   - Jenkins pipelines match enterprise pattern
   - Unit tests cover dict format validation and usage

4. **Documentation**
   - Implementation plan complete with i8g.4xlarge recommendations
   - Configuration format clearly documented
   - Migration path clear for existing tests

5. **Performance**
   - Tests successfully find max throughput
   - Latency thresholds appropriate for latte
   - No regression in test execution time

## Risk Mitigation

### Risk: Breaking Existing Tests
**Impact**: High
**Mitigation**:
- Maintain backward compatibility with string format
- All existing cassandra-stress tests continue to work unchanged
- Dict format is additive, not replacing string format
- Extensive testing with existing configs before merging

### Risk: Complex Dict Validation
**Impact**: Medium
**Mitigation**:
- Clear validation error messages
- Document dict format with examples
- Start with simple use cases in unit tests
- Gradual rollout to more complex scenarios

### Risk: Jenkins Pipeline Issues
**Impact**: Medium
**Mitigation**:
- Follow exact pattern from cassandra-stress enterprise pipelines
- Create in same folder structure (scylla-enterprise not scylla-master)
- Test jenkinsfile syntax before committing
- Disable email initially during validation

### Risk: Incorrect Performance Tuning
**Impact**: Medium
**Mitigation**:
- Document conservative starting parameters
- Provide tuning methodology, not just values
- Include validation checklist for results
- Start with lower throughput steps, increase gradually

### Risk: Dict/String Format Confusion
**Impact**: Low
**Mitigation**:
- Clear examples in plan and code comments
- Validation catches format mismatches early
- Error messages indicate expected format
- Unit tests cover all format combinations

## Open Questions

1. **Latency Thresholds**: What are appropriate P90/P99 thresholds for latte on i8g.4xlarge?
   - **Recommendation**: Start with cassandra-stress thresholds, adjust after initial runs
   - Expect slightly better latency from latte due to Rust implementation

2. **Test Duration**: Should latte tests use same 30m step duration as cassandra-stress?
   - **Recommendation**: Yes, keep consistent for comparison purposes
   - May reduce later if latte proves more stable

3. **Workload Coverage**: Should we include write_during_read tests like cassandra-stress?
   - **Recommendation**: Start with standard read/write/mixed, add later if needed
- Use unique placeholder names (`$concurrency` vs `$threads`)
- Test replacement order to avoid conflicts
- Add unit tests for edge cases (multiple placeholders in one command)

### Risk: Documentation Drift
**Impact**: Low
**Mitigation**:
- Pre-commit hooks auto-generate configuration documentation
- Verify docs/configuration_options.md is updated
- Include examples in this plan and commit messages

## Open Questions

1. **Concurrency Values**: What are the optimal concurrency values for different cluster sizes?
   - Recommendation: Start with values from existing latte tests (128-256)
   - Monitor performance and adjust in follow-up PRs

2. **Test Duration**: Should gradual throughput tests have different durations than steady-state tests?
   - Current: 30m per step in existing configs
   - Recommendation: Keep same duration initially, tune later based on results

3. **Sub-test Selection**: Should all three sub-tests (read, write, mixed) run in v17 pipelines?
   - Recommendation: Start with all three, disable underperformers later if needed
