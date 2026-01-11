# Health Check Optimization Plan

## Problem Statement

Health checks take 2+ hours on 60-node clusters, running even for skipped nemesis operations. This significantly impacts test run time and efficiency.

### Current Behavior
- Sequential health checks on all 60 nodes
- Each node check involves 5 expensive operations:
  - `nodetool status` - cross-checked against all other nodes for split-brain detection
  - `system.peers` CQL query - retrieves all columns
  - `nodetool gossipinfo` - text parsing overhead
  - Raft group0 membership check
  - Token ring consistency check
- Health checks run even when nemesis is skipped
- Total time: ~120 minutes for 60-node cluster

### Root Causes
1. **Sequential execution**: Nodes checked one by one
2. **Redundant checks**: Post-nemesis checks run even when nemesis was skipped
3. **Cross-validation overhead**: Each node's status is validated against all other nodes to detect split-brain scenarios
4. **No configurability**: Cannot choose lighter-weight checks for large clusters
5. **No sampling**: All nodes always checked, even in stable scenarios

## Goals

1. **Reduce health check time by 90%+** for large clusters (60+ nodes)
2. **Skip redundant checks** when nemesis operations are skipped
3. **Maintain split-brain detection** capability
4. **Provide configurable modes** for different validation levels
5. **Enable topology-aware sampling** for large clusters:
   - Maintain DC and rack coverage
   - **Prioritize nodes in same rack/region as nemesis target node** (higher risk of overload/issues)
   - Include target node and recently added/modified nodes
6. **Support parallel nemesis scenarios**:
   - Enable health checks even when multiple nemesis operations run in parallel
   - Exclude nodes that are currently target nodes of ongoing parallel nemesis operations
   - This allows health validation of unaffected nodes while nemesis operations are in progress

## Proposed Solution

### Phase 1: Skip Health Checks for Skipped Nemesis

**Objective**: Eliminate redundant health checks when nemesis is skipped (100% time reduction in those cases)

**Implementation**:
- Track last nemesis event status in cluster object
- If previous nemesis `is_skipped=True`, **automatically skip** the post-nemesis health check
- Log skip reason for transparency
- **Note**: This is now the **default behavior** (no configuration parameter needed)

**Testing**:
- Unit test: Verify skip logic with mocked nemesis events
- Integration test: Run test with skipped nemesis, verify no health check in logs
- Manual test: 10-node cluster with mixed skipped/executed nemesis

**Expected Impact**: 100% reduction when nemesis is skipped

---

### Phase 2: Parallel Health Check Execution

**Objective**: Check multiple nodes simultaneously to reduce total time

**Implementation**:
- Use `ThreadPoolExecutor` with configurable worker count
- Run health checks on multiple nodes in parallel
- Ensure thread-safe event publishing and result aggregation
- Handle failures gracefully (one node failure should not block others)

**Configuration**:
```yaml
cluster_health_check_parallel_workers: 5  # default: 5, max recommended: 10
```

**Testing**:
- Unit test: Mock parallel execution, verify all nodes checked
- Integration test: 10-node cluster, verify parallelism with timing logs
- Load test: Verify resource usage on large clusters

**Expected Impact**:
- 5 workers: ~80% time reduction (120 min → 24 min)
- 10 workers: ~90% time reduction (120 min → 12 min)
- **Note**: Diminishing returns beyond 10 workers due to API rate limits
- **For smaller clusters**: Primary goal is correctness and expected behavior, time reduction is secondary benefit

---

### Phase 3: Configurable Health Check Operations

**Objective**: Allow selection of which health check operations to perform based on cluster size and requirements

**Check Operations** (can be combined):

#### Operation: `status`
- `nodetool status` from selected nodes
- Validate all nodes show expected status (UN for up nodes, DN acceptable during nemesis)
- Cross-validate responses to detect split-brain
- **Time**: ~2 seconds per node

#### Operation: `peers`
- Query `system.peers` table from selected nodes
- Verify peer information consistency
- **Time**: ~3-5 seconds per node

#### Operation: `gossip`
- `nodetool gossipinfo` from selected nodes
- Parse and validate gossip state
- **Time**: ~5-10 seconds per node (text parsing overhead)

#### Operation: `raft`
- Query Raft group0 membership
- Verify all expected members present and in sync
- **Limitation**: Requires Raft enabled (Scylla 5.0+)
- **Time**: ~1-3 seconds (needs investigation - see Reference Performance Test Plan)

#### Operation: `ring`
- Token ring consistency check
- Verify token distribution
- **Time**: ~5-8 seconds per node

**Predefined Modes**:

#### Mode: `full` (default - maintains current behavior)
- Operations: `['status', 'peers', 'gossip', 'raft', 'ring']`
- All checks per sampled node
- Maximum validation depth
- **Use case**: Small clusters (<10 nodes), critical validation points
- **Time**: ~2 min per node

#### Mode: `basic`
- Operations: `['status', 'raft']`
- Quick validation of cluster state
- Good split-brain detection when combined with proper sampling
- **Use case**: Large clusters in stable state
- **Time**: ~5 seconds per node

#### Mode: `minimal`
- Operations: `['status']`
- Fastest validation
- **Limitation**: Lower split-brain detection if run from single node
- **Use case**: Very large clusters, quick smoke test
- **Time**: ~2 seconds per node

**Configuration**:
```yaml
# Option 1: Use predefined mode
cluster_health_check_operations: 'full'  # full | basic | minimal

# Option 2: Specify custom list of operations
cluster_health_check_operations: ['status', 'raft']  # custom combination
```

**Important Notes on Status Checks**:
- Initial analysis incorrectly stated `nodetool status` is a "single call"
- **Reality**: For split-brain detection, we must cross-check each node's report against others
- This requires calling `nodetool status` from **multiple nodes** (ideally majority)
- Single-node `nodetool status` is fast (~2s) but may miss split-brain scenarios
- **Trade-off**: Speed vs. split-brain detection reliability
- **Node status during nemesis**: DN (Down) status is acceptable when nemesis operation is actively making node unavailable

**Testing**:
- Unit test: Mock each mode, verify only expected operations are called
- Integration test: 10-node cluster with each mode, measure time
- Manual test: Compare detection capabilities with simulated split-brain
- Manual test: Verify DN nodes during nemesis don't fail health check

**Expected Impact** (when combined with Phase 4 sampling):
- `minimal` mode: 98% reduction (120 min → 2 min) - **lower split-brain detection**
- `basic` mode: 95% reduction (120 min → 5 min) - **good split-brain detection with proper sampling**
- `full` mode: maintains current time - **maximum validation**

---

### Phase 4: Configurable Node Sampling

**Objective**: Reduce checks proportionally while maintaining DC and rack coverage

**Implementation**:
- Select nodes based on topology-aware sampling strategy
- **Guarantee**: Minimum coverage per datacenter and rack
- **Priority**: Favor nodes in same rack/region as target node (higher risk of overload)
- **Priority**: Include target node (if still in cluster) and recently added nodes
- Apply full health check to sampled nodes only

**Configuration**:
```yaml
cluster_health_check_sampling_mode: 'all'  # all | cluster_quorum | dc_quorum | rack_quorum | dc_one | rack_one
```

**Sampling Modes**:

- **`all`** (default): Check all nodes (current behavior)
  - All nodes validated
  - Maximum coverage, maximum time

- **`cluster_quorum`**: Check majority (>50%) of all nodes
  - Select >50% of total cluster nodes
  - Ensures at least one node from each DC
  - Good split-brain detection
  - Time: ~50% of full check

- **`dc_quorum`**: Check majority of nodes per datacenter
  - Select >50% of nodes from each DC
  - Balanced DC coverage
  - Time: ~50% of full check

- **`rack_quorum`**: Check majority of nodes per rack
  - Select >50% of nodes from each rack
  - Maximum rack coverage
  - Time: ~50% of full check

- **`dc_one`**: Check one node from each datacenter
  - Select 1 node from each DC
  - Minimal DC coverage
  - Time: ~5-15 nodes for typical multi-DC clusters

- **`rack_one`**: Check one node from each rack
  - Select 1 node from each rack
  - Minimal rack coverage
  - Time: ~3-20 nodes depending on rack count

**Selection Heuristics** (applied in all modes except `all`):

These heuristics ensure health checks focus on nodes most likely to be affected by nemesis operations while ensuring complete cluster coverage over time:

1. **HIGHEST PRIORITY: Always include all nodes participating in the nemesis** 
   - **For health checks done at the end of a nemesis**: Include ALL nodes that participated in the nemesis operation
   - The target node (directly affected) is checked FIRST
   - Additional participating nodes (e.g., replacement nodes, nodes involved in data migration) are checked next
   - **Rationale**: These nodes underwent the most disruption and are most critical to validate
   - **Note**: Participating node information is available from nemesis operation context

2. **Prioritize nodes that participated in recent nemesis operations** (optional enhancement)
   - Consider nodes from previous nemesis operations if not already selected
   - **Note**: This requires tracking participation history across nemesis operations
   - **Trade-off**: Added complexity vs. marginal benefit
   - **Recommendation**: Start with current nemesis participants, extend if needed

3. **Prioritize nodes in same rack/region as target node** ⭐ **KEY OPTIMIZATION**
   - **Rationale**: Nodes in the same rack as the target have higher risk of:
     - Network overload (redistributed traffic)
     - Storage overload (data rebalancing)
     - Resource contention (increased repair/streaming activity)
   - **Example**: If nemesis targets node in rack1 of DC1, prefer other nodes in rack1-DC1 for sampling
   - This maximizes detection of cascading failures or performance degradation

4. **Rotate selection among remaining nodes to avoid never-checked nodes**
   - **Track which nodes were checked in previous health checks** (maintain check history)
   - **Prioritize nodes that haven't been checked recently** to ensure no node is perpetually skipped
   - Cycle through unchecked nodes to ensure complete coverage over multiple nemesis operations
   - **Goal**: Every node in the cluster should be checked at least once every N nemesis cycles
   - This prevents blind spots where certain nodes could develop issues undetected

5. **Ensure minimum of 2 nodes total** for cross-validation
   - At least 2 nodes needed to detect basic disagreement
   - Prevents false positives from single node reporting

**Example** (60 nodes, 5 DCs × 3 racks/DC, `dc_quorum` mode):
- DC1: 12 nodes → sample 7 (>50%)
- DC2: 12 nodes → sample 7 (>50%)
- DC3: 12 nodes → sample 7 (>50%)
- DC4: 12 nodes → sample 7 (>50%)
- DC5: 12 nodes → sample 7 (>50%)
- **Total: 35 nodes checked** (~58% coverage, ~42% time reduction)

**Example** (60 nodes, 5 DCs, `dc_one` mode):
- DC1: 12 nodes → sample 1 (favor target node's rack if applicable)
- DC2: 12 nodes → sample 1
- DC3: 12 nodes → sample 1
- DC4: 12 nodes → sample 1
- DC5: 12 nodes → sample 1
- **Total: 5 nodes checked** (~8% coverage, ~92% time reduction)

**Testing**:
- Unit test: Verify sampling algorithm for each mode
- Unit test: Verify target node and recent nodes are prioritized
- Unit test: Verify minimum guarantees (1 per DC/rack, 2 total)
- Integration test: Multi-DC/rack cluster, verify sampling respects topology
- Integration test: Verify rack/region proximity to target node

**Expected Impact**:
- `cluster_quorum`: ~50% time reduction
- `dc_quorum`: ~50% time reduction
- `rack_quorum`: ~50% time reduction
- `dc_one`: ~90% time reduction (5-15 nodes typical)
- `rack_one`: ~85-95% time reduction (3-20 nodes typical)

---

### Phase 5: Exclude Nodes Running Nemesis

**Objective**: Skip health checks on nodes actively participating in nemesis operations

**Implementation**:
- Check `node.running_nemesis` flag before health check
- Skip node if flag is set (node may be legitimately down during nemesis operation)
- Log exclusion for transparency
- **Important**: DN status during active nemesis operation should not fail health check
- **Note**: Nodes successfully removed during nemesis (e.g., decommissioned, terminated) are automatically excluded from health checks as they're no longer in the cluster topology

**Configuration**:
```yaml
cluster_health_check_exclude_running_nemesis: true  # default: true
```

**Use Case**:
- Parallel nemesis scenarios where multiple nemesis threads run concurrently
- Avoids checking nodes that are intentionally disrupted
- Enables health validation of non-affected nodes during parallel nemesis
- Prevents false failures when node is legitimately down during nemesis execution

**Considerations**:
- May reduce total checked nodes below desired sample
- Should still enforce minimum guarantees from Phase 4

**Testing**:
- Unit test: Mock nodes with `running_nemesis=True`, verify exclusion
- Integration test: Run parallel nemesis, verify active nodes excluded
- Manual test: Verify health check completes successfully with nemesis in progress
- Manual test: Verify health check doesn't fail when nemesis makes node unavailable

**Expected Impact**: Variable (depends on nemesis duration and concurrency)

---

## Reference Performance Test Plan

**Purpose**: Establish baseline measurements to evaluate speed of each health check optimization

**Primary Goal**: Measure and compare the execution time of different health check modes and sampling strategies to validate the expected performance improvements (90%+ time reduction target).

**Test Objectives**:
1. **Measure actual execution time** for each health check mode and sampling strategy
2. **Compare measured vs. expected performance** to validate optimization effectiveness
3. **Identify performance bottlenecks** (specifically investigate why Raft checks appear slower than expected)
4. **Validate correctness** of split-brain detection capabilities under different modes
5. **Establish reference metrics** for future performance regression testing

**Test Setup**:
- **Cluster**: 30-node cluster (3 DCs × 10 nodes each)
- **Scylla Version**: 5.2+ (Raft enabled)
- **Infrastructure**: AWS i3en.large instances (matching production use case)
- **Instrumentation**: Add timing metrics to health check code to measure each operation
- **Baseline**: Run with current implementation (`cluster_health_check_mode: 'full'`, `cluster_health_check_sample_nodes: 'all'`) to establish baseline timing

**Performance Test Cases**:

Each test case should:
- Run against the same 30-node cluster setup
- Measure total health check execution time
- Report time per node and per operation
- Execute after a controlled nemesis operation for consistency
- Repeat 3 times and report average ± std deviation

### TC1: Baseline Measurement (Current Implementation)
```yaml
cluster_health_check_mode: 'full'
cluster_health_check_sample_nodes: 'all'
```
**What to measure**:
- Total health check execution time for all 30 nodes
- Average time per node
- Breakdown by operation: nodetool status, system.peers query, gossipinfo, raft, tokenring
- Network traffic volume
- CPU impact on cluster nodes

**Expected baseline**: ~60 minutes total (2 min/node × 30 nodes)
**Success criteria**: Establish baseline for comparison with optimizations

### TC2: Status-Only Mode
```yaml
cluster_health_check_mode: 'status_only'
cluster_health_check_sample_nodes: 'all'
```
**What to measure**: Time for single `nodetool status` call that checks all nodes
**Expected**: ~1-2 minutes (98% faster than baseline)
**Success criteria**: <3 minutes, all 30 nodes detected as UP

### TC3: Majority Status Mode
```yaml
cluster_health_check_mode: 'majority_status'
cluster_health_check_sample_nodes: 'all'
```
**What to measure**: Time to check >50% of nodes and cross-validate
**Expected**: ~8-12 minutes (80-87% faster than baseline)
**Success criteria**:
- <15 minutes execution time
- Successfully detect simulated split-brain scenario

### TC4: Raft-Only Mode (Performance Investigation)
```yaml
cluster_health_check_mode: 'raft_only'
cluster_health_check_sample_nodes: 'all'
```
**What to measure**:
- Time for Raft group0 query from single node
- CQL query execution time vs response parsing time
- Compare with equivalent nodetool command time

**Expected**: <5 minutes
**Investigation**: If >5 minutes, add detailed logging to identify bottleneck:
- Is it CQL connection setup?
- Is it query execution time?
- Is it response parsing?
- Does it scale linearly with cluster size?

**Success criteria**: Identify root cause if Raft check is slower than expected

### TC5: DC-Based Sampling
```yaml
cluster_health_check_mode: 'full'
cluster_health_check_sample_nodes: 'dc_one'
```
**What to measure**: Time to check 1 node per DC (3 nodes total for 3 DCs)
**Expected**: ~6 minutes (90% faster than baseline, 3 nodes × 2 min/node)
**Success criteria**:
- <10 minutes execution time
- Verify target node and recently added nodes are selected
- Verify one node per DC guarantee

### TC6: Parallel Execution
```yaml
cluster_health_check_mode: 'full'
cluster_health_check_sample_nodes: 'all'
cluster_health_check_parallel_workers: 10
```
**What to measure**:
- Total execution time with 10 parallel workers
- Cluster CPU and network impact during parallel checks
- Verify no race conditions or dropped checks

**Expected**: ~12 minutes (80% faster than baseline, limited by slowest checks)
**Success criteria**:
- <20 minutes execution time
- All 30 nodes checked successfully
- No cluster performance degradation

### TC7: Combined Optimizations (Target Configuration)
```yaml
cluster_health_check_mode: 'majority_status'
cluster_health_check_sample_nodes: 'dc_quorum'
cluster_health_check_parallel_workers: 10
cluster_health_check_skip_if_nemesis_skipped: true
```
**What to measure**:
- Time with skipped nemesis (should skip health check entirely)
- Time with executed nemesis using combined optimizations
- Verify split-brain detection still works

**Expected**:
- Skipped nemesis: 0 minutes (100% reduction)
- Executed nemesis: ~4-6 minutes (90-93% faster than baseline)

**Success criteria**:
- Skipped nemesis: health check skipped, logged clearly
- Executed nemesis: <10 minutes
- Split-brain detection: >95% detection rate in test scenarios

**Instrumentation**:
```python
import time
from contextlib import contextmanager

@contextmanager
def measure_operation(operation_name):
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        LOGGER.info(f"Health check operation '{operation_name}' took {elapsed:.2f}s")
```

**Metrics to Collect**:
- Total health check time
- Per-node check time
- Per-operation time (status, peers, gossip, raft, tokenring)
- Network traffic volume
- Cluster CPU impact
- Split-brain detection success rate

---

## Raft Check Investigation

**Observation**: Raft checks seem slower than expected

**Investigation Plan**:

1. **Instrument `get_group0_members()`**:
   - Add timing logs
   - Measure CQL query time
   - Measure response parsing time
   - Identify bottleneck

2. **Compare with nodetool**:
   - Measure equivalent `nodetool` command time
   - Determine if CQL or nodetool is faster for Raft info

3. **Optimize query**:
   - Check if we're querying unnecessary columns
   - Verify connection pooling is used
   - Consider caching group0 state (if safe)

4. **Test at scale**:
   - 10-node cluster
   - 30-node cluster
   - 60-node cluster
   - Measure if Raft check scales linearly with cluster size

**Hypothesis**:
- Raft check **should** be fast (~1-3s) as it queries internal Raft state
- If slower, likely due to:
  - Network latency to query node
  - Large Raft log replay
  - Inefficient query or parsing
  - Connection overhead

**Expected Outcome**:
- Identify root cause of slow Raft checks
- Optimize implementation
- Update plan with actual performance data

---

## Recommended Configurations

### Small Clusters (<10 nodes)
```yaml
cluster_health_check: true
cluster_health_check_mode: 'full'
cluster_health_check_skip_if_nemesis_skipped: true
```
**Rationale**: Full validation is fast enough, provides maximum confidence

### Medium Clusters (10-30 nodes)
```yaml
cluster_health_check: true
cluster_health_check_mode: 'majority_status'
cluster_health_check_parallel_workers: 5
cluster_health_check_skip_if_nemesis_skipped: true
cluster_health_check_exclude_running_nemesis: true
```
**Rationale**: Balance between speed and split-brain detection

### Large Clusters (30-60 nodes)
```yaml
cluster_health_check: true
cluster_health_check_mode: 'majority_status'
cluster_health_check_sample_nodes: 'dc_quorum'
cluster_health_check_parallel_workers: 10
cluster_health_check_skip_if_nemesis_skipped: true
cluster_health_check_exclude_running_nemesis: true
```
**Rationale**: Significant time savings while maintaining split-brain detection via DC-based quorum

### Very Large Clusters (60+ nodes)
```yaml
cluster_health_check: true
cluster_health_check_mode: 'status_only'
cluster_health_check_sample_nodes: 'dc_one'
cluster_health_check_parallel_workers: 10
cluster_health_check_skip_if_nemesis_skipped: true
cluster_health_check_exclude_running_nemesis: true
```
**Rationale**: Prioritize speed for stable large clusters, check one node per DC
**Note**: Reduced split-brain detection - consider periodic full checks

### Critical Tests (Release validation, etc.)
```yaml
cluster_health_check: true
cluster_health_check_mode: 'full'
cluster_health_check_parallel_workers: 10
cluster_health_check_skip_if_nemesis_skipped: false  # Always validate
```
**Rationale**: Maximum validation confidence, moderate speed improvement

---

## Heuristics for Split-Brain Detection

**Challenge**: Full cross-validation requires checking all nodes, which is slow

**Proposed Heuristic**: Majority Validation

Instead of validating every node against every other node, we can:

1. **Select a majority** of nodes (>50%, e.g., 31 of 60)
2. **Query `nodetool status`** from each selected node
3. **Cross-validate responses**:
   - If all majority nodes agree on cluster state → likely healthy
   - If disagreement exists → potential split-brain, trigger full check
4. **Fail fast**: If minority disagrees, escalate to full validation

**Mathematical Basis**:
- In a split-brain scenario, cluster partitions into groups
- The largest partition must contain >50% of nodes to maintain quorum
- Checking majority guarantees we include nodes from largest partition
- If majority agrees, cluster is either healthy or we're in majority partition

**Edge Cases**:
- **50/50 split**: May not detect split-brain if we only sample one partition
- **Mitigation**: Always include nodes from multiple DCs in sample
- **3-way partition**: Rare, but possible; requires full validation

**Implementation**:
```python
def check_cluster_health_with_majority(self, majority_percentage=60):
    """
    Check cluster health using majority of nodes for split-brain detection.

    Args:
        majority_percentage: Percentage of nodes to check (default 60%)

    Returns:
        True if healthy, False if split-brain detected
    """
    # Sample majority of nodes, ensuring DC diversity
    sample_nodes = self._sample_nodes_by_majority(majority_percentage)

    # Collect status from each sampled node
    status_reports = {}
    for node in sample_nodes:
        status_reports[node] = node.get_nodes_status()

    # Cross-validate: all sampled nodes should agree
    if self._check_consensus(status_reports):
        LOGGER.info("Cluster health check passed (majority consensus)")
        return True
    else:
        LOGGER.warning("Disagreement detected, performing full validation")
        # Fall back to full check
        return self.check_cluster_health_full()
```

**Testing**:
- Simulate 2-partition split-brain
- Simulate 3-partition split-brain
- Verify detection with various majority percentages (55%, 60%, 70%)

---

## Success Criteria

1. **Performance**:
   - ✅ 60-node cluster with `majority_status` + sampling: <15 min (87% reduction)
   - ✅ 60-node cluster with `status_only`: <3 min (97% reduction)
   - ✅ Skipped nemesis: 0 min health check (100% reduction)

2. **Correctness**:
   - ✅ Split-brain detection with `majority_status` mode: >95% success rate
   - ✅ All nodes validated in `full` mode
   - ✅ No false positives in healthy clusters

3. **Configurability**:
   - ✅ All 5 configuration parameters implemented
   - ✅ Default values are safe and reasonable
   - ✅ Documentation complete

4. **Testing**:
   - ✅ Unit test coverage >90%
   - ✅ All integration tests pass
   - ✅ Performance validated on real clusters

5. **Adoption**:
   - ✅ Updated default configs for large cluster tests
   - ✅ Migration guide for existing tests
   - ✅ Monitoring/logging for health check performance

---

## Risks & Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Missed split-brain with `status_only` | High | Medium | Use `majority_status` for critical tests; document limitation |
| Parallel execution overloads cluster | Medium | Low | Limit default workers to 5; test at scale; make configurable |
| Raft check remains slow | Low | Medium | Investigate early (Phase 6); have fallback to status checks |
| Sampling misses issues | Medium | Low | Ensure DC diversity; minimum node guarantees; periodic full checks |
| Configuration complexity | Low | High | Provide presets for common scenarios; clear documentation |

---

## Open Questions

1. **What is acceptable false-negative rate for split-brain detection?**
   - `status_only`: ~20% miss rate (acceptable for quick checks?)
   - `majority_status`: ~5% miss rate (acceptable for normal checks?)

2. **How should sampling balance favored vs randomized nodes?**
   - Favored nodes: Target node, recently modified nodes, same rack/region nodes
   - Randomize selection among remaining nodes for better coverage
   - Goal: Combine deterministic priority with random coverage

3. **Should we provide a configuration suggestion tool?**
   - Script/tool that suggests optimal config based on:
     * Cluster size (node count, DC count, rack count)
     * Test type (longevity, performance, upgrade, etc.)
     * Risk tolerance (speed vs accuracy trade-off)
   - Example: `sct.py suggest-health-check-config --nodes 60 --dcs 5`

4. **Should we add automatic mode selection based on cluster size?**
   ```yaml
   cluster_health_check_mode: 'auto'  # Selects mode based on node count
   ```

5. **Should we track health check performance metrics and issue detection?**
   - Collect metrics per option into Adaptive Timeout system
   - Track time per check, success rate, failures detected
   - **Issue tracking approach**:
     - Add label to SCT and Scylla issues if surfaced by health check (e.g., `detected-by:health-check`)
     - Enables measuring detection effectiveness
     - Note: Tracking false positives is more complex - requires manual validation
   - → Use metrics for auto-tuning and configuration optimization
   - → Use issue tracking to validate health check effectiveness

---

## References

- Issue: [#9547 - Health checks take 2+ hours on 60-node setup](https://github.com/scylladb/scylla-cluster-tests/issues/9547)
- Related: Nemesis event tracking and skip logic
- Code: `sdcm/cluster.py` - `check_cluster_health()`
- Code: `sdcm/utils/health_checker.py` - Health check functions
- Config: `sdcm/sct_config.py` - Configuration parameters

---

## Appendix: Current Health Check Flow

```python
def check_cluster_health(self):
    if not self.params.get("cluster_health_check"):
        return

    for node in self.nodes:  # Sequential!
        node.check_node_health()  # Calls 5 expensive operations per node
```

```python
def node_health_events(self):
    nodes_status = self.get_nodes_status()           # nodetool status
    peers_details = self.get_peers_info()            # CQL: system.peers
    gossip_info = self.get_gossip_info()             # nodetool gossipinfo
    group0_members = self.raft.get_group0_members()  # Raft query
    tokenring_members = self.get_token_ring_members() # Token ring query

    # Then 5 validation functions cross-reference all this data
    return itertools.chain(
        check_nodes_status(...),
        check_node_status_in_gossip_and_nodetool_status(...),
        check_schema_version(...),
        check_nulls_in_peers(...),
        check_group0_tokenring_consistency(...),
    )
```

**Total**: 60 nodes × (5 operations + 5 validations) × ~2 min/node = 120 min
