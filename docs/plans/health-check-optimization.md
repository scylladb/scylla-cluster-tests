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
5. **Enable sampling** for large clusters while maintaining DC coverage

## Proposed Solution

### Phase 1: Skip Health Checks for Skipped Nemesis

**Objective**: Eliminate redundant health checks when nemesis is skipped (100% time reduction in those cases)

**Implementation**:
- Track last nemesis event status in cluster object
- If previous nemesis `is_skipped=True`, skip the post-nemesis health check
- Log skip reason for transparency

**Configuration**:
```yaml
health_check_skip_after_skipped_nemesis: true  # default: true
```

**Testing**:
- Unit test: Verify skip logic with mocked nemesis events
- Integration test: Run test with skipped nemesis, verify no health check in logs
- Manual test: 10-node cluster with mixed skipped/executed nemesis

**Expected Impact**: 100% reduction when nemesis is skipped

---

### Phase 2: Configurable Health Check Modes

**Objective**: Allow selection of validation depth based on cluster size and requirements

**Health Check Modes**:

#### Mode 1: `full` (current behavior)
- All 5 checks per node
- Cross-validation for split-brain detection
- Schema version consistency
- Raft and token ring validation
- **Use case**: Small clusters (<10 nodes), critical validation points
- **Time**: ~2 min per node (120 min for 60 nodes)

#### Mode 2: `status_only`
- Single `nodetool status` call from **one verification node**
- Validate all nodes show "UN" status
- **Limitation**: No cross-validation, relies on single node's view
- **Split-brain risk**: May miss split-brain if verification node is in minority partition
- **Mitigation**: Run from majority of nodes (configurable sample)
- **Use case**: Large clusters in stable state, quick smoke test
- **Time**: ~2 minutes for any cluster size

#### Mode 3: `majority_status`
- Run `nodetool status` from majority of nodes (>50%)
- Cross-validate responses to detect split-brain
- Skip peers query, gossipinfo, raft checks
- **Split-brain detection**: Yes, requires consensus among majority
- **Use case**: Balanced approach for large clusters
- **Time**: ~10-15 minutes for 60 nodes (30 nodes × 30 seconds each)

#### Mode 4: `raft_only`
- Query Raft group0 membership from one node
- Verify all expected members present and in sync
- **Limitation**: Requires Raft enabled (Scylla 5.0+)
- **Split-brain detection**: Limited to Raft topology
- **Use case**: Modern clusters with Raft enabled
- **Time**: ~2-5 minutes (needs investigation - see Integration Test Plan)

#### Mode 5: `sampled`
- Full checks on sampled percentage of nodes
- Ensure at least 1 node per DC is checked
- **Split-brain detection**: Limited to sampled nodes
- **Use case**: Very large clusters (100+ nodes) in stable state
- **Time**: Proportional to sample percentage

**Configuration**:
```yaml
health_check_mode: 'status_only'  # full | status_only | majority_status | raft_only | sampled
```

**Important Notes on nodetool status**:
- Initial analysis incorrectly stated `nodetool status` is a "single call"
- **Reality**: For split-brain detection, we must cross-check each node's report against others
- This requires calling `nodetool status` from **multiple nodes** (ideally majority)
- Single-node `nodetool status` is fast (~2s) but may miss split-brain scenarios
- **Trade-off**: Speed vs. split-brain detection reliability

**Testing**:
- Unit test: Mock each mode, verify only expected checks are called
- Integration test: 10-node cluster with each mode, measure time
- Manual test: Compare detection capabilities with simulated split-brain

**Expected Impact**: 
- `status_only`: 98% reduction (120 min → 2 min) - **low split-brain detection**
- `majority_status`: 87% reduction (120 min → 15 min) - **good split-brain detection**
- `raft_only`: 97% reduction (120 min → 3 min) - **limited to Raft topology**

---

### Phase 3: Node Sampling for Large Clusters

**Objective**: Reduce checks proportionally while maintaining DC coverage

**Implementation**:
- Sample N% of nodes randomly
- **Guarantee**: At least 1 node per DC is always checked
- **Guarantee**: At least 2 nodes total (for minimal cross-validation)
- Apply full health check to sampled nodes only

**Configuration**:
```yaml
health_check_sample_nodes_percentage: 50  # 0-100, default: 100
```

**Sampling Algorithm**:
1. Group nodes by datacenter
2. For each DC: Select max(1, ceil(DC_nodes × percentage / 100))
3. Randomly select from each DC
4. Ensure total >= 2 nodes for cross-validation

**Example** (60 nodes, 5 DCs, 50% sampling):
- DC1: 12 nodes → sample 6
- DC2: 12 nodes → sample 6
- DC3: 12 nodes → sample 6
- DC4: 12 nodes → sample 6
- DC5: 12 nodes → sample 6
- **Total: 30 nodes checked**

**Testing**:
- Unit test: Verify sampling algorithm with various percentages and DC configurations
- Unit test: Verify minimum guarantees (1 per DC, 2 total)
- Integration test: Multi-DC cluster, verify sampling respects DC distribution

**Expected Impact**: Proportional reduction (50% sample = 50% time reduction)

---

### Phase 4: Exclude Nodes Running Nemesis

**Objective**: Skip health checks on nodes actively participating in nemesis operations

**Implementation**:
- Check `node.running_nemesis` flag before health check
- Skip node if flag is set
- Log exclusion for transparency

**Configuration**:
```yaml
health_check_exclude_running_nemesis: true  # default: true
```

**Considerations**:
- May reduce total checked nodes below desired sample
- Should still enforce minimum guarantees from Phase 3

**Testing**:
- Unit test: Mock nodes with `running_nemesis=True`, verify exclusion
- Integration test: Run parallel nemesis, verify active nodes excluded
- Manual test: Verify health check completes successfully with nemesis in progress

**Expected Impact**: Variable (depends on nemesis duration and concurrency)

---

### Phase 5: Parallel Health Check Execution

**Objective**: Check multiple nodes simultaneously to reduce total time

**Implementation**:
- Use `ThreadPoolExecutor` with configurable worker count
- Collect results from all workers
- Publish events in thread-safe manner
- Handle exceptions gracefully

**Configuration**:
```yaml
health_check_parallel_workers: 10  # default: 5
```

**Considerations**:
- Network bandwidth: More concurrent checks = more network traffic
- Cluster load: Parallel `nodetool` calls may impact cluster performance
- Event publishing: Must be thread-safe
- Recommended: 5-10 workers for typical clusters

**Testing**:
- Unit test: Mock parallel execution, verify all nodes checked
- Unit test: Verify thread-safe event publishing
- Integration test: 20+ node cluster, compare serial vs parallel time
- Load test: Verify cluster can handle concurrent nodetool calls

**Expected Impact**: 
- With 10 workers: 90% reduction (120 min → 12 min)
- Combines with other optimizations for greater effect

---

## Integration Test Plan

**Problem**: Raft check timing seems unexpectedly high, needs investigation

**Test Objectives**:
1. Measure actual time for each health check method
2. Compare theoretical vs actual performance
3. Identify bottlenecks in Raft checks
4. Validate split-brain detection capabilities

**Test Setup**:
- 30-node cluster (3 DCs × 10 nodes)
- Scylla 5.2+ (Raft enabled)
- Instrumented health check code with timing metrics

**Test Cases**:

### TC1: Baseline Measurement
```yaml
health_check_mode: 'full'
health_check_sample_nodes_percentage: 100
```
- Measure total time
- Measure per-node time
- Break down time per operation (status, peers, gossip, raft, tokenring)
- **Expected**: Identify which operations are slowest

### TC2: Status-Only Mode
```yaml
health_check_mode: 'status_only'
```
- Measure time for single `nodetool status` call
- Verify all nodes detected
- **Expected**: ~2 minutes total

### TC3: Majority Status Mode
```yaml
health_check_mode: 'majority_status'
```
- Measure time for majority validation
- Test split-brain detection with simulated partition
- **Expected**: ~10-15 minutes, detect split-brain

### TC4: Raft-Only Mode
```yaml
health_check_mode: 'raft_only'
```
- Measure Raft group0 query time
- Verify membership consistency
- **Expected**: <5 minutes (investigate if higher)
- **Debug**: Add detailed logging to Raft check implementation

### TC5: Sampling at 33%
```yaml
health_check_mode: 'full'
health_check_sample_nodes_percentage: 33
```
- Verify ~10 nodes checked (1/3 of 30)
- Verify at least 1 per DC
- Measure time reduction
- **Expected**: ~33% of baseline time

### TC6: Parallel Execution
```yaml
health_check_mode: 'full'
health_check_parallel_workers: 10
```
- Measure total time with parallel execution
- Monitor cluster CPU/network load
- Verify thread-safe operation
- **Expected**: ~10x speedup (limited by slowest operation)

### TC7: Combined Optimizations
```yaml
health_check_mode: 'majority_status'
health_check_sample_nodes_percentage: 50
health_check_parallel_workers: 10
health_check_skip_after_skipped_nemesis: true
```
- Test with mixed skipped/executed nemesis
- Measure overall time savings
- **Expected**: >95% reduction for skipped nemesis, >90% for executed

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
health_check_mode: 'full'
health_check_skip_after_skipped_nemesis: true
```
**Rationale**: Full validation is fast enough, provides maximum confidence

### Medium Clusters (10-30 nodes)
```yaml
cluster_health_check: true
health_check_mode: 'majority_status'
health_check_parallel_workers: 5
health_check_skip_after_skipped_nemesis: true
health_check_exclude_running_nemesis: true
```
**Rationale**: Balance between speed and split-brain detection

### Large Clusters (30-60 nodes)
```yaml
cluster_health_check: true
health_check_mode: 'majority_status'
health_check_sample_nodes_percentage: 50
health_check_parallel_workers: 10
health_check_skip_after_skipped_nemesis: true
health_check_exclude_running_nemesis: true
```
**Rationale**: Significant time savings while maintaining split-brain detection

### Very Large Clusters (60+ nodes)
```yaml
cluster_health_check: true
health_check_mode: 'status_only'
health_check_sample_nodes_percentage: 20
health_check_parallel_workers: 10
health_check_skip_after_skipped_nemesis: true
health_check_exclude_running_nemesis: true
```
**Rationale**: Prioritize speed for stable large clusters
**Note**: Reduced split-brain detection - consider periodic full checks

### Critical Tests (Release validation, etc.)
```yaml
cluster_health_check: true
health_check_mode: 'full'
health_check_parallel_workers: 10
health_check_skip_after_skipped_nemesis: false  # Always validate
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

## Implementation Phases & Timeline

### Phase 1: Skip for Skipped Nemesis (Week 1)
- [ ] Implement skip logic
- [ ] Add configuration parameter
- [ ] Unit tests
- [ ] Integration test
- [ ] Documentation

### Phase 2: Health Check Modes (Week 2-3)
- [ ] Implement mode enum
- [ ] Implement `status_only` mode
- [ ] Implement `majority_status` mode
- [ ] Implement `raft_only` mode
- [ ] Unit tests for each mode
- [ ] Integration tests
- [ ] Documentation

### Phase 3: Node Sampling (Week 4)
- [ ] Implement sampling algorithm
- [ ] Add DC-aware selection
- [ ] Unit tests
- [ ] Integration tests
- [ ] Documentation

### Phase 4: Exclude Running Nemesis (Week 5)
- [ ] Implement exclusion logic
- [ ] Unit tests
- [ ] Integration tests
- [ ] Documentation

### Phase 5: Parallel Execution (Week 6-7)
- [ ] Implement ThreadPoolExecutor
- [ ] Thread-safe event publishing
- [ ] Error handling
- [ ] Unit tests
- [ ] Load tests
- [ ] Documentation

### Phase 6: Integration Test Suite (Week 8)
- [ ] Build 30-node test cluster
- [ ] Implement all 7 test cases
- [ ] Collect performance metrics
- [ ] Investigate Raft timing
- [ ] Optimize based on findings
- [ ] Document results

### Phase 7: Refinement (Week 9)
- [ ] Address findings from integration tests
- [ ] Optimize slow paths
- [ ] Update configurations
- [ ] Final documentation

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

2. **Should we cache Raft group0 state?**
   - Pro: Faster checks
   - Con: May show stale data if topology changes rapidly

3. **Should sampling be deterministic or random?**
   - Deterministic: Same nodes always checked, easier debugging
   - Random: Better coverage over time, avoids blind spots

4. **Should we add automatic mode selection based on cluster size?**
   ```yaml
   health_check_mode: 'auto'  # Selects mode based on node count
   ```

5. **Should we track health check performance metrics?**
   - Time per check
   - Issues detected
   - False positive rate
   - → Use for auto-tuning

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
