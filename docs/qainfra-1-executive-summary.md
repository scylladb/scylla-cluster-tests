# QAINFRA-1: Capacity Reservation for GCE/Azure/OCI - Executive Summary

## Problem Statement

The ScyllaDB Cluster Tests framework currently supports **capacity reservation only on AWS**, which guarantees compute resource availability during test execution. This feature is missing for **GCE, Azure, and OCI** backends, leading to:

- Test failures due to insufficient capacity
- Increased test flakiness
- Inability to guarantee resources for critical testing
- Time wasted on retries

## Proposed Solution

Implement capacity reservation support for GCE, Azure, and OCI backends, mirroring the existing AWS implementation.

## Implementation Approach

### Phased Implementation (Recommended)

```
Phase 1: GCE (2-3 weeks)
    ├── Most mature platform support in SCT
    ├── Simpler API (Compute Engine Reservations)
    └── Immediate value for active GCE users

Phase 2: Azure (3-4 weeks)
    ├── Growing usage in SCT
    ├── More complex API (Capacity Reservation Groups)
    └── Enterprise customer demand

Phase 3: OCI (2-3 weeks, OPTIONAL)
    ├── Limited current usage
    ├── Straightforward API
    └── Future-proofing

Phase 4: Common Abstraction (1-2 weeks)
    └── Refactor all backends to share common interface
```

### Technical Architecture

Each backend will have its own capacity reservation module:

```
sdcm/provision/
├── aws/
│   └── capacity_reservation.py  [✓ EXISTS]
├── gce/
│   └── capacity_reservation.py  [NEW - Phase 1]
├── azure/
│   └── capacity_reservation.py  [NEW - Phase 2]
└── oci/
    └── capacity_reservation.py  [NEW - Phase 3]
```

Common interface in later phase:
```
sdcm/provision/common/
└── capacity_reservation.py      [NEW - Phase 4]
```

## Cloud Provider Comparison

| Feature | AWS | GCE | Azure | OCI |
|---------|-----|-----|-------|-----|
| **Current Support** | ✓ Complete | ✗ None | ✗ None | ✗ None |
| **API Complexity** | Medium | Low | High | Medium |
| **Scope** | AZ-specific | Zone/Regional | Zone-specific | AD-specific |
| **Cost Model** | Pay for reserved | Pay for reserved | Pay-as-you-go | Pay for reserved |
| **Fallback Support** | ✓ Multi-AZ | Can do multi-zone | Can do multi-zone | AD-limited |
| **Implementation Effort** | N/A | 2-3 weeks | 3-4 weeks | 2-3 weeks |

## Key Features to Implement

For each backend, implement:

1. **Enable/Disable Logic**
   - Configuration parameter: `{backend}_use_capacity_reservation`
   - Check if supported for test type (single-DC, on-demand instances)

2. **Reservation Creation**
   - Calculate required instance types and counts from test config
   - Create reservations before provisioning
   - Try multiple zones if capacity unavailable
   - Tag with test_id for tracking

3. **Reservation Usage**
   - Apply reservation IDs during instance provisioning
   - Verify instances use reserved capacity

4. **Cleanup**
   - Cancel reservations after test completion
   - Support cleanup across all regions
   - Handle cleanup on test failure

5. **Error Handling**
   - Graceful fallback if capacity unavailable
   - Skip tests if reservation fails (configurable)
   - Detailed error logging

## Configuration Examples

### GCE Configuration
```yaml
cluster_backend: gce
gce_use_capacity_reservation: true
gce_reservation_type: zonal  # or regional
n_db_nodes: 3
instance_type_db: n2-highmem-16
```

### Azure Configuration
```yaml
cluster_backend: azure
azure_use_capacity_reservation: true
azure_capacity_reservation_group: sct-test-group
n_db_nodes: 3
instance_type_db: Standard_L16s_v3
```

### OCI Configuration
```yaml
cluster_backend: oci
oci_use_capacity_reservation: true
oci_availability_domain: AD-1
n_db_nodes: 3
instance_type_db: VM.Standard.E4.Flex
```

## Cost Implications

### Important Cost Considerations:

⚠️ **All providers charge for reserved capacity even when unused**

**Mitigation Strategies:**
1. ✅ Automatic cleanup after test completion
2. ✅ Maximum reservation duration limits (based on test duration)
3. ✅ Monitoring and alerting for orphaned reservations
4. ✅ Resource tagging for cost tracking
5. ✅ Parallel cleanup jobs to prevent accumulation

**Estimated Cost Impact:**
- **GCE**: ~$2-5/hour per reserved instance
- **Azure**: ~$2-6/hour per reserved VM
- **OCI**: ~$1-4/hour per reserved instance

With proper cleanup, cost increase should be **< 5% of current test infrastructure costs**.

## Benefits

### For Test Reliability
- ✅ Eliminate capacity-related test failures
- ✅ Reduce test flakiness
- ✅ Faster test execution (no retries)
- ✅ Predictable test duration

### For Resource Management
- ✅ Guaranteed capacity for critical tests
- ✅ Better resource planning
- ✅ Support for large-scale testing
- ✅ Multi-zone failover capability

### For Development Velocity
- ✅ Fewer manual interventions
- ✅ More reliable CI/CD
- ✅ Consistent test results
- ✅ Better debugging experience

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Orphaned Reservations** | High cost | Automated cleanup, monitoring, alerts |
| **API Changes** | Code breaks | Version pinning, extensive testing |
| **Capacity Still Unavailable** | Test failures | Multi-zone fallback, skip logic |
| **Implementation Bugs** | Cost/reliability | Phased rollout, extensive testing |
| **Learning Curve** | Slow adoption | Good documentation, examples |

## Success Metrics

### Phase 1 (GCE) Success Criteria:
- [ ] Zero capacity-related GCE test failures
- [ ] < 5% cost increase from reservations
- [ ] < 1% orphaned reservations
- [ ] Documentation complete
- [ ] Unit test coverage > 80%

### Phase 2 (Azure) Success Criteria:
- [ ] Zero capacity-related Azure test failures
- [ ] < 5% cost increase from reservations
- [ ] < 1% orphaned reservations
- [ ] Documentation complete
- [ ] Unit test coverage > 80%

### Phase 3 (OCI) Success Criteria:
- [ ] Zero capacity-related OCI test failures
- [ ] < 5% cost increase from reservations
- [ ] < 1% orphaned reservations
- [ ] Documentation complete
- [ ] Unit test coverage > 80%

## Timeline Summary

### Aggressive (Parallel Work):
```
┌─────────────────────────────────────────────┐
│ Phase 1: GCE        │ 3 weeks               │
├─────────────────────────────────────────────┤
│ Phase 2: Azure      │ 4 weeks               │
├─────────────────────────────────────────────┤
│ Phase 3: OCI        │ 3 weeks (optional)    │
├─────────────────────────────────────────────┤
│ Phase 4: Abstraction│ 2 weeks               │
└─────────────────────────────────────────────┘
Total: 10-12 weeks (if all phases)
```

### Conservative (Sequential Work):
```
┌─────────────────────────────────────────────┐
│ Phase 1: GCE        │ 3 weeks               │
│ Phase 2: Azure      │ 4 weeks               │
│ Phase 3: OCI        │ 3 weeks (optional)    │
│ Phase 4: Abstraction│ 2 weeks               │
│ Phase 5: Polish     │ 2 weeks               │
└─────────────────────────────────────────────┘
Total: 12-14 weeks (if all phases)
```

## Recommendations

### Immediate Actions:
1. ✅ **Approve Phase 1 (GCE)** - Highest priority, immediate value
2. ✅ **Secure test accounts** - Ensure GCE/Azure/OCI accounts have necessary permissions
3. ✅ **Allocate resources** - 1 senior engineer for 3 weeks (Phase 1)

### Phase 1 Only (Minimum Viable):
- Implement GCE capacity reservation
- Validate approach and gather learnings
- Decide on Phase 2/3 based on results

### Full Implementation (All Phases):
- Complete all backends for consistency
- Common abstraction for maintainability
- Comprehensive documentation

## Decision Required

### Questions for Stakeholders:

1. **Scope**: Which phases should be implemented?
   - [ ] Phase 1 only (GCE)
   - [ ] Phases 1-2 (GCE + Azure)
   - [ ] All phases (GCE + Azure + OCI)

2. **Timeline**: Aggressive or conservative approach?
   - [ ] Aggressive (parallel work, 10 weeks)
   - [ ] Conservative (sequential, 14 weeks)

3. **Priority**: Any specific backend that's higher priority?
   - Current recommendation: GCE → Azure → OCI

4. **Budget**: Cost approval for reservation testing?
   - Estimated testing cost: $500-1000 per phase

## Next Steps (Awaiting Approval)

**If approved:**
1. Set up GCE test project with capacity reservation quota
2. Begin Phase 1 implementation
3. Weekly progress updates
4. Demo at end of Phase 1

**For more details:**
- See full plan: `docs/qainfra-1-capacity-reservation-plan.md`
- AWS reference: `sdcm/provision/aws/capacity_reservation.py`
- Questions? Contact the implementation team

---

**Document Status**: ✅ Ready for Review  
**Last Updated**: 2026-01-19  
**Owner**: SCT Infrastructure Team  
**Reviewers**: TBD
