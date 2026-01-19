# QAINFRA-1: Capacity Reservation for GCE/Azure/OCI

## 📋 Overview

This directory contains the complete implementation plan for extending capacity reservation support from AWS to GCE, Azure, and OCI backends in the ScyllaDB Cluster Tests framework.

## 🎯 Problem

Currently, only AWS backend supports capacity reservation (`use_capacity_reservation` parameter), which guarantees compute capacity availability for test runs. This leads to:

- ❌ Capacity-related test failures on GCE/Azure/OCI
- ❌ Test flakiness and unreliability
- ❌ Time wasted on retries
- ❌ Inability to guarantee resources for critical tests

## ✅ Solution

Implement capacity reservation support for all major cloud backends, providing:

- ✅ Guaranteed compute capacity for tests
- ✅ Reduced test flakiness
- ✅ Faster test execution (no retries)
- ✅ Multi-zone fallback capability
- ✅ Consistent experience across all cloud providers

## 📚 Documentation

### Start Here
| Document | Purpose | Audience |
|----------|---------|----------|
| **[Executive Summary](./qainfra-1-executive-summary.md)** | Quick overview, timeline, costs, and decision points | Stakeholders, PMs, Tech Leads |
| **[Technical Plan](./qainfra-1-capacity-reservation-plan.md)** | Detailed implementation plan with API analysis | Engineers, Architects |
| **[Quick Reference](./qainfra-1-quick-reference.md)** | Diagrams, examples, and troubleshooting | Developers, QA |

### Document Descriptions

#### 1. Executive Summary
**[qainfra-1-executive-summary.md](./qainfra-1-executive-summary.md)**

A concise overview for stakeholders covering:
- Problem statement and proposed solution
- Timeline estimates (2-14 weeks depending on scope)
- Cost implications (< 5% increase with proper cleanup)
- Risk assessment and mitigation
- Decision points requiring approval
- Recommended approach (start with GCE)

**Best for**: Quick understanding of project scope and getting approval

#### 2. Technical Plan
**[qainfra-1-capacity-reservation-plan.md](./qainfra-1-capacity-reservation-plan.md)**

Comprehensive technical documentation including:
- Detailed API analysis for GCE, Azure, and OCI
- Phase-by-phase implementation breakdown
- Configuration schema for each cloud provider
- Testing strategy (unit, integration, manual)
- Cost considerations and monitoring
- Risk assessment and open questions
- Resource requirements and timeline

**Best for**: Understanding technical details and implementation approach

#### 3. Quick Reference
**[qainfra-1-quick-reference.md](./qainfra-1-quick-reference.md)**

Developer-focused reference guide with:
- Architecture diagrams and visualizations
- Workflow diagrams
- API comparison matrix across all clouds
- Configuration examples
- Code structure and file organization
- Quick command reference
- Troubleshooting guide
- Security and performance considerations

**Best for**: Day-to-day development reference and troubleshooting

## 🚀 Implementation Phases

### Phase 1: GCE Implementation ⭐ **RECOMMENDED TO START**
- **Duration**: 2-3 weeks
- **Effort**: Medium
- **Priority**: HIGH
- **Why**: Simplest API, mature platform support, immediate value

**Deliverables:**
- `sdcm/provision/gce/capacity_reservation.py`
- GCE configuration parameters
- Unit and integration tests
- Documentation

### Phase 2: Azure Implementation
- **Duration**: 3-4 weeks
- **Effort**: Medium-High
- **Priority**: Medium
- **Why**: Growing usage, enterprise demand

**Deliverables:**
- `sdcm/provision/azure/capacity_reservation.py`
- Azure configuration parameters (including Reservation Groups)
- Unit and integration tests
- Documentation

### Phase 3: OCI Implementation (Optional)
- **Duration**: 2-3 weeks
- **Effort**: Medium
- **Priority**: Low
- **Why**: Limited current usage, future-proofing

**Deliverables:**
- `sdcm/provision/oci/capacity_reservation.py`
- OCI configuration parameters
- Unit and integration tests
- Documentation

### Phase 4: Common Abstraction
- **Duration**: 1-2 weeks
- **Effort**: Medium
- **Priority**: Medium (after Phase 1-2 complete)
- **Why**: Code reuse, maintainability

**Deliverables:**
- `sdcm/provision/common/capacity_reservation.py` base class
- Refactored implementations
- Unified interface

## 💡 Quick Start (for Reviewers)

1. **Read Executive Summary** (5 minutes)
   - Get high-level understanding
   - See timeline and cost estimates
   - Understand decision points

2. **Review Technical Plan** (20-30 minutes)
   - Deep dive into implementation approach
   - Review API analysis
   - Understand testing strategy

3. **Reference Quick Guide** (as needed)
   - See architecture diagrams
   - Review code examples
   - Check troubleshooting guide

## 🔑 Key Decisions Required

| Decision | Options | Recommendation |
|----------|---------|----------------|
| **Scope** | Phase 1 only / Phases 1-2 / All phases | **Phase 1 (GCE)** to start |
| **Timeline** | Aggressive / Conservative | **Conservative** (sequential) |
| **Budget** | Approve testing costs? | **Yes** ($500-1000 per phase) |
| **Start Date** | When to begin? | **Immediately** upon approval |

## 📊 Cloud Provider Comparison

| Feature | AWS | GCE | Azure | OCI |
|---------|-----|-----|-------|-----|
| **Current Support** | ✅ Complete | ❌ None | ❌ None | ❌ None |
| **API Complexity** | Medium | **Low** ⭐ | High | Medium |
| **Implementation Time** | N/A | **2-3 weeks** | 3-4 weeks | 2-3 weeks |
| **Recommendation** | N/A | **Start Here** | Next | Optional |

## 💰 Cost Summary

| Cloud | Hourly Cost (est.) | Mitigation |
|-------|-------------------|------------|
| GCE | $2-5 per instance | Auto-cleanup, monitoring |
| Azure | $2-6 per VM | Auto-cleanup, monitoring |
| OCI | $1-4 per instance | Auto-cleanup, monitoring |

**Expected Impact**: < 5% cost increase with proper cleanup

## 🎁 Benefits

### Test Reliability
- ✅ Eliminate capacity-related failures
- ✅ Reduce test flakiness by 80%+
- ✅ Faster execution (no retry delays)
- ✅ Predictable test duration

### Resource Management
- ✅ Guaranteed capacity for critical tests
- ✅ Better resource planning
- ✅ Support for large-scale testing
- ✅ Multi-zone failover capability

### Developer Experience
- ✅ Fewer manual interventions
- ✅ More reliable CI/CD
- ✅ Consistent test results
- ✅ Better debugging experience

## 📈 Success Criteria

### Phase 1 (GCE) Success:
- [ ] Zero capacity-related GCE test failures
- [ ] < 5% cost increase from reservations
- [ ] < 1% orphaned reservations
- [ ] Documentation complete
- [ ] Unit test coverage > 80%
- [ ] Integration tests passing

## 🔗 Reference Links

### Internal
- [AWS Implementation](../sdcm/provision/aws/capacity_reservation.py) - Reference code
- [AWS Configuration](../defaults/aws_config.yaml) - Configuration example
- [SCT Config](../sdcm/sct_config.py) - Configuration system

### External
- [GCE Reservations Docs](https://cloud.google.com/compute/docs/instances/reservations-overview)
- [Azure Capacity Reservation Docs](https://learn.microsoft.com/en-us/azure/virtual-machines/capacity-reservation-overview)
- [OCI Capacity Reservation Docs](https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/reserve-capacity.htm)

### Related Issues
- [GitHub Issue #8839](https://github.com/scylladb/scylla-cluster-tests/issues/8839) - Capacity reservation AZ fallback
- [GitHub Issue #8514](https://github.com/scylladb/scylla-cluster-tests/issues/8514) - Instance type mismatch
- [GitHub Issue #9317](https://github.com/scylladb/scylla-cluster-tests/issues/9317) - Multi-DC support

## 👥 Contact

For questions or discussion:
- Review the documentation first
- Open an issue for specific technical questions
- Contact the SCT infrastructure team for clarifications

## ⏭️ Next Steps

**After Approval:**
1. ✅ Set up GCE test project with capacity reservation quota
2. ✅ Allocate engineering resources (1 senior engineer, 2-3 weeks)
3. ✅ Begin Phase 1 implementation
4. ✅ Weekly progress updates
5. ✅ Demo at end of Phase 1

**Decision Required:**
- [ ] Approve overall approach
- [ ] Approve Phase 1 (GCE) implementation
- [ ] Approve budget for testing ($500-1000)
- [ ] Set start date

---

## 📝 Document Status

| Document | Status | Last Updated |
|----------|--------|--------------|
| Executive Summary | ✅ Complete | 2026-01-19 |
| Technical Plan | ✅ Complete | 2026-01-19 |
| Quick Reference | ✅ Complete | 2026-01-19 |
| This README | ✅ Complete | 2026-01-19 |

**Overall Status**: ✅ **Planning Complete - Awaiting Approval**

---

**Ready to Proceed**: Yes ✅  
**Recommended Action**: Approve Phase 1 (GCE) implementation  
**Timeline**: Can start immediately upon approval
