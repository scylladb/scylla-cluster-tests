# Mini-Plan: Remove MultitenantValue and K8S Multitenancy Support

**Date:** 2026-04-20
**Estimated LOC:** ~500 removed, ~100 modified
**Related PR:** N/A

## Problem
K8S multitenancy (running multiple Scylla clusters per K8S cluster) is unused.
The `MultitenantValue` type system, `k8s_tenants_num` parameter, multitenant test modules,
and associated configs/pipelines add complexity with no active use case.
The feature is documented in `docs/shelved/MULTITENANTK8s.md` for future re-implementation.

## Approach
- Extend `docs/shelved/MULTITENANTK8s.md` with full documentation of removed components
- Remove `MultitenantValueMarker`, `is_multitenant_field()`, `MultitenantValue()` from `sdcm/sct_config.py`
- Unwrap 18 config params from `MultitenantValue(X)` to plain `X`
- Remove `k8s_tenants_num` parameter from `sdcm/sct_config.py`
- Remove multitenant validation branches and doc-gen logic from `sdcm/sct_config.py`
- Delete `sdcm/utils/k8s_operator/multitenant_common.py`
- Remove `*_multitenant` lists from `sdcm/tester.py`; simplify GKE/EKS provisioning to single cluster
- Replace `self.k8s_clusters[0].tenants_number` loops with direct single-cluster creation
- Delete 2 test modules, 3 test configs, 6 configuration fragments, 8 Jenkins pipelines
- Delete unit test and test data for multitenant
- Simplify remaining references in `sstables.py`, `loader_utils.py`, `nemesis/__init__.py`, `cluster_k8s/__init__.py`

## Files to Modify
- `docs/shelved/MULTITENANTK8s.md` -- extend with full feature documentation
- `sdcm/sct_config.py` -- remove MultitenantValue type system, k8s_tenants_num, validation, doc-gen
- `sdcm/tester.py` -- remove *_multitenant lists, simplify K8S provisioning to 1 cluster
- `sdcm/cluster_k8s/__init__.py` -- remove tenants_number property, simplify crypto buffer to constant 10
- `sdcm/nemesis/__init__.py` -- remove k8s_tenants_num check, simplify _get_neighbour_scylla_pods
- `sdcm/teardown_validators/sstables.py` -- replace db_clusters_multitenant with [self.tester.db_cluster]
- `sdcm/utils/loader_utils.py` -- remove multitenant nested list handling

**Files to delete (24):**
- `sdcm/utils/k8s_operator/multitenant_common.py`
- `longevity_operator_multi_tenant_test.py`
- `performance_regression_operator_multi_tenant_test.py`
- `unit_tests/unit/test_utils__operator__multitenant_common.py`
- `unit_tests/test_data/test_config/multitenant/shared_option_values.yaml`
- `unit_tests/test_data/test_config/multitenant/unique_option_values.yaml`
- `test-cases/scylla-operator/longevity-scylla-operator-3h-multitenant.yaml`
- `test-cases/scylla-operator/longevity-scylla-operator-12h-multitenant-14-clients.yaml`
- `test-cases/performance/perf-regression-latency-k8s-multitenant.yaml`
- `configurations/operator/perf-regression-latency-and-throughput-multitenant-2-clients.yaml`
- `configurations/operator/perf-regression-latency-and-throughput-multitenant-7-clients.yaml`
- `configurations/operator/perf-regression-latency-and-throughput-multitenant-14-clients.yaml`
- `configurations/operator/perf-regression-latency-multitenant-2-clients.yaml`
- `configurations/operator/perf-regression-latency-multitenant-7-clients.yaml`
- `configurations/operator/perf-regression-latency-multitenant-14-clients.yaml`
- `jenkins-pipelines/operator/eks/longevity-scylla-operator-3h-multitenant-eks.jenkinsfile`
- `jenkins-pipelines/operator/eks/longevity-scylla-operator-12h-multitenant-eks.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-and-throughput-eks-multitenant-2-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-and-throughput-eks-multitenant-7-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-and-throughput-eks-multitenant-14-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-eks-multitenant-2-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-eks-multitenant-7-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-eks-multitenant-14-clients.jenkinsfile`

## Verification
- [ ] `uv run sct.py unit-tests` passes
- [ ] `uv run sct.py pre-commit` passes
- [ ] `grep -rn "multitenant\|MultitenantValue\|k8s_tenants_num" sdcm/ *.py` returns no matches (excluding docs/shelved/)
- [ ] `docs/shelved/MULTITENANTK8s.md` contains full documentation of removed features
