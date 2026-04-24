# Background

Running on K8s with Multitenancy (i.e. more DB on one physical node) was supported but not used much recently.
With no real cases running regularly, it is not needed anymore.

Goal of this document is to document features that were required for making multitenancy work that were removed so if needed they can be revisited.

### Exclusive nemesis

Nemesis which touch VM directly in K8s environment can affect other NemesisRunners when run in parallel.
For this reason exclusive mechanism was introduced, which is a simple locking mechanism with global lock across different NemesisRunners.
NemesisRunners needs to acquire the lock before executing nemesis that is marked as "exclusive" (via class level varible in NemesisBaseClass)

### MultitenantValue config type system

`MultitenantValue` was a type wrapper in `sdcm/sct_config.py` that enabled per-tenant config values for K8S multitenancy.

**How it worked:**
- `MultitenantValueMarker` — a marker class to tag fields at the type-annotation level
- `is_multitenant_field(field)` — inspected pydantic `FieldInfo` to detect the marker
- `MultitenantValue(inner_type)` — wrapped type `T` into `Annotated[T | dict[str, T], MultitenantValueMarker()]`

This allowed configuration values to be specified as:
- **Single value:** `5` (shared across all tenants)
- **List (index-based):** `[5, 7]` (one per tenant, matched by position)
- **Dict (key-based):** `{tenant1: 5, tenant2: 7}` (explicit tenant keys)

**18 config parameters** used `MultitenantValue`:
`nemesis_class_name`, `nemesis_interval`, `nemesis_sequence_sleep_between_ops`, `nemesis_during_prepare`,
`nemesis_seed`, `nemesis_add_node_cnt`, `space_node_threshold`, `nemesis_filter_seeds`, `stress_cmd`,
`round_robin`, `stress_cmd_w`, `stress_cmd_r`, `stress_cmd_m`, `stress_cmd_read_disk`,
`stress_cmd_cache_warmup`, `prepare_write_cmd`, `nemesis_selector`, `nemesis_multiply_factor`

**Validation logic** in `SCTConfiguration._check_env_params()` had two special branches:
- Dict-based: validated each `tenant_value` individually when `k8s_tenants_num > 1`
- List-based: validated each list element individually when `k8s_tenants_num > 1`

**Documentation generation** in `get_config_doc_as_md()` appended a multitenancy note to supported fields,
and `get_annotations_as_strings()` unwrapped `Union[T, dict[str, T]]` to display just `T`.

### k8s_tenants_num parameter

`k8s_tenants_num` (type `int`) controlled how many Scylla clusters to create per K8S cluster.
Default was 1. Used by:
- `cluster_k8s/__init__.py` `tenants_number` property — returned `k8s_tenants_num or 1`
- `cluster_k8s/__init__.py` crypto key buffer sizing — `k8s_tenants_num * 10`
- `tester.py` GKE/EKS provisioning — `range(self.k8s_clusters[0].tenants_number)` loops
- `nemesis/__init__.py` `_get_neighbour_scylla_pods()` — only searched for neighbours when `k8s_tenants_num >= 2`

### Runtime tenant splitting (multitenant_common.py)

`sdcm/utils/k8s_operator/multitenant_common.py` contained:

**`TenantMixin`** — A mixin class that represented a single tenant. It held per-tenant `db_cluster`, `loaders`, `monitors`, `prometheus_db`, and a deep-copied `params`. Each tenant got a unique `_test_id` suffixed with the cluster index.

**`get_tenants(test_class_instance)`** — Created tenant instances by:
1. Dynamically generating a tenant class inheriting from `TenantMixin` and the parent test class
2. Iterating `db_clusters_multitenant` to create one tenant per DB cluster
3. Splitting `MultitenantValue` parameters: dict values by `tenant{i}` keys, list values by index
4. Writing the split values to each tenant's `params` and `db_cluster.params`

**`MultiTenantTestMixin`** — A test mixin with `setUp()` that called `get_tenants()` and `stop_resources()` that stopped all tenants in parallel via `ParallelObject`.

### Multitenant lists in tester.py

`ClusterTester` maintained four parallel lists:
- `db_clusters_multitenant: list[BaseScyllaCluster]`
- `loaders_multitenant: list`
- `monitors_multitenant: list`
- `prometheus_db_multitenant: list`

These were populated during `get_cluster_k8s_gke()` and `get_cluster_k8s_eks()` in `init_resources()`.
Each method looped `range(tenants_number)` creating DB clusters, loaders, and monitors with
prefixed names (`{i+1}-` prefix for i>0). The first element was aliased to `self.db_cluster`,
`self.loaders`, `self.monitors`.

All teardown, post-validation, monitoring init, and loader setup iterated these lists.
Non-K8S backends populated them with single-element lists for uniformity.

### Test modules

**`longevity_operator_multi_tenant_test.py`** — `LongevityOperatorMultiTenantTest(MultiTenantTestMixin, LongevityTest)`.
Ran `test_custom_time()` in parallel across all tenants using `ParallelObject`.

**`performance_regression_operator_multi_tenant_test.py`** — `PerformanceRegressionOperatorMultiTenantTest(MultiTenantTestMixin, PerformanceRegressionTest)`.
Overrode `preload_data`, `run_read_workload`, `run_write_workload`, `run_mixed_workload`, `run_workload`,
`run_fstrim_on_all_db_nodes`, `wait_no_compactions_running` — all dispatching to tenants in parallel.
Raised `NotImplementedError` for MV and counter/timeseries tests.

### Test configs

- `test-cases/scylla-operator/longevity-scylla-operator-3h-multitenant.yaml` — 2 tenants, list-based per-tenant nemesis config
- `test-cases/scylla-operator/longevity-scylla-operator-12h-multitenant-14-clients.yaml` — 14 tenants, dict-based per-tenant config
- `test-cases/performance/perf-regression-latency-k8s-multitenant.yaml` — base config for perf multitenant tests

### Configuration fragments

- `configurations/operator/perf-regression-latency-and-throughput-multitenant-{2,7,14}-clients.yaml`
- `configurations/operator/perf-regression-latency-multitenant-{2,7,14}-clients.yaml`

### Jenkins pipelines

- `jenkins-pipelines/operator/eks/longevity-scylla-operator-3h-multitenant-eks.jenkinsfile`
- `jenkins-pipelines/operator/eks/longevity-scylla-operator-12h-multitenant-eks.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-and-throughput-eks-multitenant-{2,7,14}-clients.jenkinsfile`
- `jenkins-pipelines/operator/performance/perf-regression-latency-eks-multitenant-{2,7,14}-clients.jenkinsfile`

### Unit tests

- `unit_tests/unit/test_utils__operator__multitenant_common.py` — tested `get_tenants()` with shared and unique option values
- `unit_tests/test_data/test_config/multitenant/shared_option_values.yaml`
- `unit_tests/test_data/test_config/multitenant/unique_option_values.yaml`

### Re-implementation notes

To bring back multitenancy:
1. Re-introduce a type-level marker for config params that support per-tenant values (the `Annotated[T | dict[str, T], Marker]` pattern worked well)
2. Create a tenant factory that deep-copies params and splits per-tenant values
3. Add `*_multitenant` lists to `ClusterTester` or use a `TenantManager` abstraction
4. Provision loops in `get_cluster_k8s_{gke,eks}()` need `range(tenants_number)` iteration
5. The `TenantMixin` approach of dynamically creating classes via `type()` inheriting from both `TenantMixin` and the parent test class was effective for running isolated test logic per tenant
