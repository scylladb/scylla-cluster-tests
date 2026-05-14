---
status: draft
domain: framework
created: 2026-05-14
last_updated: 2026-06-24
owner: aleksbykov
---

# Feature-Aware Adaptive Timeouts for Topology Operations

## Problem Statement

`adaptive_timeout` currently sizes topology-operation budgets from node-local load, disk, and throughput signals, plus a tablet-mode branch, but it does not account for cluster features that materially slow topology changes such as CDC, materialized views (including secondary-index workloads), counters, and LWT-heavy workloads. The current formulas for decommission, new-node bootstrap, and tablet migration are in `sdcm/utils/adaptive_timeouts/__init__.py:38-110`, while timeout monitoring and event publication happen in `sdcm/utils/adaptive_timeouts/__init__.py:187-237` and `sdcm/sct_events/system.py:100-114`. In feature-heavy clusters this underestimation can produce premature soft-timeout or hard-timeout events even though the topology operation is still making normal progress.

The failure mode is most visible for decommission, where a feature-heavy cluster routinely needs more time to stream data and migrate per-feature state (CDC log, view updates, Paxos/counter coordination) than the current load-only formula predicts. The effect of correcting the timeout calculation differs by decommission mode. In **tablet mode**, `_get_decommission_timeout` returns both a soft and a hard timeout and `adaptive_timeout` starts a `TimeoutMonitor`, so a feature-aware budget keeps the monitor from firing prematurely while the hard timeout still aborts an operation that has genuinely stalled. In **vnode (non-tablet) mode**, the calculator returns `(soft_timeout, None)` and no `TimeoutMonitor` is started; the yielded soft timeout *is* the operation budget consumed by the caller, so scaling it directly widens the actual wait window before the operation is reported as timed out. Both paths benefit from feature-aware scaling, but only the tablet path is guarded by a hard-timeout monitor. In either case, no change to the underlying decommission command or its execution path is required.

This plan fixes that gap by making `adaptive_timeout` feature-aware so the computed soft and hard timeouts account for active cluster features, starting with decommission. Feature detection uses the existing `NodeLoadInfoService` metrics path: it reads Prometheus counters through the same `_get_scylla_metrics()` implementation already used for adaptive timeout metrics. The detected values are cumulative activity totals (e.g. `scylla_cdc_operations_total` accumulates since boot), so they identify **active feature signals** (`metric > 0`) rather than proving that a feature is currently enabled, and they are not suitable for proportional scaling. Feature activity therefore *gates* the timeout uplift, while the magnitude of the uplift is driven by the decommission node's `cpu_load_5` (the `node_load5` 5-minute load average already exposed at `sdcm/utils/adaptive_timeouts/load_info_store.py:166`): a busier node receives a larger budget than an idle one for the same feature set. Secondary-index workloads are covered by the materialized-view signal/model in v1. After real decommission validation, a follow-up phase can apply the same budget logic to new-node and tablet-migration monitoring. A boolean master switch allows disabling all feature-aware adjustments without code changes.

## Current State

- The only topology-specific timeout calculators are `_get_decommission_timeout`, `_get_new_node_timeout`, and `_get_tablet_migration_timeout`; all three derive budgets from node size, disk size, expected throughput, caller-supplied timeout, and `tablets_enabled`, with no feature-aware adjustment path today. `sdcm/utils/adaptive_timeouts/__init__.py:38-110`

- `adaptive_timeout` currently treats only `{DECOMMISSION, NEW_NODE, TABLET_MIGRATION}` as tablet-sensitive, computes `tablets_enabled`, gathers node metrics only when `adaptive_timeout_store_metrics` is enabled and the node is available, yields `hard_timeout or soft_timeout`, and starts `TimeoutMonitor` only when a hard timeout exists. There is no feature-signal profile in `NodeLoadInfoService`, no multiplier composition, and no distinction between base and adjusted budgets. `sdcm/utils/adaptive_timeouts/__init__.py:240-300`

- The only extra cluster context attached to stored metrics is `n_db_nodes`, so post-failure analysis has almost no visibility into topology shape, active feature signals, or why a timeout was chosen. `sdcm/utils/adaptive_timeouts/__init__.py:179-184`

- `NodeLoadInfoService` is intentionally node-centric: it provides CPU load, shards, bandwidth, IOPS, data size, disk size, and expected streaming throughput through the existing metrics-gathering path, but it does not currently expose a `ClusterFeatureProfile` derived from feature-specific Prometheus indicators for CDC, MV, LWT, or counters. `sdcm/utils/adaptive_timeouts/load_info_store.py:68-242`

- Existing adaptive-timeout metrics gathering and telemetry behavior is toggled through `adaptive_timeout_store_metrics`. A per-operation multiplier system already exists: `adaptive_timeout_multipliers` (a pydantic `AdaptiveTimeoutMultipliers` `RootModel` keyed by `Operations` enum value, `sdcm/sct_config.py:306-348`, field at `sdcm/sct_config.py:1376`) is applied inside `adaptive_timeout` via `_get_operation_timeout_factor()` (`sdcm/utils/adaptive_timeouts/__init__.py:19-30`), which multiplies both soft and hard timeouts at `sdcm/utils/adaptive_timeouts/__init__.py:279-282`. The new feature-aware `combined_factor` must compose with this existing `operation_factor`, not replace it. This plan preserves the current metrics-gathering behavior: if adaptive timeout metrics are not collected today, feature-aware scaling falls back to the baseline rather than introducing a separate metrics-fetch path. `sdcm/sct_config.py:1373-1390`, `defaults/test_default.yaml:358-359`, `docs/configuration_options.md:2154-2169`

- The metrics stack already exposes stable counters that can be used for runtime feature-activity detection with no additional schema queries. All are cumulative activity totals, so they support boolean (`> 0`) activity detection only, not proportional counts or current enabled-state detection:
  - CDC: `scylla_cdc_operations_total`
  - LWT: `scylla_storage_proxy_coordinator_cas_total_operations`
  - MV/SI: `scylla_database_total_view_updates_pushed_local` / `scylla_database_total_view_updates_pushed_remote` (SI is treated as part of the MV/view-update signal in v1)
  - Counters: `scylla_database_counter_cell_lock_acquisition`

- **Empirical metric availability (validated against 5 production Prometheus instances, June 2026):**
  - CDC metrics (`scylla_cdc_operations_total`, `scylla_cdc_operations_on_clustering_row_performed_total`) are **only emitted when CDC is enabled at the table level**. On 4/5 tested clusters without CDC, these metrics do not exist in TSDB at all (not zero -- absent). Metric absence must be treated as `cdc=False`.
  - LWT primary (`scylla_storage_proxy_coordinator_cas_total_operations`) is **version/config-dependent**: absent on 4/5 clusters, present with values up to 823K on the feature-heavy cluster. The fallback `cas_prune` mirrors `cas_total_operations` exactly when both exist.
  - **`scylla_storage_proxy_coordinator_cas_foreground` is a gauge (in-flight operations), NOT a cumulative counter.** On the feature-heavy cluster with active LWT, 3/15 nodes showed `cas_foreground=0` despite `cas_total_operations > 100K`. It is unreliable for boolean `> 0` activity detection and must NOT be used as primary LWT signal.
  - Counter fallback (`scylla_storage_proxy_replica_received_counter_updates`) is **version-dependent**: absent on 4/5 clusters, present on the feature-heavy cluster. It detected activity on 2 nodes where `counter_cell_lock_acquisition=0`, proving it adds real coverage as a fallback.
  - MV/SI metrics (`total_view_updates_pushed_local/remote`) are **universally present** across all 5 clusters. On 1/5 clusters (multi-DC, 21 nodes), they are present but at value=0 -- confirming the `> 0` boolean detection correctly returns `False` for inactive-but-present metrics.
  - `node_load5` is **universally present** on all clusters.
  - Observed `cpu_load_5 / shards_count` ranges: 0.2-1.0 at steady state (30-shard nodes), 0.2-1.66 at peak (8-shard nodes). The `CPU_SCALE_CAP=2.0` floor at 1.0 is empirically validated: most nodes stay at or below 1.0 in steady state, with meaningful uplift only during active topology operations.
  - **Consolidated validation matrix (5 Prometheus instances):**

    | Metric | Prom 1 (6n/30s, eu-west-1) | Prom 2 (9n/30s, us-east-1) | Prom 3 (6n/30s, us-east-1) | Prom 4 (15n/8s, eu-west-1) | Prom 5 (21n/~10s, multi-DC) |
    |--------|:---:|:---:|:---:|:---:|:---:|
    | `scylla_cdc_operations_total` | ABSENT | ABSENT | ABSENT | **7,063,215** | ABSENT |
    | `scylla_storage_proxy_coordinator_cas_total_operations` | ABSENT | ABSENT | ABSENT | **823,058** | ABSENT |
    | `scylla_storage_proxy_coordinator_cas_foreground` | val=0 | val=0 | val=0 | **130** (gauge!) | val=0 |
    | `scylla_storage_proxy_coordinator_cas_prune` | val=0 | val=0 | val=0 | **823,058** | val=0 |
    | `scylla_database_total_view_updates_pushed_local` | **20.7M** | **69.6M** | **14.4M** | **37.1M** | val=0 |
    | `scylla_database_total_view_updates_pushed_remote` | **10.4M** | **65.3M** | **11.1M** | **14.3M** | val=0 |
    | `scylla_database_counter_cell_lock_acquisition` | val=0 | val=0 | val=0 | **150M** | val=0 |
    | `scylla_storage_proxy_replica_received_counter_updates` | ABSENT | ABSENT | ABSENT | **50M** | ABSENT |
    | `node_load5` | **76.78** | **76.45** | **72.66** | **13.29** | **41.37** |

- The node load signal used to scale the uplift is already collected: `NodeLoadInfoService.cpu_load_5` returns `node_load5` (5-minute load average), and `shards_count` is available on the same service, so `cpu_load_5 / shards_count` (load per shard) can be computed with no new collection path. `sdcm/utils/adaptive_timeouts/load_info_store.py:165-171`

- Existing unit coverage validates the baseline decommission/new-node/tablet-migration formulas, fallback behavior, and tablet-mode branching, but there is no coverage for feature-aware adjustments or expanded telemetry. `unit_tests/unit/test_adaptive_timeouts.py:185-287`

## Goals

1. **Increase decommission budgets first when relevant feature activity is detected.**
   The first implementation must apply feature-aware scaling only to `DECOMMISSION`. For decommission, the computed soft timeout must be strictly greater than the baseline when any supported feature signal in `{cdc, mv, lwt, counter}` is active. Secondary-index workloads are covered by the MV/view-update signal. When a hard timeout already exists, the computed hard timeout must also be strictly greater than the baseline. The size of the increase is modulated by the decommission node's `cpu_load_5` per shard, so a busier node receives a larger budget than an idle one for the same feature set. `NEW_NODE` and `TABLET_MIGRATION` are enabled only after decommission has been validated with real tests.

2. **Preserve current behavior for featureless, unknown, or metrics-disabled runs.**
   If no additional feature activity is detected, feature detection fails, or adaptive-timeout metrics are not gathered under the current `adaptive_timeout_store_metrics` behavior, the resulting timeout values must remain identical to today's baseline formulas in `sdcm/utils/adaptive_timeouts/__init__.py:38-110`.

3. **Make timeout decisions explainable without changing the store API.**
   Stored adaptive-timeout results must record the active feature signals, the base budgets, the adjusted budgets, the `cpu_load_5` load signal, and the composed multiplier (feature factor, cpu scale, combined factor) inside the existing metrics payload so future failures can be diagnosed from telemetry alone without changing the `AdaptiveTimeoutStore.store()` signature.

4. **Provide deterministic regression coverage and rollout guidance.**
   Unit tests must cover individual features, composed features, cap behavior, and detection failure fallback, and the implementation must include documentation/default updates for any new configuration surface.

5. **Support graceful disable without code revert.**
   All feature-aware behavior must be controllable via configuration so operators can disable the entire feature-detection system without modifying code.

## Implementation Phases

**PR bundling:** Phases 1 and 2 ship together as the core-logic PR (detection + multipliers are not independently testable end-to-end). Phase 3 (telemetry schema + Argus) is a second PR. Phase 4 is validation/docs folded into the Phase 3 PR description. Phase 5 is a separate, validation-gated follow-up PR.

### Phase 1 -- Add feature-activity profile to NodeLoadInfoService

**Importance:** Critical

**Description**

Extend `NodeLoadInfoService` with a `ClusterFeatureProfile` derived from the existing `_get_scylla_metrics()` metrics path. This avoids a second resolver abstraction and preserves the current adaptive-timeout behavior: feature-aware scaling is available only when the same node metrics are already gathered for adaptive timeout calculations. The service reads feature-activity counters with `> 0` detection rules:

- **CDC:** `scylla_cdc_operations_total` (fallback: `scylla_cdc_operations_on_clustering_row_performed_total`). Note: both metrics are only emitted when CDC is enabled at the table level; metric absence means `cdc=False`.
- **LWT:** `scylla_storage_proxy_coordinator_cas_total_operations` (fallback: `scylla_storage_proxy_coordinator_cas_prune`). Both are cumulative counters. Note: `scylla_storage_proxy_coordinator_cas_foreground` is a gauge (in-flight operations) and must NOT be used for `> 0` boolean detection -- it reads 0 on active LWT nodes that have completed their operations.
- **MV/SI:** `scylla_database_total_view_updates_pushed_local` and `scylla_database_total_view_updates_pushed_remote` (SI is treated as part of the MV/view-update signal). These are universally present across all validated Scylla versions.
- **Counters:** `scylla_database_counter_cell_lock_acquisition` (fallback: `scylla_storage_proxy_replica_received_counter_updates`). Note: the fallback is version-dependent but detects activity on nodes where the primary shows 0.

Because these metrics are cumulative activity totals, detection is strictly boolean (`metric > 0` means activity was observed). `NodeLoadInfoService` stores per-feature booleans plus the raw detector values for telemetry only (the raw values are never used to scale the multiplier). The magnitude of the timeout uplift is determined later in Phase 2 from `cpu_load_5`, not from these counters.

A master switch `adaptive_timeout_feature_enabled` (boolean, default `true`) disables all feature-aware adjustments when set to `false`. When disabled, `adaptive_timeout` must not access `NodeLoadInfoService.feature_profile`.

**Implementation steps**

1. Introduce an immutable dataclass `ClusterFeatureProfile` in `sdcm/utils/adaptive_timeouts/load_info_store.py` with:
   - Boolean fields: `cdc`, `materialized_views`, `lwt`, `counters` (each `True` when its detector metric is `> 0`)
   - An `any_enabled` convenience property

   ```python
   @dataclass(frozen=True)
   class ClusterFeatureProfile:
       cdc: bool = False
       materialized_views: bool = False
       lwt: bool = False
       counters: bool = False
       detector_values: tuple[tuple[str, float], ...] = ()  # (metric_name, summed_value) pairs; telemetry only, not used for scaling
       # (feature_name, metric_name) pairs recording which metric triggered each active feature,
       # e.g. (("cdc", "scylla_cdc_operations_total"), ("lwt", "scylla_storage_proxy_coordinator_cas_prune")).
       # Telemetry/debugging only; never used for scaling.
       sources: tuple[tuple[str, str], ...] = ()

       @classmethod
       def empty(cls) -> "ClusterFeatureProfile":
           return cls()

       @property
       def any_enabled(self) -> bool:
           return self.cdc or self.materialized_views or self.lwt or self.counters

       @property
       def active_features(self) -> tuple[str, ...]:
           names = (("cdc", self.cdc), ("mv", self.materialized_views), ("lwt", self.lwt), ("counter", self.counters))
           return tuple(name for name, on in names if on)
   ```

2. Add a cached `feature_profile` property to `NodeLoadInfoService`. It must call the existing `_get_scylla_metrics()` method, which already uses `curl_with_retry()` through `NodeLoadInfoService._get_metrics()` and is cached with a 60-second TTL. Do not add a separate metrics-fetching implementation.

3. Implement metrics-based activity detection by evaluating feature metrics from `NodeLoadInfoService._get_scylla_metrics()`:
     - **MV/SI**: `scylla_database_total_view_updates_pushed_local + scylla_database_total_view_updates_pushed_remote > 0` (stored as `materialized_views=True`). Universally present.
     - **CDC**: `scylla_cdc_operations_total > 0` (fallback: `scylla_cdc_operations_on_clustering_row_performed_total > 0`). Metric absence (not zero, but not in TSDB) means `cdc=False`.
     - **LWT**: `scylla_storage_proxy_coordinator_cas_total_operations > 0` (fallback: `scylla_storage_proxy_coordinator_cas_prune > 0`). Both are cumulative counters. Do NOT use `cas_foreground` (it is a gauge and reads 0 on active LWT nodes).
     - **Counters**: `scylla_database_counter_cell_lock_acquisition > 0` (fallback: `scylla_storage_proxy_replica_received_counter_updates > 0`). Fallback is version-dependent but more sensitive.

   **Metrics collection strategy:**

   Collection happens via direct HTTP scrape of the node's `/metrics` endpoint (port 9180), using the existing `_get_scylla_metrics()` method (cached with 60s TTL). This is NOT a Prometheus server query -- it reads the raw exposition format from the Scylla process on the node being decommissioned. Tests typically run 2-48 hours, and the topology operation (decommission) is triggered at a specific point within that window. Feature detection is performed **once at the start of the topology operation** using the already-cached metrics dict, because:
   - Cumulative counters only grow monotonically; if a feature was active at any point since node boot, its counter is > 0 at decommission time
   - The 60s TTL cache means the metrics snapshot is at most 60 seconds stale, which is acceptable for boolean detection
   - No repeated polling is needed -- a single snapshot captures the full history of feature activity

   **Raw metrics format and aggregation:**

   The `_get_metrics()` parser (`load_info_store.py:115-128`) stores each exposition line as `{key: value}` where the key includes inline labels (e.g., `scylla_cdc_operations_total{shard="0",type="preimage"}`). To detect feature activity, iterate all keys matching a metric name prefix and sum their float values across all shards:

   ```python
   def _sum_metric(self, metrics: dict[str, str], prefix: str) -> float | None:
       """Sum all values for metrics matching prefix across shards.

       Returns None if the metric is entirely absent (prefix not found in any key),
       or the sum of all matching values (>= 0.0) when at least one key matches.
       """
       total = None
       for key, value in metrics.items():
           if key == prefix or key.startswith(prefix + "{"):
               if total is None:
                   total = 0.0
               total += float(value)
       return total
   ```

   **Note:** match on the metric-name boundary, not a bare `key.startswith(prefix)`. Exposition keys are formatted as `name{labels}` or bare `name`, so `key == prefix or key.startswith(prefix + "{")` avoids accidentally matching longer sibling metrics that share the same prefix (e.g. a `..._cas` prefix would otherwise also match `scylla_storage_proxy_coordinator_cas_total_operations`).

   Detection logic per feature:

   ```python
   metrics = self._get_scylla_metrics()

   # MV/SI -- universally present; sum across all shards
   mv_local = self._sum_metric(metrics, "scylla_database_total_view_updates_pushed_local")
   mv_remote = self._sum_metric(metrics, "scylla_database_total_view_updates_pushed_remote")
   materialized_views = (mv_local is not None and mv_local > 0) or (mv_remote is not None and mv_remote > 0)

   # CDC -- absent when no CDC tables exist
   cdc_primary = self._sum_metric(metrics, "scylla_cdc_operations_total")
   cdc_fallback = self._sum_metric(metrics, "scylla_cdc_operations_on_clustering_row_performed_total")
   cdc = (cdc_primary is not None and cdc_primary > 0) or (cdc_fallback is not None and cdc_fallback > 0)

   # LWT -- primary is version-dependent; fallback mirrors primary when both exist
   lwt_primary = self._sum_metric(metrics, "scylla_storage_proxy_coordinator_cas_total_operations")
   lwt_fallback = self._sum_metric(metrics, "scylla_storage_proxy_coordinator_cas_prune")
   lwt = (lwt_primary is not None and lwt_primary > 0) or (lwt_fallback is not None and lwt_fallback > 0)

   # Counters -- primary is universal; fallback is version-dependent but more sensitive
   ctr_primary = self._sum_metric(metrics, "scylla_database_counter_cell_lock_acquisition")
   ctr_fallback = self._sum_metric(metrics, "scylla_storage_proxy_replica_received_counter_updates")
   counters = (ctr_primary is not None and ctr_primary > 0) or (ctr_fallback is not None and ctr_fallback > 0)
   ```

   **Why this works for 2-48 hour tests:**
   - SCT provisions fresh clusters for each test run (counters start at 0 on boot)
   - Stress workload begins before topology operations, so by the time decommission starts, cumulative counters reflect all feature activity since test start
   - For reused clusters (`SCT_REUSE_CLUSTER`), counters persist from previous runs; this is correct behavior because the feature infrastructure (CDC logs, MV definitions, Paxos state) also persists and will slow the topology operation regardless of when it was created
   - A single instant scrape at decommission start captures the complete activity history

   **Timing within test lifecycle:**
   ```
   Test start -> Cluster provision -> Stress begins -> [2-48h workload] -> Topology op triggered
                                                                            ^
                                                              Feature detection happens HERE
                                                              (single scrape of /metrics on target node)
   ```

4. Add configuration parameters to `sdcm/sct_config.py`, following the existing `adaptive_timeout_multipliers` convention (`sdcm/sct_config.py:306-348`):
   - `adaptive_timeout_feature_enabled`: `Boolean`, default `true`. Master switch to disable all feature-aware adjustments.
   - `adaptive_timeout_feature_factors`: a new `AdaptiveTimeoutFeatureFactors` pydantic `RootModel` mirroring `AdaptiveTimeoutMultipliers` -- `root: dict[str, confloat(gt=0)]` wired as `Annotated[AdaptiveTimeoutFeatureFactors, BeforeValidator(dict_or_str_or_pydantic)]`, default `{}`. A `@model_validator(mode="before")` must **raise `ValueError`** on any key outside `{"cdc", "mv", "lwt", "counter"}` (matching how `_validate_operations` rejects unknown operation keys -- silently ignoring typos like `mvs: 2.0` would hide a misconfiguration). When provided, each key overrides the internal per-feature base factor; missing keys fall back to the internal default. Provide a `get_factor(feature: str, default: float) -> float` helper analogous to `get_multiplier`. This gives YAML, string, and dot-notation env-var parsing (`SCT_ADAPTIVE_TIMEOUT_FEATURE_FACTORS.mv=2.0`) for free.
   - `adaptive_timeout_max_combined_factor`: `Annotated[confloat(ge=1.0) | None, BeforeValidator(dict_or_str)]`, default `null` (uses internal constant `3.0`). When provided, overrides `MAX_COMBINED_FACTOR`. The `confloat(ge=1.0)` bound enforces `>= 1.0` at config-parse time (a plain `float` `SctField` would not).
   All must be documented in `docs/configuration_options.md` and have defaults in `defaults/test_default.yaml` (`adaptive_timeout_feature_enabled: true`, `adaptive_timeout_feature_factors: {}`, `adaptive_timeout_max_combined_factor: null`).

5. Caching strategy: rely on the existing `_get_scylla_metrics()` TTL cache (60s) and avoid adding a second cache layer.

6. Feature-profile failures (metrics fetch errors, parse errors, unavailable node metrics) must not fail the topology operation. Log the error and fall back to `ClusterFeatureProfile.empty()`.

7. Add unit tests for the `NodeLoadInfoService.feature_profile` property covering:
   - Metrics detection with mocked Prometheus payload detects MV/SI activity as `materialized_views=True`
   - Metrics detection with mocked `scylla_cdc_operations_total` detects CDC
   - Metrics detection with mocked `scylla_storage_proxy_coordinator_cas_total_operations` detects LWT
   - Metrics detection with mocked `scylla_storage_proxy_coordinator_cas_prune` detects LWT as fallback when `cas_total_operations` is absent
   - Metrics detection confirms `scylla_storage_proxy_coordinator_cas_foreground` is NOT used (gauge, unreliable)
   - Metrics detection with mocked `scylla_database_counter_cell_lock_acquisition` detects counters
   - Metrics detection with mocked `scylla_storage_proxy_replica_received_counter_updates` detects counters when primary is 0
   - No feature metrics (or all zeros) returns empty profile
   - Metric absence (metric not in payload at all) returns `False` for that feature (distinct from value=0)
   - Fallback metrics are used when primary metrics are missing
   - `detector_values` is populated with the `(metric_name, summed_value)` pairs that were evaluated, so the raw metric value behind each decision is recoverable from telemetry (assert the tuple contents, not just the boolean flags)
   - `sources` is populated with the `(feature_name, metric_name)` pair that triggered each active feature, distinguishing primary vs. fallback (e.g. `("lwt", "scylla_storage_proxy_coordinator_cas_prune")` when the fallback fired); assert it records the fallback metric name, not the primary, when only the fallback is present
   - Feature-profile failure (metrics fetch exception) falls back to `ClusterFeatureProfile.empty()` with empty `detector_values` and `sources`
   - `adaptive_timeout_feature_enabled=false` does not access `feature_profile`
   - Existing `_get_scylla_metrics()` TTL cache is reused

**Dependencies:** None

**Deliverables**

- `ClusterFeatureProfile` and `NodeLoadInfoService.feature_profile` in `sdcm/utils/adaptive_timeouts/load_info_store.py`
- New `AdaptiveTimeoutFeatureFactors` `RootModel` (mirroring `AdaptiveTimeoutMultipliers`) and config parameters (`adaptive_timeout_feature_enabled`, `adaptive_timeout_feature_factors`, `adaptive_timeout_max_combined_factor`) in `sdcm/sct_config.py`
- Default values in `defaults/test_default.yaml`
- Import update in `sdcm/utils/adaptive_timeouts/__init__.py` to use `ClusterFeatureProfile`
- Unit tests in `unit_tests/unit/test_adaptive_timeouts.py` or `unit_tests/unit/test_load_info_store.py`

**Definition of Done**

- [ ] `adaptive_timeout` can obtain a feature-activity profile from `NodeLoadInfoService` when current adaptive-timeout metrics gathering is enabled.
- [ ] No standalone feature resolver or separate metrics-fetch path is introduced.
- [ ] Metrics detection auto-detects MV/SI, CDC, LWT, and counter activity through `_get_scylla_metrics()`.
- [ ] CDC detection uses `scylla_cdc_operations_total` (with fallback `scylla_cdc_operations_on_clustering_row_performed_total`). Metric absence (not in payload) correctly yields `cdc=False`.
- [ ] LWT detection uses cumulative CAS counters: `scylla_storage_proxy_coordinator_cas_total_operations` primary, `cas_prune` fallback. `cas_foreground` is explicitly excluded (gauge, unreliable for boolean detection).
- [ ] Counter detection uses `scylla_database_counter_cell_lock_acquisition` primary, `scylla_storage_proxy_replica_received_counter_updates` fallback (version-dependent, more sensitive).
- [ ] Feature-profile failures do not fail the operation; they log context and fall back to a no-feature profile.
- [ ] `adaptive_timeout_feature_enabled: false` disables all feature-aware adjustments and short-circuits before reading `NodeLoadInfoService.feature_profile`.
- [ ] New config fields (`adaptive_timeout_feature_enabled`, `adaptive_timeout_feature_factors`, `adaptive_timeout_max_combined_factor`) are defined in `sdcm/sct_config.py`, documented in `docs/configuration_options.md`, and have defaults in `defaults/test_default.yaml`.
- [ ] Zero-token and K8s decommission paths preserve the same metrics-availability behavior as the current adaptive-timeout implementation.
- [ ] Unit tests verify the `detector_values` (raw `(metric_name, summed_value)` pairs) and `sources` (`(feature_name, metric_name)` triggering pairs) telemetry fields are populated correctly, not only the boolean flags.
- [ ] Unit tests pass for metrics detection, fallback behavior, metric-absence handling, disabled short-circuit behavior, and cache reuse.

---

### Phase 2 -- Apply feature-aware multipliers to decommission budgets

**Importance:** Critical

**Description**

Keep the current decommission timeout formula as the baseline and layer feature-aware scaling on top of it. The implementation should make the original formula easy to reason about, so baseline behavior remains stable and feature-aware behavior is clearly attributable to explicit adjustments. This phase must not change `NEW_NODE` or `TABLET_MIGRATION`; those operations stay on current baseline behavior until Phase 5 after decommission is validated with real tests. Feature base factors and the cpu-scale cap have internal-constant defaults, but they are overridable per test via the `adaptive_timeout_feature_factors` and `adaptive_timeout_max_combined_factor` config parameters defined in Phase 1.

The feature-aware `combined_factor` composes multiplicatively with the existing per-operation `operation_factor` (`adaptive_timeout_multipliers`, applied at `sdcm/utils/adaptive_timeouts/__init__.py:279-282`). The final budget is `math.ceil(base_timeout * operation_factor * combined_factor)`. The feature layer's own cap (`MAX_COMBINED_FACTOR`) bounds only `combined_factor`; the pre-existing `operation_factor` is intentionally uncapped and unchanged, so a test that already sets `adaptive_timeout_multipliers` keeps that behavior and additionally receives feature scaling.

The multiplier has two parts: **active feature signals gate and select a feature factor** (a product of per-feature constants), and the decommission node's **`cpu_load_5` per shard modulates the magnitude** of that factor. When no feature activity is detected, when metrics are unavailable, or when current adaptive-timeout metrics gathering is disabled, the combined factor is exactly `1.0`, preserving baseline behavior. When feature activity is detected, a busier decommission node (higher `cpu_load_5 / shards_count`) yields a larger budget than an idle one. The same combined factor scales both the soft and hard timeouts.

**Implementation steps**

1. Introduce a structured timeout result dataclass `AdaptiveTimeoutBudget`:

   ```python
   @dataclass
   class AdaptiveTimeoutBudget:
       base_soft_timeout: float
       base_hard_timeout: float | None
       adjusted_soft_timeout: float
       adjusted_hard_timeout: float | None
       feature_profile: ClusterFeatureProfile
       cpu_load_5: float
       cpu_scale: float        # 1.0..CPU_SCALE_CAP, 1.0 for idle node
       feature_factor: float   # 1.0 when no features
       combined_factor: float  # 1.0 when no features; applied to both soft and hard
   ```

2. Define per-feature base factors and scaling bounds as internal module-level constants. These serve as defaults; per-test overrides are available via `adaptive_timeout_feature_factors` and `adaptive_timeout_max_combined_factor` config parameters (Phase 1, step 4). Feature detection is boolean, so each active feature signal contributes a fixed base factor; the cumulative-counter metrics are never used for proportional scaling:

| Feature | Base factor (when active) | Rationale |
|---------|---------------------------|-----------|
| CDC | 1.3 | CDC log streaming adds overhead to topology changes |
| Materialized views / secondary indexes | 1.5 | MV/view-update signal covers both MV and SI workloads in v1 |
| LWT | 1.2 | Paxos state migration adds modest overhead |
| Counters | 1.2 | Counter lock/update coordination adds write-path overhead |

3. Composition rule: multiply the base factors of all active feature signals to get the feature factor, derive the cpu scale from `cpu_load_5 / shards_count` (floored at `1.0`, capped at `CPU_SCALE_CAP`), multiply them, and cap at `MAX_COMBINED_FACTOR`. Featureless runs short-circuit to `1.0` so the baseline is untouched.

   ```python
   _FEATURE_BASE_FACTOR = {"cdc": 1.3, "mv": 1.5, "lwt": 1.2, "counter": 1.2}
   CPU_SCALE_CAP = 2.0        # a busy node may at most double the feature uplift
   MAX_COMBINED_FACTOR = 3.0  # never more than 3x baseline

   def _cpu_scale(cpu_load_5: float, shards_count: int) -> float:
       """Load per shard, floored at 1.0 (idle node keeps feature factor) and capped."""
       load_per_shard = cpu_load_5 / max(shards_count, 1)
       return min(max(load_per_shard, 1.0), CPU_SCALE_CAP)

   def compose_factor(profile: ClusterFeatureProfile, cpu_load_5: float, shards_count: int,
                      factor_overrides: dict | None = None, max_combined_override: float | None = None
                      ) -> tuple[float, float, float]:
       """Return (feature_factor, cpu_scale, combined_factor).

       factor_overrides: per-feature base factor overrides from config (adaptive_timeout_feature_factors).
       max_combined_override: override for MAX_COMBINED_FACTOR from config (adaptive_timeout_max_combined_factor).
       """
       active_features = profile.active_features
       if not active_features:
           return 1.0, 1.0, 1.0  # featureless -> baseline preserved (Goal 2)
       effective_factors = {**_FEATURE_BASE_FACTOR, **(factor_overrides or {})}
       effective_max = max_combined_override if max_combined_override is not None else MAX_COMBINED_FACTOR
       feature_factor = 1.0
       for feature in active_features:
           feature_factor *= effective_factors.get(feature, 1.0)
       cpu_scale = _cpu_scale(cpu_load_5, shards_count)
       # Reaching this point means at least one feature is active, so feature_factor >= 1.2
       # (smallest single-feature factor) and cpu_scale >= 1.0; combined is therefore always
       # >= 1.2. No `max(combined, 1.0)` floor is applied because it would be dead code --
       # the featureless case already returned (1.0, 1.0, 1.0) above.
       combined = min(feature_factor * cpu_scale, effective_max)
       return feature_factor, cpu_scale, combined
   ```

   Adjusted timeout values are computed with `math.ceil(base_timeout * operation_factor * combined_factor)`, not `round()`, so an active feature signal cannot round back down to the baseline (`operation_factor` is the pre-existing `adaptive_timeout_multipliers` value, `1.0` by default). `cpu_load_5`, `shards_count`, and `feature_profile` come from the decommission node's existing `NodeLoadInfoService` (`load_info_store.py:165-171`), which is already gathered when adaptive-timeout metrics collection is enabled. This preserves current behavior: if metrics collection is disabled or metrics fetching fails, feature-aware scaling falls back to the baseline (`combined_factor=1.0`) rather than introducing a separate metrics-fetch path.

   Operation-node mapping for this phase is decommission-only:
   - `DECOMMISSION`: the node being decommissioned (`sdcm/cluster.py:6456`, `sdcm/cluster_k8s/__init__.py:3178`)
   - `NEW_NODE`: no feature-aware scaling in this phase; keep current behavior
   - `TABLET_MIGRATION`: no feature-aware scaling in this phase; keep current behavior

4. Refactor the topology timeout calculators in `sdcm/utils/adaptive_timeouts/__init__.py:38-110` so they first compute the current baseline, then apply feature multipliers in a separate step. The baseline functions (`_get_decommission_timeout`, etc.) remain unchanged. The feature multiplier is applied to the *output* of the baseline calculator (the final soft/hard timeout values), not to the formula inputs. This means:
   - For `DECOMMISSION` (this phase): the baseline is always formula-derived (`node_data_size_mb`-based), so the multiplier scales the formula result.
   - For `NEW_NODE` and `TABLET_MIGRATION` (Phase 5 only): when the baseline calculator returns `caller_timeout` directly (e.g., non-tablet `NEW_NODE`), the feature multiplier will also scale that `caller_timeout` output. This ensures user-supplied hints benefit from feature-aware scaling.

5. Scope feature-aware adjustment to `Operations.DECOMMISSION` only. `Operations.NEW_NODE` and `Operations.TABLET_MIGRATION` must continue to use existing baseline behavior in this phase. `sdcm/utils/adaptive_timeouts/__init__.py:146-189`

6. Preserve these invariants:
   - Adjusted values never shrink below baseline
   - `adjusted_hard_timeout >= adjusted_soft_timeout` whenever a hard timeout exists
   - Non-tablet operations that currently lack a hard timeout do not gain one

7. Update `adaptive_timeout` context manager to:
   - Evaluation order: check `store_metrics` (the existing `adaptive_timeout_store_metrics and node_available` gate) **first**; only if metrics are gathered does `adaptive_timeout_feature_enabled` act as the secondary guard before reading `feature_profile`. This ensures the many perf test-cases that already set `adaptive_timeout_store_metrics: false` never enter the feature path.
   - Source the inputs from the data already gathered when `store_metrics` is true: `cpu_load_5` and `shards_count` are present in the `load_metrics` dict (populated via `node_info_service.as_dict()` at `sdcm/utils/adaptive_timeouts/load_info_store.py:228-242`), so read them from `load_metrics` rather than re-touching the service. Obtain `feature_profile` from the same `NodeLoadInfoService` instance -- note this instance is held in the confusingly-named local variable `metrics` in `adaptive_timeout` (`sdcm/utils/adaptive_timeouts/__init__.py:262-266`), which is `{}` when `store_metrics` is false. Guard accordingly so feature access only happens on a real service instance.
   - Timing: all metric collection (including `feature_profile`) happens at `adaptive_timeout` **entry** -- before the decommission/bootstrap command starts streaming -- so the target node is still fully available. It is never re-scraped mid-operation when the node may be partially drained; an entry-time scrape failure falls back to the baseline (`combined_factor=1.0`).
   - Compute `feature_factor`, `cpu_scale`, and `combined_factor` via `compose_factor` (passing `factor_overrides` from `adaptive_timeout_feature_factors` and `max_combined_override` from `adaptive_timeout_max_combined_factor`), then build the `AdaptiveTimeoutBudget`
   - Composition with the existing per-operation multiplier: the budget's `combined_factor` is applied **in addition to** the `operation_factor` already computed at `sdcm/utils/adaptive_timeouts/__init__.py:279`. Apply both in one `math.ceil(base_timeout * operation_factor * combined_factor)` so the existing `adaptive_timeout_multipliers` behavior is preserved and feature scaling stacks on top. Do not drop or duplicate the existing `soft_timeout * operation_factor` line.
   - Use `math.ceil()` when applying the factors to base soft/hard timeouts
   - Use adjusted values for monitoring and yield
   - Log the budget at DEBUG level before execution begins

8. Add structured logging around the final budget calculation:
   ```
   DEBUG: Adaptive timeout budget for DECOMMISSION: base_soft=7200s, adjusted_soft=14040s,
          features=[cdc,mv], feature_factor=1.95, cpu_load_5=12.0, shards=8, cpu_scale=1.5,
          combined_factor=2.925, operation_factor=1.0, cap=3.0
   ```

9. Add unit tests for multiplier logic:
   - No-feature identity (combined_factor = 1.0, adjusted == baseline regardless of `cpu_load_5`)
   - Each feature individually increases adjusted values (idle node, `cpu_scale=1.0`)
   - `cpu_scale` floor: idle node (`cpu_load_5 / shards <= 1.0`) keeps `combined_factor == feature_factor`
   - `cpu_scale` lift: busy node (`cpu_load_5 / shards > 1.0`) increases `combined_factor` up to `CPU_SCALE_CAP`
   - `cpu_scale` cap enforcement (e.g., very high load still capped at 2.0)
   - Combined features multiply (e.g., CDC + MV = 1.3 * 1.5 = 1.95x at `cpu_scale=1.0`)
   - `math.ceil()` prevents active feature adjustments from rounding down to baseline
   - Missing `cpu_load_5`, missing `shards_count`, disabled metrics collection, or metrics fetch failure returns baseline unchanged
   - Global cap enforcement at `MAX_COMBINED_FACTOR=3.0`
   - Both soft and hard timeouts scale by the same `combined_factor`
   - Composition with the pre-existing `operation_factor`: a non-default `adaptive_timeout_multipliers` (e.g. `{decommission: 2}`) and an active feature stack multiplicatively -- final budget is `ceil(base * operation_factor * combined_factor)` -- and the existing `operation_factor` line is preserved (not dropped or double-applied)
   - `adaptive_timeout_feature_enabled=false` bypasses all feature adjustment

**Needs Investigation**

- The initial feature base factors and `CPU_SCALE_CAP` are estimates. After deploying Phase 2 with telemetry, tune them against real Argus data by correlating actual `duration / base_timeout` ratios with the recorded `cpu_load_5` and active feature-signal set. Plan to revisit after 2-4 weeks of production data collection.
- The `cpu_load_5 / shards_count` normalization assumes shard count approximates available parallelism; verify this holds for nodes where streaming is shard-bound rather than CPU-bound, and adjust the cpu-scale shape if telemetry shows weak correlation.
- User-facing feature factor and max-combined-factor overrides are provided via `adaptive_timeout_feature_factors` and `adaptive_timeout_max_combined_factor` config parameters (Phase 1, step 4). After deploying with telemetry, evaluate whether the default factors need permanent adjustment based on Argus data.

**Dependencies:** Phase 1

**Deliverables**

- Add `import math` to `sdcm/utils/adaptive_timeouts/__init__.py` (currently absent) for the `math.ceil()` rounding of adjusted budgets
- Feature base-factor constants, `_cpu_scale`, and `compose_factor` composition logic in `sdcm/utils/adaptive_timeouts/__init__.py`
- `AdaptiveTimeoutBudget` dataclass
- Refactored `adaptive_timeout` context manager using the budget and decommission-node `cpu_load_5`
- Unit tests in `unit_tests/unit/test_adaptive_timeouts.py` (extended) covering multiplier behavior

**Definition of Done**

- [ ] Featureless, metrics-disabled, and metrics-failure runs return exactly the same timeout values as current SCT, regardless of `cpu_load_5`.
- [ ] Each supported feature increases the adjusted soft timeout for decommission.
- [ ] A busier decommission node (`cpu_load_5 / shards_count > 1.0`) produces a larger adjusted budget than an idle one for the same feature set, bounded by `CPU_SCALE_CAP`.
- [ ] Existing decommission hard-timeout paths keep hard timeouts and scale by the same combined factor; operations without hard timeouts do not gain new hard timeouts.
- [ ] `Operations.NEW_NODE` and `Operations.TABLET_MIGRATION` remain unchanged from current behavior in this phase.
- [ ] Combined-feature budgets are capped at `MAX_COMBINED_FACTOR` and remain monotonic relative to the baseline.
- [ ] The feature set, `cpu_load_5`, cpu scale, combined factor, and adjusted values computed with `ceil()` are visible in operation logs.
- [ ] `adaptive_timeout_feature_enabled=false` disables all feature-aware adjustments (rollback switch).
- [ ] Unit tests pass for identity, individual features, cpu-scale floor/lift/cap, composition, global cap, and rollback behavior.

---

### Phase 3 -- Expand telemetry schema and Argus integration

**Importance:** Important

**Description**

Make feature-aware timeout behavior observable in stored telemetry so post-failure analysis can determine why a specific budget was chosen without re-running the test. Keep the initial telemetry expansion lean: `base_timeout`, `adjusted_timeout`, `active_features`, the `cpu_load_5` load signal (already stored), and the multiplier breakdown (`feature_factor`, `cpu_scale`, `combined_factor`), plus the legacy `timeout` field for backward compatibility. Do not change the `AdaptiveTimeoutStore.store()` signature; all new fields are added to the existing `metrics` payload before calling `store()`.

**Implementation steps**

1. Keep the `AdaptiveTimeoutStore.store()` interface unchanged in `sdcm/utils/adaptive_timeouts/load_info_store.py:245-253`:

   ```python
   def store(self, metrics: dict[str, Any], operation: str, duration: int | float, timeout: int,
             timeout_occurred: bool) -> None:
       ...
   ```

   Add new telemetry fields to the `metrics` dict in `adaptive_timeout` before calling `stats_storage.store()`. This preserves existing store implementations and test doubles.

2. Add these keys to `load_metrics` when a feature-aware budget is computed:
   - `active_features: str` (comma-separated list from `ClusterFeatureProfile.active_features`, e.g. `"cdc,mv"`)
   - `base_timeout: int` (base soft timeout)
   - `adjusted_timeout: int` (adjusted soft timeout)
   - `feature_factor: float`
   - `cpu_scale: float`
   - `combined_factor: float`

   `cpu_load_5` is already part of stored node metrics when metrics are gathered and needs no new key.

3. Expand `AdaptiveTimeoutResultsTable` in `sdcm/utils/adaptive_timeouts/load_info_store.py:271-290` with new columns:
   - `base_timeout` (DURATION, visible=False)
   - `adjusted_timeout` (DURATION)
   - `active_features` (TEXT)
   - `feature_factor` (FLOAT, visible=False)
   - `cpu_scale` (FLOAT, visible=False)
   - `combined_factor` (FLOAT)

4. Preserve backward compatibility: keep the legacy `timeout` field populated with `adjusted_timeout` so existing dashboards continue to work.

5. Update `ArgusAdaptiveTimeoutStore.send_adaptive_timeout_results_to_argus()` to read the new values from `result.metrics`, matching the existing pattern for `cpu_load_5`, `shards_count`, and disk/throughput metrics.

6. Add unit tests validating that stored payloads contain the new fields without changing `MemoryAdaptiveTimeoutStore.store()` or the `AdaptiveTimeoutStore.store()` signature.

7. **Argus CLI querying:** Timeout telemetry (active features, base/adjusted budgets, combined factor) should be extractable using the Argus CLI. Reference: [ARGUS-176](https://scylladb.atlassian.net/browse/ARGUS-176).

**Dependencies:** Phase 2

**Deliverables**

- Expanded Argus result model with unchanged store interface
- Backward-compatible telemetry with new feature-aware fields
- Unit tests for telemetry payload contents

**Definition of Done**

- [ ] Stored adaptive-timeout results include active feature signals, base budget, adjusted budget, `cpu_load_5`, and the multiplier breakdown (`feature_factor`, `cpu_scale`, `combined_factor`).
- [ ] The legacy `timeout` field remains available and populated with `adjusted_timeout`.
- [ ] `AdaptiveTimeoutStore.store()` and existing test store signatures remain unchanged.
- [ ] Argus table renders new columns without breaking existing dashboards.
- [ ] Unit tests validate telemetry payload schema.

---

### Phase 4 -- Documentation and decommission validation

**Importance:** Important

**Description**

Finalize configuration documentation and validate the decommission-only rollout with real tests before enabling the same feature-aware scaling for any other topology operation.

**Implementation steps**

1. Update `docs/configuration_options.md` for the new adaptive-timeout parameters:
   - `adaptive_timeout_feature_enabled` (boolean, default `true`)
   - `adaptive_timeout_feature_factors` (dict, default `{}`)
   - `adaptive_timeout_max_combined_factor` (float, default `null`)

2. Verify defaults in `defaults/test_default.yaml` are present and safe.

3. Run `uv run sct.py pre-commit` to validate formatting and config-doc consistency.

4. Add a component-level integration test (if feasible with docker backend) that:
    - Creates a table with a secondary index
    - Triggers adaptive_timeout resolution in metrics mode
    - Verifies the adjusted decommission timeout is larger than baseline

5. Run at least one real VM decommission validation and, if K8s environment is available, one real K8s rack-shrink validation with feature activity enabled before broadening scope.

6. Document recommended manual validation scenarios (see Testing Requirements below) in the implementation PR description, including observed `duration / base_timeout`, `cpu_load_5`, `active_features`, and `combined_factor`.

**Dependencies:** Phase 3

**Deliverables**

- Updated configuration documentation
- Verified defaults
- Optional integration test
- PR description with manual validation checklist and decommission validation results

**Definition of Done**

- [ ] `uv run sct.py pre-commit` passes.
- [ ] Configuration docs describe the master switch, feature factor overrides, and max combined factor override with type, default, valid values, and examples.
- [ ] At least one manual feature-heavy decommission validation performed before merge.
- [ ] Decommission validation results are captured in the PR and used as the gate for Phase 5.

---

### Phase 5 -- Extend feature-aware scaling to new-node and tablet-migration operations after validation

**Importance:** Important

**Description**

After the decommission-only implementation is validated with real tests, apply the same feature-aware budget calculation to `Operations.NEW_NODE` and `Operations.TABLET_MIGRATION`. This phase is explicitly gated on Phase 4 validation: do not enable these operations in the initial PR. The follow-up must reuse the Phase 2 multiplier code and preserve current behavior when metrics are unavailable, disabled, or feature activity is absent.

**Implementation steps**

1. Review Phase 4 decommission validation results and confirm that:
   - Feature-heavy decommission no longer fails early due to too-small SCT timeouts.
   - Featureless and metrics-disabled decommission results remain baseline-compatible.
   - Recorded `combined_factor` and `cpu_load_5` values are within expected bounds.

2. Extend feature-aware adjustment scope from `Operations.DECOMMISSION` to include `Operations.NEW_NODE` and `Operations.TABLET_MIGRATION`. `sdcm/utils/adaptive_timeouts/__init__.py:146-189`

3. Preserve current operation-node mapping from existing call sites:
   - `NEW_NODE`: the existing cluster node currently passed by callers (`sdcm/nemesis/__init__.py:1244`, `sdcm/nemesis/__init__.py:1291`)
   - `TABLET_MIGRATION`: the node passed by the tablet migration caller (`sdcm/utils/tablets/common.py:47`)

4. Keep execution-time behavior unchanged for `NEW_NODE` and `TABLET_MIGRATION`; this phase adjusts monitoring/yielded adaptive budgets only unless a concrete command-timeout gap is identified during implementation.

5. Add regression tests proving:
   - `NEW_NODE` and `TABLET_MIGRATION` remain baseline when no feature activity is detected.
   - `NEW_NODE` and `TABLET_MIGRATION` receive increased adjusted budgets when feature activity is detected and metrics are available.
   - Metrics-disabled and metrics-failure paths remain baseline.
   - Existing hard-timeout behavior is preserved.

**Dependencies:** Phase 4 validation results

**Deliverables**

- Updated scope in `adaptive_timeout` to include `Operations.NEW_NODE` and `Operations.TABLET_MIGRATION`
- Unit tests covering new-node and tablet-migration feature-aware budgets
- Follow-up PR notes linking to Phase 4 decommission validation results

**Definition of Done**

- [ ] Phase 4 real decommission validation results are available and reviewed before this phase is implemented.
- [ ] `NEW_NODE` and `TABLET_MIGRATION` use the same feature-aware multiplier logic as decommission.
- [ ] `NEW_NODE` and `TABLET_MIGRATION` retain baseline behavior when no feature activity is detected, metrics are disabled, or metrics fail.
- [ ] Execution-time behavior for `NEW_NODE` and `TABLET_MIGRATION` is unchanged unless separately justified.
- [ ] Unit tests cover both operations for feature-active and baseline fallback scenarios.

## Testing Requirements

### Unit tests (Phases 1-3 delivered with the decommission rollout; Phase 5 delivered separately)

- **Phase 1 tests** (`unit_tests/unit/test_adaptive_timeouts.py` or `unit_tests/unit/test_load_info_store.py`):
  - Metrics detection: mocked Prometheus payload with view update counters detects MV/SI activity as `materialized_views=True`
  - Metrics detection: mocked `scylla_cdc_operations_total` detects CDC
  - Metrics detection: mocked `scylla_cdc_operations_on_clustering_row_performed_total` detects CDC when primary is absent
  - Metrics detection: mocked `scylla_storage_proxy_coordinator_cas_total_operations` detects LWT (cumulative counter)
  - Metrics detection: mocked `scylla_storage_proxy_coordinator_cas_prune` detects LWT as fallback when `cas_total_operations` is absent
  - Metrics detection: `scylla_storage_proxy_coordinator_cas_foreground` is NOT used for detection (gauge, reads 0 on active LWT nodes)
  - Metrics detection: mocked `scylla_database_counter_cell_lock_acquisition` detects counters
  - Metrics detection: mocked `scylla_storage_proxy_replica_received_counter_updates` detects counters when primary is 0
  - Metrics detection: no feature activity (all zeros) returns empty profile
  - Metrics detection: metric absence (not in payload at all, distinct from value=0) returns `False` for that feature
  - Metrics detection: fallback metrics are used when primary metrics are missing
  - Telemetry field `detector_values`: assert it holds the `(metric_name, summed_value)` pairs evaluated, so the raw value behind each decision (e.g. the summed CAS count) is recoverable, not just the boolean flag
  - Telemetry field `sources`: assert it holds the `(feature_name, metric_name)` pair that triggered each active feature, recording the fallback metric name (not the primary) when only the fallback fired
  - Feature-profile failure (metrics fetch exception) falls back to `ClusterFeatureProfile.empty()` with empty `detector_values` and `sources`
  - `adaptive_timeout_feature_enabled=false` bypasses `NodeLoadInfoService.feature_profile`
  - Existing `_get_scylla_metrics()` TTL cache is reused

- **Phase 2 tests** (extend `unit_tests/unit/test_adaptive_timeouts.py`):
  - Baseline decommission unchanged when no features detected
  - Baseline decommission unchanged when `adaptive_timeout_store_metrics=false` under the current metrics-gathering behavior
  - `NEW_NODE` and `TABLET_MIGRATION` remain unchanged during the decommission-only rollout
  - Each feature in `{cdc, mv, lwt, counter}` increases decommission adjusted budgets relative to baseline (idle node, `cpu_scale=1.0`)
  - cpu-scale floor: idle node (`cpu_load_5 / shards <= 1.0`) keeps `combined_factor == feature_factor`
  - cpu-scale lift: busy node (`cpu_load_5 / shards > 1.0`) increases `combined_factor`, capped at `CPU_SCALE_CAP=2.0`
  - Combined-feature multiplier composition (e.g., CDC + MV = 1.3 * 1.5 = 1.95x at `cpu_scale=1.0`)
  - `ceil()` rounding guarantees adjusted timeout is greater than baseline when any feature signal is active
  - Both soft and hard timeouts scale by the same `combined_factor`
  - Global cap enforcement at `MAX_COMBINED_FACTOR=3.0`
  - `adaptive_timeout_feature_enabled=false` returns baseline unchanged
  - `adaptive_timeout_feature_factors` override replaces internal defaults for specified keys (e.g., `{"mv": 2.0}` raises the MV factor while others stay at internal defaults)
  - `adaptive_timeout_max_combined_factor` override raises or lowers the combined cap (e.g., `5.0` allows combined factor beyond the default `3.0`)
  - Invalid keys in `adaptive_timeout_feature_factors` raise `ValueError` at config-parse time (matching `AdaptiveTimeoutMultipliers._validate_operations`)
  - `adaptive_timeout_max_combined_factor < 1.0` is rejected by the `confloat(ge=1.0)` bound
  - A non-default `adaptive_timeout_multipliers` (`operation_factor`) composes multiplicatively with an active feature `combined_factor`, and the existing `operation_factor` behavior is preserved

- **Phase 3 tests** (extend existing test module):
  - Stored metrics payload contains `active_features`, `base_timeout`, `adjusted_timeout`, `feature_factor`, `cpu_scale`, `combined_factor`
  - Stored payload retains `cpu_load_5` from node metrics
  - Legacy `timeout` field == `adjusted_timeout`
  - `AdaptiveTimeoutStore.store()` signature remains unchanged and existing `MemoryAdaptiveTimeoutStore` test double does not require new positional/keyword parameters

- **Phase 5 tests** (follow-up PR only):
  - Baseline `NEW_NODE` and `TABLET_MIGRATION` unchanged when no features detected
  - Baseline `NEW_NODE` and `TABLET_MIGRATION` unchanged when `adaptive_timeout_store_metrics=false`
  - Each feature in `{cdc, mv, lwt, counter}` increases adjusted budgets for `NEW_NODE` and `TABLET_MIGRATION` when metrics are available
  - Existing hard-timeout behavior is preserved for both operations

### Integration / component tests

- Add a small component-level test around `NodeLoadInfoService.feature_profile` so metrics-mode MV/SI/CDC/LWT/Counter detection can be validated without a full end-to-end longevity run.
- If a lightweight integration test is feasible with docker backend, exercise one decommission flow with feature activity detected and verify the computed soft and hard timeouts are larger than the featureless baseline.
- Do not add `NEW_NODE` or `TABLET_MIGRATION` integration coverage until Phase 5; Phase 5 tests should link back to Phase 4 decommission validation results.

### Manual validation

1. **Featureless VM decommission**
   Run a control scenario with `adaptive_timeout_feature_enabled: false` and verify that the resulting timeout matches the current baseline formulas from `sdcm/utils/adaptive_timeouts/__init__.py:38-110`.

2. **Metrics-mode MV/SI/CDC/Counter/LWT decommission**
   With `adaptive_timeout_feature_enabled: true` (default) and existing adaptive-timeout metrics gathering enabled, run workloads that exercise views/indexes, CDC, counters, and LWT before decommission, then verify the adjusted budget reflects detected metrics activity.

3. **K8s rack shrink with feature-aware budget**
   Run a feature-heavy K8s decommission and confirm the adjusted soft and hard timeouts reflect the detected feature activity, and that the hard-timeout monitor does not fire prematurely while the rack shrink is still progressing. `sdcm/cluster_k8s/__init__.py:3177-3186`

4. **Telemetry inspection**
   Confirm Argus/store output includes active feature signals, `cpu_load_5`, the multiplier breakdown (`feature_factor`, `cpu_scale`, `combined_factor`), base budget, and adjusted budget. `sdcm/utils/adaptive_timeouts/load_info_store.py:271-383`

### Quality gates

- `uv run sct.py pre-commit`
- All unit tests pass (each phase adds its own)
- At least one manual feature-heavy decommission validation before broadening to Phase 5

## Success Criteria

- For identical decommission node metrics, detecting any supported feature signal increases the adjusted decommission timeout relative to the baseline calculation, with the magnitude scaling by the decommission node's `cpu_load_5` per shard and final values rounded with `ceil()`.
- During the initial rollout, `NEW_NODE` and `TABLET_MIGRATION` remain unchanged from current behavior until Phase 5 is explicitly implemented after decommission validation.
- When no extra feature activity is detected, metrics are disabled under the current `adaptive_timeout_store_metrics` behavior, or metrics fail, the decommission timeout values returned by `adaptive_timeout` remain unchanged from the current formulas in `sdcm/utils/adaptive_timeouts/__init__.py:38-110`, regardless of `cpu_load_5`.
- Adaptive-timeout telemetry can answer all of the following without rerunning the test: which feature signals were active, what the `cpu_load_5` load signal was, what the baseline budget was, what feature factor / cpu scale / combined factor was applied, and what adjusted budget was finally used.
- New config surface (`adaptive_timeout_feature_enabled`, `adaptive_timeout_feature_factors`, `adaptive_timeout_max_combined_factor`) is documented and has defaults in SCT config/docs.
- Automated coverage exists for metrics detection, decommission multiplier composition (feature factor and cpu scale), metrics-disabled fallback behavior, and telemetry payloads without store signature changes. Phase 5 adds separate coverage for `NEW_NODE` and `TABLET_MIGRATION`.
- Setting `adaptive_timeout_feature_enabled: false` in test config reverts all behavior to current baseline (rollback without code change).

## Post-Merge Lifecycle and Governance

This section defines who owns the feature factors after merge, what process governs changes to them, and how factor inflation is prevented.

### Background: replacing the manual timeout ratchet

Historically, topology-operation timeouts in SCT were hand-tuned constants. Every time an operation timed out in CI, the reflexive fix was to bump the number up — and it only ever went up, because lowering it risked re-introducing a flaky failure nobody wanted to own. This produced a one-directional ratchet: timeouts grew to accommodate the slowest observed cluster, which then masked genuine regressions on every faster cluster. The single global "1h bar" mandated for tablet topology changes is the current expression of that approach: one absolute number that cannot distinguish a featureless cluster from one running CDC + MV + LWT + counters, and that must be manually revisited (upward) whenever it stops holding.

This plan deliberately changes the *shape* of the knob to break that ratchet:

1. **Budgets are derived, not absolute.** The final budget is `baseline(data_size, throughput) × operation_factor × combined_factor`. The data-size-derived baseline tracks the cluster automatically, so nobody edits "seconds" when hardware or dataset size changes. The "1h bar" becomes the *baseline floor* for tablets, and any deviation above it is explained by a named, bounded factor rather than a quietly-raised global constant.
2. **The adjustable surface is multiplicative and capped.** Feature factors are small per-feature constants whose product is bounded by `MAX_COMBINED_FACTOR` (default `3.0`). A factor cannot silently grow unbounded the way a free-form seconds value could.
3. **The cap is a hard wall, not a suggestion.** Hitting `MAX_COMBINED_FACTOR` is defined as a signal to investigate Scylla-side performance, *not* to raise the cap. This is the structural replacement for "just bump the number again."
4. **Lowering is a first-class, mandated path.** Unlike the old ratchet, the governance below requires periodic review *for reduction*, so the default review question is "can this come down?" — not only "does this need to go up?"

### Ownership (RACI)

| Role | Who | Responsibility |
|------|-----|----------------|
| **Responsible** | Adaptive-timeouts module maintainer (currently the author of this plan) | Proposes factor changes, attaches Argus evidence, opens the PR |
| **Accountable** | QA / infra lead | Approves or rejects factor-change PRs; owns the decision of record |
| **Consulted** | Scylla topology/dev team | Engaged when a budget approaches or exceeds `MAX_COMBINED_FACTOR` (a performance signal, not a tuning one) |
| **Informed** | Test owners and stakeholders | Notified via the PR, Argus telemetry, and quarterly-review notes |

No single engineer can raise a factor unilaterally: the Responsible role proposes with data, the Accountable role approves. This removes the old "whoever hit the flaky timeout bumps the number" pattern.

### Anti-inflation guard

These mechanisms exist specifically to prevent feature factors from drifting upward the way hand-tuned constants did:

- **Hard ceiling.** `MAX_COMBINED_FACTOR` (default `3.0`) bounds the product of all feature factors and the cpu scale. It can only be exceeded per-test via `adaptive_timeout_max_combined_factor`, and the internal constant is *not* raised without a referenced Scylla issue (see Escalation path).
- **Evidence-gated increases.** A factor increase is only accepted with Argus telemetry proving the current budget is genuinely near-exhaustion (criteria below). "It timed out once, bump it" is explicitly not sufficient.
- **Mandatory downward review.** The quarterly cadence forces a "can these come down?" pass, so the system is biased toward the *minimum* budget that holds, not the maximum anyone has ever needed.
- **Cap breach routes to Scylla, not to SCT.** If a real workload cannot fit under `3.0×`, the response is a Scylla performance investigation, not a larger SCT number. This keeps SCT timeouts from absorbing (and hiding) Scylla regressions.

### Change criteria for factor increases

A factor increase PR must include evidence from Argus telemetry showing that:
1. `actual_duration / adjusted_timeout > 0.85` for at least 5 independent occurrences of the specific feature combination (i.e., the current budget is consistently near-exhaustion).
2. The `cpu_load_5` and `combined_factor` values from those runs are within expected bounds (not caused by anomalous node conditions like disk throttling or network partitions).
3. The proposed new factor value is the minimum adjustment that brings `actual_duration / adjusted_timeout` below `0.7` based on the observed data.

Factor increases without Argus data links are not accepted.

### Change criteria for factor decreases

Factors should be *lowered* when telemetry shows consistent headroom:
- If `actual_duration / adjusted_timeout < 0.3` for a feature combination across 10+ runs over 4+ weeks, the factor is a candidate for reduction.
- Factor decreases follow the same review process as increases.

### Escalation path

If a factor needs to exceed `MAX_COMBINED_FACTOR` (default `3.0`), that is a signal to investigate the Scylla-side topology operation performance rather than continuing to inflate SCT timeouts. This is the deliberate stopping point that the old manual ratchet never had — instead of raising "1h" to "2h" for everyone, a cap breach forces the question back to Scylla. The expected response is:
1. File a Scylla issue with the Argus telemetry data showing the topology operation duration relative to data size and feature set.
2. Use `adaptive_timeout_max_combined_factor` config override as a temporary, test-scoped workaround for the affected test case (visible in that test's config, not hidden in a global constant).
3. Do NOT raise the internal `MAX_COMBINED_FACTOR` constant without a Scylla issue reference.

### Visibility to stakeholders

The intent is that no budget change is invisible or unexplained — the opposite of a global constant being quietly edited:

- **Every factor change is a reviewed PR with data.** PRs must link Argus telemetry showing `duration / base_timeout` ratios, `cpu_load_5`, `active_features`, and `combined_factor` from real runs, plus a before/after comparison of the affected timeout budgets. A change with no data link is not accepted.
- **Per-run budgets are self-explaining in telemetry.** Phase 3 records `active_features`, `base_timeout`, `adjusted_timeout`, `feature_factor`, `cpu_scale`, and `combined_factor` in Argus, so any stakeholder can see *why* a given run got the budget it did without rerunning the test.
- **Independent queryability.** Argus CLI tooling (see Phase 3) lets stakeholders query timeout telemetry themselves, so the data backing any increase/decrease is not gated on the module maintainer.
- **Quarterly review notes are shared.** The cadence review (below) produces a short written summary of which factors moved, in which direction, and why — giving stakeholders a running record of how budgets evolve over time.

### Periodic review cadence

Quarterly review of Argus telemetry to determine:
1. Whether any factors can be lowered (as Scylla improves topology operation performance).
2. Whether any factors need raising (new failure patterns).
3. Whether new feature signals should be added (e.g., new Scylla features that affect topology operations).

## Risk Mitigation

- **Risk:** Feature metrics are temporarily zero or unavailable, under-detecting active feature signals
  **Likelihood:** Medium (counter resets, scrape lag, or endpoint unavailability can hide short-lived activity)
  **Impact:** Medium -- topology operations may wait longer than necessary before reporting failures.
  **Mitigation:** Use primary + fallback metrics per feature, short TTL cache, and no-fail fallback to baseline when metrics cannot be fetched.
  **Rollback:** Set `adaptive_timeout_feature_enabled: false` in test config to disable entirely without code revert.

- **Risk:** Stale cumulative counters over-detect a feature (false positive) -- e.g. CDC was enabled months ago, produced data, then had its CDC tables dropped, yet `scylla_cdc_operations_total > 0` persists
  **Likelihood:** Medium for reused/long-lived CI clusters (`SCT_REUSE_CLUSTER`); Low for fresh per-test clusters that boot counters at 0.
  **Impact:** Low -- the timeout is inflated for a feature that is no longer active, but is never reduced below baseline.
  **Mitigation:** Accept the inflation by design: an over-estimated budget only delays failure reporting (bounded by `MAX_COMBINED_FACTOR`, default 3x), whereas an under-estimate causes a false premature timeout, which is the failure mode this plan exists to prevent. The tablet-mode hard-timeout monitor still aborts genuinely stalled operations. If telemetry shows chronic over-inflation on reused clusters, narrow detection to a recent-activity signal (e.g. rate over a window) in a follow-up.
  **Rollback:** Same master switch / per-feature override (`adaptive_timeout_feature_factors`) as above.

- **Risk:** MV/SI signal is coarse
  **Likelihood:** Medium (SI does not expose a single dedicated metric as strong as CDC/LWT)
  **Impact:** Low-Medium -- SI-heavy runs may receive less aggressive uplift than expected.
  **Mitigation:** Treat SI as part of the MV/view-update signal in v1, track Argus outcomes, and tune detector set from telemetry if a better dedicated signal appears.

- **Risk:** Feature detection queries fail on unhealthy or zero-token targets
  **Likelihood:** Medium
  **Impact:** Medium -- feature-aware logic could become unavailable during the exact scenarios it is meant to protect.
  **Mitigation:** Reuse the current `NodeLoadInfoService` metrics path and fall back to a no-feature baseline instead of hard failing the topology operation. This preserves current adaptive-timeout behavior when node metrics are unavailable.

- **Risk:** Telemetry schema expansion breaks existing Argus readers
  **Likelihood:** Low
  **Impact:** Medium -- result ingestion or dashboards may mis-handle the richer payload.
  **Mitigation:** Keep the legacy `timeout` field during the migration, add new columns in a backward-compatible way, and validate against current `AdaptiveTimeoutResultsTable` behavior in `sdcm/utils/adaptive_timeouts/load_info_store.py:271-383`.

- **Risk:** Feature base factors or `CPU_SCALE_CAP` are wrong (too high or too low)
  **Likelihood:** High (initial values are estimates)
  **Impact:** Low-Medium -- either timeouts are still too short (low impact, same as today) or too long (medium, masks real failures).
  **Mitigation:** Ship decommission first with telemetry (Phases 1-4), validate against real decommission runs, then tune base factors and the cpu-scale shape based on actual `duration / base_timeout` ratios correlated with the recorded `cpu_load_5` and feature set. Do not enable `NEW_NODE` or `TABLET_MIGRATION` until decommission results are reviewed. Per-test tuning is available via `adaptive_timeout_feature_factors` and `adaptive_timeout_max_combined_factor` config parameters (Phase 1, step 4).

- **Risk:** `cpu_load_5` on the decommission target is dominated by streaming load (or scrape lag), skewing the cpu scale
  **Likelihood:** Medium
  **Impact:** Low-Medium -- the uplift magnitude may be larger or smaller than the underlying topology cost warrants.
  **Mitigation:** Floor `cpu_scale` at `1.0` and bound it by `CPU_SCALE_CAP`, so the signal can only widen the budget within a fixed range and never shrink below the feature factor. Fall back to the current baseline behavior when `cpu_load_5`/`shards_count` is unavailable or metrics collection is disabled. If telemetry shows persistent skew, revisit the operation-node mapping in a follow-up plan.

- **Risk:** Broadening to `NEW_NODE` and `TABLET_MIGRATION` before decommission validation carries unproven timeout changes into additional flows
  **Likelihood:** Medium if Phase 5 is implemented too early
  **Impact:** Medium -- new-node or tablet-migration monitoring thresholds could become too lenient or too noisy before the decommission model is validated.
  **Mitigation:** Gate Phase 5 on real decommission validation results from Phase 4. Keep `NEW_NODE` and `TABLET_MIGRATION` on baseline behavior in the initial rollout and require follow-up tests before enabling them.

## Cross-Plan Dependencies

This plan has no blocking dependencies on other active plans. It touches adaptive timeout internals that are not modified by any other plan in MASTER.md. The closest related plan is "Health Check Optimization" (`docs/plans/infrastructure/health-check-optimization.md`) which also uses `adaptive_timeout` for `Operations.HEALTHCHECK`, but that operation is not topology-sensitive and is unaffected by this work.
