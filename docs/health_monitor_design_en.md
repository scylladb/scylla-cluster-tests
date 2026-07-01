# Health Monitor — Motivation and Change Plan

## Problem

```
SoftTimeoutEvent Severity.ERROR: operation 'HEALTHCHECK' took 19643.2s (soft timeout: 180s)
```

**5.5 hours** — nemesis thread blocked by health check → `ClusterHealthCheckError` → nemesis killed → test lost nemesis coverage.

### Root Cause

```
check_cluster_health() — synchronous call from nemesis thread
  └── check_node_health(retries=10, delay=15s) × N nodes
        └── node_health_events()
              ├── get_peers_info()     @retrying(n=5) → cql_connection(connect_timeout=100s)
              │                                          └── @retrying(n=30, sleep_time=10) ← 55 min worst case
              ├── get_gossip_info()    @retrying(n=10) → run_nodetool(timeout=∞)
              ├── get_group0_members() → cql_connection(connect_timeout=100s)
              └── get_token_ring_members() @retrying(n=5) → remoter.run(timeout=∞)
```

**Worst case per node:** ~9 hours. For 6 nodes — **days**.

---

## Goals

1. Health check **never** blocks the nemesis thread
2. Health check **never** kills the nemesis thread
3. Execution time is **bounded** — hard ceiling instead of unbounded
4. **Observability** — events in Argus, but no flooding
5. **Backward compatibility** — interfaces are preserved

---

## Solution: 9 Steps

### Step 1: Background daemon thread

**Before:** `check_cluster_health()` blocked the nemesis thread.  
**After:** Background `ClusterHealthMonitor` daemon thread. Nemesis reads cache instantly (0ms).

### Step 2: Non-blocking executor

**Before:** `with ThreadPoolExecutor(...)` → `shutdown(wait=True)` blocks.  
**After:** `wait(timeout=deadline)` + `shutdown(wait=False, cancel_futures=True)`. Cycle deadline 600s — hard ceiling.

### Step 3: `connect_timeout=30s`

**Before:** `cql_connection_patient_exclusive(connect_timeout=100s)` × `@retrying(n=30)` = 55 min.  
**After:** Propagated `connect_timeout=30s` through the entire call chain.

### Step 4: `@retrying` per-call override

**Before:** `get_gossip_info` decorated with `@retrying(n=10)` — cannot change the global default.  
**After:** Decorator supports `_retry_n`, `_retry_sleep_time` kwargs (popped before calling the function).

### Step 5: Nemesis awareness (3 levels)

| Level | Mechanism | Effect |
|-------|-----------|--------|
| 1 | `node.running_nemesis` | Skip node under active nemesis |
| 2 | `last_nemesis_finish_time` + 60s grace period | Skip node after nemesis completion |
| 3 | `nemesis_node_ips` → severity downgrade | Validators reduce severity for expected DN states |

### Step 6: Event deduplication (state transitions)

**Before:** Event every cycle (60s) → Argus flooding.  
**After:** Publish only on transitions: healthy→unhealthy, unhealthy→healthy, reminder every 10 cycles, circuit breaker CRITICAL.

### Step 7: Circuit breaker

**Before:** Chronically failing node spawns abandoned threads every cycle.  
**After:** 5 consecutive failures → skip 5 min → CRITICAL event → single probe after cooldown.

### Step 8: Config dataclass

**Before:** 12 parameters mixed with runtime state in `__init__`.  
**After:** `@dataclass(frozen=True)` + `from_params()`. Immutable, repr, testable.

### Step 9: Graceful shutdown + safety guards

- `_stop_event.is_set()` check before each submit — instant exit
- Warning when `total_in_flight > parallel_workers * 3` — pile-up observability
- `add_health_check()` guard: warning if called after `start()`

---

## Five-Layer Defense Against Unbounded Execution

| # | Mechanism | What it bounds | Without it |
|---|-----------|---------------|------------|
| 1 | `connect_timeout=30s` | Single CQL connect | 100s |
| 2 | `_retry_n=2` override | Internal retries: 2 instead of 5-10 | 50 min |
| 3 | `wait(timeout)` + `shutdown(wait=False)` | Total cycle time | Blocking |
| 4 | `_nodes_in_flight` | No overlapping per-node | N threads per 1 node |
| 5 | Circuit breaker | Thread pile-up | Infinite flapping |

---

## Worst-Case Comparison

| Metric | Before | After |
|--------|--------|-------|
| Nemesis thread blocked | 0–5.5 hours | **0ms** (instant cache read) |
| Health check failure kills nemesis | Yes | **Never** |
| Worst case single worker | ~9 hours | **~22 min** |
| Worst case full cycle | Unbounded (days) | **600s ceiling** |
| Events in Argus | Flooding | **State transitions only** |
| Backward compatibility | — | **100%** |

---

## Files Changed

| File | Purpose |
|------|---------|
| `sdcm/cluster_health_monitor.py` (NEW) | Core: monitor thread, config, dataclasses, circuit breaker, in-flight tracking |
| `sdcm/cluster.py` | `check_cluster_health` non-blocking, `node_health_events` + `check_node_health` with override parameters, `init/stop_health_monitor` |
| `sdcm/utils/decorators.py` | `@retrying` per-call override via `_retry_*` kwargs |
| `sdcm/nemesis/__init__.py` | `try/except` around `check_cluster_health()` |
| `sdcm/nemesis/utils/node_allocator.py` | `last_nemesis_finish_time` in `unset_running_nemesis()` |
| `sdcm/sct_config.py` | New config parameters |
| `sdcm/tester.py` | `init/stop_health_monitor` in setUp/tearDown |
| `sdcm/utils/health_checker.py` | `connect_timeout` propagation |
| `sdcm/utils/raft/__init__.py` | `connect_timeout` propagation |
| `defaults/test_default.yaml` | Defaults for new parameters |
| `unit_tests/unit/test_cluster_health_monitor.py` | 39 tests |

---

## Known Limitation

**Abandoned worker from `get_gossip_info`** may live ~10 min (2 × 300s nodetool timeout + 3s sleep). In-flight tracking prevents overlapping, WARNING is logged when stuck > deadline. However, the thread holds an SSH connection.

Possible follow-ups:
- `_retry_timeout` override to bound total nodetool call time
- Long-lived executor (single `ThreadPoolExecutor` for the entire monitor lifecycle)

