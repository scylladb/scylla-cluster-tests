# Diagnostic Collector

This module provides a small, reusable framework for collecting and storing diagnostics data
during any cluster operation or CQL command execution. Collection runs in a background thread,
so the diagnostics are gathered periodically while the main code keeps doing its work.

It was originally introduced to capture materialized view (MV) and secondary index (SI) build
state, but it is intentionally generic: a collector can gather any data (system tables, metrics,
files, etc.). Potential uses include SLA metrics and tracking tablet splits/merges during cluster
operations.

## Building Blocks

- `DiagnosticCollector`: Abstract base class for collectors. Implement `collect()` / `store()`
  (and optionally `clean()`).
- `DiagnosticManager`: Owns a background worker thread that runs the collectors on a fixed
  interval and keeps a `DiagnosticResult` per cycle. Performs one final collection before stopping
  so the last interval is not lost. Exposes `is_running`/`is_healthy()`/`error` to monitor the
  worker, and can be restarted after being stopped.
- `collect_diagnostics(...)`: A generic context-manager helper — the recommended way to run
  collectors. Works with any collector, so no per-collector wrapper is needed.
- `ExceptionHandler`: Strategy-based handling of collect/store failures
  (`ExceptionStrategy.CONTINUE` or `ExceptionStrategy.STOP`).
- `MVDiagnosticCollector`: Built-in collector for MV/SI build diagnostics.

## Recommended Usage: `collect_diagnostics`

`collect_diagnostics` accepts one or more collectors and runs them in the background for the
duration of the `with` block. This is the simplest and preferred entry point:

```python
from sdcm.utils.diagnostic_collector.manager import collect_diagnostics
from sdcm.utils.diagnostic_collector.views import MVDiagnosticCollector

# `session` is a Cassandra/Scylla session object.
with collect_diagnostics(MVDiagnosticCollector(session)):
    # Diagnostics are collected periodically while this block runs.
    run_workload()
```

Multiple collectors and a custom interval are supported:

```python
with collect_diagnostics(MVDiagnosticCollector(session), MyCollector(session), interval=30.0):
    run_workload()
```

The output directory defaults to the current test log directory (`TestConfig().logdir()`), so it
does not need to be passed explicitly. Override it only when needed:

```python
MVDiagnosticCollector(session, dir_path="/tmp/sct-results")
```

## Advanced Usage

### As a context manager

When you need access to the manager object (for example, to read results afterwards), use
`DiagnosticManager` directly:

```python
from sdcm.utils.diagnostic_collector import ExceptionHandler
from sdcm.utils.diagnostic_collector.manager import DiagnosticManager
from sdcm.utils.diagnostic_collector.views import MVDiagnosticCollector

manager = DiagnosticManager(
    collectors=[MVDiagnosticCollector(session, exception_handler=ExceptionHandler())],
    interval=60.0,
)

with manager:
    run_workload()

# Access results after the manager stops.
results = manager.get_results()
```

### With explicit start/stop

```python
from sdcm.utils.diagnostic_collector.manager import DiagnosticManager
from sdcm.utils.diagnostic_collector.views import MVDiagnosticCollector

manager = DiagnosticManager(collectors=[MVDiagnosticCollector(session)], interval=30.0)

manager.start_collecting()
try:
    run_workload()
finally:
    manager.stop_collecting()

results = manager.get_results()
```

### Reading collected results

`DiagnosticManager` keeps an in-memory history of every collection cycle, in addition to whatever
each collector persists in its own `store()`. The history is exposed through three methods:

- `get_results()` — return a copy of the full history as a list of `DiagnosticResult` (one entry
  per collector per cycle). Safe to call while collection is running.
- `clear_results()` — drop the accumulated history (e.g. to start a fresh round before the next
  workload).
- `is_collecting` — `True` while a collection cycle is in progress.

```python
for result in manager.get_results():
    print(result.collector_name, result.timestamp, result.collected, result.stored, result.error)
    print(result.data)   # this cycle's snapshot — independent of every other cycle
```

Each cycle's `collect()` return value is recorded as an independent snapshot, so reading the
history never shows data overwritten by a later cycle. `DiagnosticManager` owns its worker thread
and creates a fresh one on each `start_collecting()`, so a stopped manager can simply be started
again to run another round (you may also use a new `collect_diagnostics(...)` block).

> **Memory note:** the in-memory history holds the full `collect()` snapshot for every cycle. It is
> a bounded ring buffer (`max_history`, default `10_000`): once full, the oldest cycle is dropped
> automatically so a long-running manager cannot grow memory without bound. For short-lived
> `collect_diagnostics(...)` blocks the whole history is freed when the block exits. Pass
> `max_history=None` for an unbounded history (and call `clear_results()` periodically yourself).
> Each collector's on-disk `store()` output is independent of this in-memory history.

## Adding a New Collector

To collect a new kind of diagnostics, subclass `DiagnosticCollector` and implement the data
collection and storage. No changes to `DiagnosticManager` or `collect_diagnostics` are required —
just pass your new collector to `collect_diagnostics`.

1. Subclass `DiagnosticCollector`.
2. Implement `collect()` — gather and return the data for one cycle.
3. Implement `store(result)` — persist the collected data (file, log, metric sink, ...).
4. Optionally implement `clean()` — release resources when collection stops.
5. Store output under `self.root_dir` (`diagnostics/`) so it gets picked up with the runner logs.

> **Snapshot ownership:** `collect()` must return a **new** object each call, and `clean()` must
> not mutate a value already returned by `collect()` (rebind internal state instead of clearing it
> in place). The manager keeps every cycle's return value in its history, so sharing/clearing a
> single object would corrupt previously recorded results.


```python
import json
from datetime import datetime
from pathlib import Path

from sdcm.test_config import TestConfig
from sdcm.utils.diagnostic_collector import DiagnosticCollector


class MyCollector(DiagnosticCollector):
    """Collect some custom diagnostics data."""

    save_dir = Path("my_diagnostics")

    def __init__(self, session, dir_path: str | None = None, name: str | None = None):
        super().__init__(name)
        self._session = session
        self._dir = Path(dir_path or TestConfig().logdir()) / self.root_dir / self.save_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._save_path = self._dir / f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def collect(self):
        return list(self._session.execute("SELECT * FROM system.some_table"))

    def store(self, result):
        with open(self._save_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"timestamp": datetime.now().isoformat(), "data": result}, default=str) + "\n")

    def clean(self):
        # Optional: release resources here.
        pass
```

Use it exactly like any other collector:

```python
with collect_diagnostics(MyCollector(session)):
    run_workload()
```

## Error Handling

`ExceptionHandler` decides what happens when a collector's `collect()` or `store()` raises. Each
collector owns its own handler (dependency injection), so different collectors can use different
policies, and a handler can be swapped at runtime via `collector.exception_handler = ...`:

- `ExceptionStrategy.CONTINUE` (default): log the error and keep collecting on the next cycle.
- `ExceptionStrategy.STOP`: stop the collection thread.

Subclass `ExceptionHandler` and override `handle_exception_during_collecting` /
`handle_exception_during_storing` to customize the behavior, then pass it to the collector via its
`exception_handler` argument. A single handler instance may be shared across several collectors.

## Notes

- `DiagnosticManager` runs as a daemon thread and performs a final collection before stopping,
  so the last interval is not lost.
- `MVDiagnosticCollector` writes JSON lines to a timestamped log file under
  `diagnostics/mv_si_diagnostics`.
- Output written under `diagnostics/` is included in the SCT runner log artifacts.
