# Diagnostic Collector

This module provides a small framework for collecting and storing diagnostics data from one or more collectors. It includes:

- `DiagnosticCollector`: Abstract base class for collectors.
- `DiagnosticManager`: Threaded scheduler that runs collectors on an interval.
- `ExceptionHandler`: Strategy-based exception handling for collect/store failures.
- `MVDiagnosticCollector`: Collector for materialized view and secondary index diagnostics.

## Key Concepts

### Collector
A collector must implement:

- `collect()`: Return diagnostics data.
- `store(result)`: Persist the collected data.
- `clean()`: Optional cleanup.

### Manager
The manager runs collectors periodically and stores `DiagnosticResult` entries for each cycle. It can be used either as a context manager or as a simple object with explicit start/stop control.

## Usage as Context Manager

Use `DiagnosticManager` as a context manager to start collection automatically and ensure clean shutdown:

```python
from sdcm.utils.diagnostic_collector import ExceptionHandler
from sdcm.utils.diagnostic_collector.diagnostic_manager import DiagnosticManager
from sdcm.utils.diagnostic_collector.mvdiagnostic_collector import MVDiagnosticCollector

# session is a Cassandra/Scylla session object
collector = MVDiagnosticCollector(session=session, dir_path="/tmp/sct-results")
manager = DiagnosticManager(
    collectors=[collector],
    interval=60.0,
    exception_handler=ExceptionHandler(),
)

with manager:
    # Run test workload here
    run_workload()

# Access results after the manager stops
results = manager.get_results()
```

## Usage as Simple Object

Use explicit start/stop if you want manual control:

```python
from sdcm.utils.diagnostic_collector.diagnostic_manager import DiagnosticManager
from sdcm.utils.diagnostic_collector.mvdiagnostic_collector import MVDiagnosticCollector

collector = MVDiagnosticCollector(session=session, dir_path="/tmp/sct-results")
manager = DiagnosticManager(collectors=[collector], interval=30.0)

manager.start_collecting()
try:
    run_workload()
finally:
    manager.stop_collecting()

results = manager.get_results()
```

## Custom Collector Example

```python
from sdcm.utils.diagnostic_collector import DiagnosticCollector

class MyCollector(DiagnosticCollector):
    def __init__(self, name=None):
        super().__init__(name)
        self._data = None

    def collect(self):
        self._data = {"value": 123}
        return self._data

    def store(self, result):
        # Persist or log result
        print(result)

    def clean(self):
        self._data = None
```

## Notes

- `ExceptionHandler` controls whether collection continues or stops on errors.
- `DiagnosticManager` performs a final collection before stopping.
- `MVDiagnosticCollector` writes JSON lines to a timestamped log file under `diagnostics/mv_si_diagnostics`.
