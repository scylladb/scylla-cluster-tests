"""Shared mock classes for diagnostic_collector unit tests."""

import time
from typing import Any

from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionHandler, ExceptionStrategy


class MockDiagnosticCollector(DiagnosticCollector):
    """Mock implementation of DiagnosticCollector shared across diagnostic_collector tests."""

    def __init__(
        self,
        name: str | None = None,
        collect_data: Any = None,
        should_fail_collect: bool = False,
        should_fail_store: bool = False,
        collect_delay: float = 0,
        exception_handler: ExceptionHandler | None = None,
    ):
        super().__init__(name, exception_handler=exception_handler)
        self.collect_data = {"test": "data"} if collect_data is None else collect_data
        self.should_fail_collect = should_fail_collect
        self.should_fail_store = should_fail_store
        self.collect_delay = collect_delay
        self.collect_count = 0
        self.store_count = 0
        self.clean_count = 0
        self.stored_results = []

    def collect(self) -> Any:
        """Mock collect implementation."""
        self.collect_count += 1
        if self.collect_delay > 0:
            time.sleep(self.collect_delay)
        if self.should_fail_collect:
            raise RuntimeError(f"Collection failed for {self.name}")
        return {"data": self.collect_data, "count": self.collect_count}

    def store(self, result: Any):
        """Mock store implementation."""
        self.store_count += 1
        if self.should_fail_store:
            raise RuntimeError(f"Storage failed for {self.name}")
        self.stored_results.append(result)

    def clean(self):
        """Mock clean implementation."""
        self.clean_count += 1


class MockExceptionHandler(ExceptionHandler):
    """Mock exception handler that records exceptions and returns configured strategies."""

    def __init__(
        self,
        collect_strategy: ExceptionStrategy = ExceptionStrategy.CONTINUE,
        store_strategy: ExceptionStrategy = ExceptionStrategy.CONTINUE,
    ):
        super().__init__()
        self.collect_strategy = collect_strategy
        self.store_strategy = store_strategy
        self.collect_exceptions = []
        self.store_exceptions = []

    def handle_exception_during_collecting(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Track collecting exceptions and return configured strategy."""
        self.collect_exceptions.append((collector, exception))
        return self.collect_strategy

    def handle_exception_during_storing(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Track storing exceptions and return configured strategy."""
        self.store_exceptions.append((collector, exception))
        return self.store_strategy
