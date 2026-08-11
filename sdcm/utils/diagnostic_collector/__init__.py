import logging

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from enum import StrEnum

LOGGER = logging.getLogger(__name__)

__all__ = [
    "DiagnosticCollector",
    "ExceptionHandler",
    "ExceptionStrategy",
    "DiagnosticException",
]


class ExceptionStrategy(StrEnum):
    """Strategy for handling exceptions during diagnostics collection."""

    CONTINUE = "continue"  # Continue collecting diagnostics even if an exception occurs
    STOP = "stop"  # Stop collecting diagnostics if an exception occurs


class DiagnosticException(Exception):
    """Base exception for diagnostic collection errors."""


class ExceptionHandler:
    """Handles exceptions that occur during diagnostics operations.

    A handler is a reusable strategy object: a single instance can be shared by several collectors,
    or each collector can own its own. It is injected into a :class:`DiagnosticCollector` and can be
    swapped at runtime (``collector.exception_handler = OtherHandler()``) to change failure policy on
    the fly. The manager always reads the handler from the failing collector, so each collector can
    independently decide whether to ``CONTINUE`` (skip itself) or ``STOP`` (halt the whole run).
    """

    def handle_exception_during_collecting(
        self, collector: "DiagnosticCollector", exception: Exception
    ) -> ExceptionStrategy:
        """Handle exceptions that occur during diagnostics collecting stage."""
        LOGGER.error("Exception occurred in collector '%s': %s", collector.name, exception)
        return ExceptionStrategy.CONTINUE

    def handle_exception_during_storing(
        self, collector: "DiagnosticCollector", exception: Exception
    ) -> ExceptionStrategy:
        """Handle exceptions that occur during diagnostics storing stage."""
        LOGGER.error("Exception occurred while storing diagnostics for collector '%s': %s", collector.name, exception)
        return ExceptionStrategy.CONTINUE


class DiagnosticCollector(ABC):
    """Abstract base class for diagnostic collectors.

    Each collector owns its own :class:`ExceptionHandler` (dependency injection), so the failure
    policy lives next to the collector it governs and can be customized or swapped per collector,
    including at runtime.
    """

    root_dir = Path("diagnostics")

    def __init__(self, name: str | None = None, exception_handler: ExceptionHandler | None = None):
        self.name = name or self.__class__.__name__
        # Per-collector strategy object; defaults to the CONTINUE-everything handler. Reassign at
        # runtime to change how this collector's collect/store failures are handled.
        self.exception_handler = exception_handler or ExceptionHandler()

    @abstractmethod
    def collect(self) -> Any:
        """Collect diagnostics data. Must be implemented by subclasses."""

    @abstractmethod
    def store(self, result: Any):
        """Store the collected diagnostics data. Must be implemented by subclasses."""

    def clean(self):
        """Clean up any resources used by the collector. Optional to implement."""
