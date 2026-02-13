import logging

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from enum import StrEnum

LOGGER = logging.getLogger(__name__)


class ExceptionStrategy(StrEnum):
    """Strategy for handling exceptions during diagnostics collection."""

    CONTINUE = "continue"  # Continue collecting diagnostics even if an exception occurs
    STOP = "stop"  # Stop collecting diagnostics if an exception occurs


class DiagnosticException(Exception):
    """Base exception for diagnostic collection errors."""


class DiagnosticCollector(ABC):
    """Abstract base class for diagnostic collectors."""

    root_dir = Path("diagnostics")

    def __init__(self, name: str | None = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def collect(self) -> Any:
        """Collect diagnostics data. Must be implemented by subclasses."""

    @abstractmethod
    def store(self, result: Any):
        """Store the collected diagnostics data. Must be implemented by subclasses."""

    def clean(self):
        """Clean up any resources used by the collector. Optional to implement."""


class ExceptionHandler:
    """Handles exceptions that occur during diagnostics operations."""

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


__all__ = [
    "DiagnosticCollector",
    "ExceptionHandler",
    "ExceptionStrategy",
    "DiagnosticException",
]
