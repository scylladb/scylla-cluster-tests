import logging
import json


from datetime import datetime
from typing import Any
from pathlib import Path

from sdcm.test_config import TestConfig
from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionHandler

LOGGER = logging.getLogger(__name__)


class MVDiagnosticCollector(DiagnosticCollector):
    """Collect diagnostics data for Materialized Views and Secondary Indexes."""

    queries = [
        "SELECT count(*) FROM system.view_building_tasks",
        "SELECT * FROM system.view_building_tasks",
        "SELECT * from system.view_build_status_v2",
        "SELECT * FROM system.views_builds_in_progress",
    ]

    save_dir = Path("mv_si_diagnostics")

    def __init__(
        self,
        session,
        dir_path: str | None = None,
        name: str | None = None,
        exception_handler: ExceptionHandler | None = None,
    ):
        super().__init__(name, exception_handler=exception_handler)
        self._session = session
        self._results: dict[str, Any] = {}
        self._dir = Path(dir_path or TestConfig().logdir()) / super().root_dir / self.save_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._save_path = self._dir / f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def collect(self) -> Any:
        """Collect MV and SI diagnostics data."""
        results: dict[str, Any] = {}
        for query in self.queries:
            result = self._execute_query(query)
            results[query] = list(result)
        # Keep a reference to the latest snapshot for the store() fallback, but return a fresh
        # dict so each recorded DiagnosticResult.data is independent and not overwritten next cycle.
        self._results = results
        return results

    def store(self, result: Any):
        """Store MV and SI diagnostics data."""
        # Only fall back to the last snapshot when the manager passed nothing; an empty-but-valid
        # mapping is a legitimate result and must not silently reuse a previous snapshot.
        if result is None:
            result = self._results

        timestamp = datetime.now().isoformat()
        try:
            with open(self._save_path, "a", encoding="utf-8") as f:
                for query, data in result.items():
                    record = {"timestamp": timestamp, "query": query, "data": data}
                    f.write(json.dumps(record, default=str) + "\n")
                    LOGGER.debug("Stored MV/SI diagnostics data: %s, %d rows", query, len(data) if data else 0)
        except (OSError, IOError) as e:
            LOGGER.error("Failed to write diagnostics to %s: %s", self._save_path, e)
            raise

    def clean(self):
        """Clean up MV and SI collector resources."""
        # Rebind instead of clearing in place: the last snapshot is shared with the
        # DiagnosticResult.data handed to the manager, so mutating it would wipe recorded history.
        self._results = {}

    def _execute_query(self, query: str) -> Any:
        try:
            return self._session.execute(query)
        except Exception as e:  # noqa: BLE001
            LOGGER.error("Exception occurred while executing query '%s': %s", query, e)
            return []  # Return empty list on error to prevent issues in collect()
