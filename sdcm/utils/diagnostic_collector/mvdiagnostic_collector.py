import logging
import json


from datetime import datetime
from typing import Any
from pathlib import Path

from sdcm.utils.diagnostic_collector import DiagnosticCollector

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

    def __init__(self, session, dir_path: str, name: str | None = None):
        super().__init__(name)
        self._session = session
        self._results: dict[str, Any] = {}
        self._dir = Path(dir_path) / super().root_dir / self.save_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        self._save_path = self._dir / f"{self.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def collect(self) -> Any:
        """Collect MV and SI diagnostics data."""
        for query in self.queries:
            result = self._execute_query(query)
            self._results[query] = list(result)
        return self._results

    def store(self, result: Any):
        """Store MV and SI diagnostics data."""
        if not result:
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
        self._results.clear()

    def _execute_query(self, query: str) -> Any:
        try:
            return self._session.execute(query)
        except Exception as e:  # noqa: BLE001
            LOGGER.error("Exception occurred while executing query '%s': %s", query, e)
            return []  # Return empty list on error to prevent issues in collect()
