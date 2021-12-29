from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Pattern, Any

from sdcm.results_analyze.nosqlbench.metrics import NoSQLBenchSummaryMetric, all_counters, \
    all_histograms, all_meters, abridged_histograms, abridged_timers, timers
from sdcm.utils.metaclasses import Singleton

LOGGER = logging.getLogger(__name__)


class NoSQLBenchReportBuilder(metaclass=Singleton):
    def __init__(self, summary_file_path: [str | Path]):
        self._path = summary_file_path if isinstance(summary_file_path, Path) else Path(summary_file_path)
        self._raw_summary = None
        self._full_report = None
        self._abridged_report = None

    @property
    def full_report(self) -> dict[str, Any]:
        if self._full_report:
            return self._full_report

        raise ValueError("No full report built yet.")

    @property
    def abridged_report(self) -> dict[str, Any]:
        if self._abridged_report:
            return self._abridged_report

        raise ValueError("No abridged report built yet.")

    def build_full_report(self):
        with open(self._path, mode="r") as infile:
            self._raw_summary = "".join(infile.readlines())
            self._full_report = self._make_full_report_dict()

    def build_abridged_report(self):
        if not self._full_report:
            self.build_full_report()

        LOGGER.info("Full report: %s", self.full_report)

        self._abridged_report = {
            "histograms": dict(sorted({k: v for k, v in self.full_report["histograms"].items()
                                       if any(k.startswith(name) for name in abridged_histograms)}.items())),
            "timers": dict(sorted({k: v for k, v in self.full_report["timers"].items()
                                   if any(k.startswith(name) for name in abridged_timers)}.items()))
        }

    def build_all_reports(self):
        self.build_abridged_report()

    def print_raw_report_file(self):
        LOGGER.info("Printing raw report from file:")
        with open(self._path, mode="r") as infile:
            for line in infile.readlines():
                LOGGER.info(line)

    def save_abridged_report_to_file(self, path: Path = Path("latency_results.json")):
        if not self._abridged_report:
            self.build_abridged_report()

        with path.open(mode="w") as outfile:
            LOGGER.info("Writing abridged performance run report to: %s", path.absolute())
            json.dump(self.abridged_report, outfile)
            LOGGER.info("Finished writing report to: %s", path.absolute())

    @staticmethod
    def make_patterns(metrics: list[NoSQLBenchSummaryMetric]) -> Pattern:
        return re.compile(
            ".*".join([f"({metric.field_pattern}).*{metric.metrics}" for metric in metrics]),
            flags=re.DOTALL
        )

    def _make_full_report_dict(self) -> dict[str, dict[str, str]]:
        groups = {
            "counters": self.make_patterns(all_counters).search(self._raw_summary),
            "histograms": self.make_patterns(all_histograms).search(self._raw_summary),
            "meters": self.make_patterns(all_meters).search(self._raw_summary),
            "timers": self.make_patterns(timers).search(self._raw_summary)
        }

        for group in groups:
            if groups[group]:
                g_dict = groups[group].groupdict()
                groups.update({group: g_dict})

        return groups
