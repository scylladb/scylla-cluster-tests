import glob
import os.path
import datetime
import sys

from typing import Any

from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogReader


CS_HDR_FILE_WC = "*/cs_hdr_*.hdr"
TIME_INTERVAL = 300


def get_list_of_hdr_files(base_path: str) -> list[str]:
    hdr_files = []
    for hdr_file in glob.glob(CS_HDR_FILE_WC, root_dir=base_path, recursive=True):
        hdr_files.append(os.path.join(base_path, hdr_file))
    return hdr_files


class CSHdrHistogram:
    LOWEST = 1
    HIGHEST = 24 * 60 * 60 * 1000 * 1000
    SIGNIFICANT = 3
    WRITE = "WRITE-rt"
    READ = "READ-rt"

    PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]

    @classmethod
    def get_empty_histogram(cls) -> HdrHistogram:
        return HdrHistogram(cls.LOWEST, cls.HIGHEST, cls.SIGNIFICANT)

    @classmethod
    def format_timestamp(cls, timestamp: float) -> str:
        return datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def __init__(self, base_path: str):
        self.base_time = None
        self._base_path = base_path
        self._tagged_histograms: dict[str, HdrHistogram] = {}
        self._untagged_histogram = self.get_empty_histogram()

    def clear_histograms(self):
        self._tagged_histograms: dict[str, HdrHistogram] = {}
        self._untagged_histogram = self.get_empty_histogram()

    def build_histogram_from_files(self, start_time: float = 0, end_time: int = sys.maxsize, absolute: bool = False):
        hdr_files = get_list_of_hdr_files(self._base_path)
        for hdr_file in hdr_files:
            hdr_reader = HistogramLogReader(hdr_file, self.get_empty_histogram())
            while True:
                next_hist = hdr_reader.get_next_interval_histogram(
                    range_start_time_sec=start_time, range_end_time_sec=end_time, absolute=absolute
                )
                if not next_hist:
                    break

                if tag := next_hist.get_tag():
                    if tag not in self._tagged_histograms.keys():
                        self._tagged_histograms[tag] = self.get_empty_histogram()
                        self._tagged_histograms[tag].set_tag(tag)
                    if self._tagged_histograms[tag].get_start_time_stamp() == 0:
                        self._tagged_histograms[tag].set_start_time_stamp(next_hist.get_start_time_stamp())
                    self._tagged_histograms[tag].add(next_hist)
                else:
                    if not self._untagged_histogram.get_start_time_stamp() == 0:
                        self._untagged_histogram.set_start_time_stamp(next_hist.get_start_time_stamp())
                    self._untagged_histogram.add(next_hist)

    def get_operation_stats_by_tag(self, tag: str = "") -> dict[str, Any]:
        histogram = self._untagged_histogram if not tag else self._tagged_histograms.get(tag)
        if not histogram:
            return {}
        percentilies = histogram.get_percentile_to_value_dict(self.PERCENTILES)
        percentils_in_ms = {
            f"percentile_{k}".replace(".", "_"): round((v / 1_000_000), 2) for k, v in percentilies.items()
        }
        return {
            "start_time": self.format_timestamp(histogram.get_start_time_stamp() / 1000),
            "end_time": self.format_timestamp(histogram.get_end_time_stamp() / 1000),
            "stddev": histogram.get_stddev() / 1_000_000,
            **percentils_in_ms,
        }

    def get_write_stats(self):
        return self.get_operation_stats_by_tag(tag=self.WRITE)

    def get_read_stats(self):
        return self.get_operation_stats_by_tag(tag=self.READ)

    def get_stats(self, operation):
        if operation == "write":
            return {"WRITE": self.get_write_stats()}
        elif operation == "read":
            return {"READ": self.get_read_stats()}
        else:
            return {"WRITE": self.get_write_stats(), "READ": self.get_read_stats()}

    def get_hdr_stats_for_interval(
        self, workload: str, start_ts: float, end_ts: float | int, base_dir: str = ""
    ) -> list[dict]:
        start_ts = int(start_ts)
        end_ts = int(end_ts)
        if not base_dir:
            base_dir = self._base_path
        if end_ts - start_ts < TIME_INTERVAL:
            window_step = int(end_ts - start_ts)
        else:
            window_step = TIME_INTERVAL

        hdr_stats = []
        for start_interval in range(start_ts, end_ts, window_step):
            end_interval = end_ts if start_interval + window_step > end_ts else start_interval + window_step
            self.build_histogram_from_files(start_time=start_interval, end_time=end_interval, absolute=True)
            hdr_stats.append(self.get_stats(workload))
            self.clear_histograms()

        return hdr_stats
