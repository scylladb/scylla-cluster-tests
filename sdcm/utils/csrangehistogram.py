import glob
import os.path
import sys
import logging

from typing import Any
from enum import Enum
from dataclasses import asdict, dataclass, make_dataclass
from functools import partial

from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogReader

LOGGER = logging.getLogger(__file__)

TIME_INTERVAL = 600
PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]
UNTAGGED_KEY = "untagged"


def generate_percentile_name(percentile: int):
    return f"percentile_{percentile}".replace(".", "_")


class CSHistogramTags(Enum):
    WRITE = "WRITE-rt"
    READ = "READ-rt"


class CSHistogramThroughputTags(Enum):
    WRITE = "WRITE-st"
    READ = "READ-st"


class CSHistogramTagTypes(Enum):
    LATENCY = 0
    THROUGHPUT = 1


@dataclass
class HistorgramSummaryBase:
    start_time: str
    end_time: str
    stddev: float


HistorgramSummary = make_dataclass("HistorgramSummary",
                                   [(generate_percentile_name(perc), float) for perc in PERCENTILES],
                                   bases=(HistorgramSummaryBase,))


class CSHistogram(HdrHistogram):
    LOWEST = 1
    HIGHEST = 24 * 3600_000_000_000
    SIGNIFICANT = 3

    def __init__(self, *args, **kwargs):
        super().__init__(lowest_trackable_value=CSHistogram.LOWEST,
                         highest_trackable_value=CSHistogram.HIGHEST,
                         significant_figures=CSHistogram.SIGNIFICANT, *args, **kwargs)


@dataclass
class CSRangeHistogram:
    start_time: float
    end_time: float
    histograms: dict[str, CSHistogram]


class CSRangeHistogramBuilder:
    """
        It is namespace for methods for
        building Time range Histogram from
        c-s hdr logs
    """

    CS_HDR_FILE_WC = "*/cs-hdr-*.hdr"

    @staticmethod
    def build_histogram_from_dir(pattern: str = "", base_path="",
                                 start_time: float = 0,
                                 end_time: int = sys.maxsize,
                                 absolute_time: bool = True) -> CSRangeHistogram:
        """
            search in dir from 'base_path' with provided pattern or
            default global pattern 'CS_HDR_FILE_WC' hdr log files
            and build Range Histogram with time interval (start_time, end_time)
            For timestamps is used absolute time in ms since epoch start
        """
        collected_histograms: list[CSRangeHistogram] = []
        hdr_files = CSRangeHistogramBuilder.get_list_of_hdr_files(pattern, base_path)
        for hdr_file in hdr_files:
            file_range_histogram = CSRangeHistogramBuilder.build_histogram_from_file(hdr_file,
                                                                                     start_time,
                                                                                     end_time,
                                                                                     absolute_time=absolute_time)
            collected_histograms.append(file_range_histogram)
        return CSRangeHistogramBuilder.merge_range_histograms(collected_histograms)

    @staticmethod
    def build_histogram_from_file(hdr_file: str,
                                  start_time: float = 0,
                                  end_time: int = sys.maxsize,
                                  absolute_time: bool = True) -> CSRangeHistogram:
        """
        Build Range Histogram with time interval (start_time, end_time)
        from provided hdr log file.
        For timestamps is used absolute time in ms since epoch start
        """
        if not os.path.exists(hdr_file):
            LOGGER.error("File doesn't exists: %s", hdr_file)
            return CSRangeHistogram(start_time=0, end_time=0, histograms={})

        histograms: dict[str, CSHistogram] = {}
        hdr_reader = HistogramLogReader(hdr_file, CSHistogram())
        end_time = end_time or sys.maxsize
        while True:
            next_hist = hdr_reader.get_next_interval_histogram(range_start_time_sec=start_time,
                                                               range_end_time_sec=end_time,
                                                               absolute=absolute_time)
            if not next_hist:
                break
            tag = next_hist.get_tag()
            if not tag:
                tag = UNTAGGED_KEY

            if tag not in histograms.keys():
                histograms[tag] = CSHistogram()
                histograms[tag].set_tag(tag)
            if histograms[tag].get_start_time_stamp() == 0:
                histograms[tag].set_start_time_stamp(next_hist.get_start_time_stamp())
            histograms[tag].add(next_hist)

        return CSRangeHistogram(start_time=start_time,
                                end_time=end_time,
                                histograms=histograms)

    @staticmethod
    def build_histograms_range_with_interval(path: str,
                                             start_time: int | float, end_time: int | float,
                                             interval=TIME_INTERVAL, absolute_time=True) -> list[CSRangeHistogram]:
        """
            Build set of time range histogrmas (as list) from
            single file or files search by pattern in provided
            dir path for time range (start_time, end_time) with
            interval 'interval'. each Time range histogram will
            have results with time duration 'interval'.
        """

        start_ts = int(start_time)
        end_ts = int(end_time)
        summary = []

        if end_ts - start_ts < TIME_INTERVAL:
            window_step = int(end_ts - start_ts)
        else:
            window_step = interval or TIME_INTERVAL

        if os.path.exists(path) and os.path.isfile(path):
            build_range_histogram = partial(CSRangeHistogramBuilder.build_histogram_from_file,
                                            hdr_file=path,
                                            absolute_time=absolute_time)
        elif os.path.exists(path) and os.path.isdir(path):
            build_range_histogram = partial(CSRangeHistogramBuilder.build_histogram_from_dir,
                                            base_path=path,
                                            absolute_time=absolute_time)
        else:
            return summary

        for start_interval in range(start_ts, end_ts, window_step):
            end_interval = end_ts if start_interval + window_step > end_ts else start_interval + window_step
            range_histograms = build_range_histogram(start_time=start_interval, end_time=end_interval)
            if not range_histograms.histograms:
                continue
            summary.append(range_histograms)

        return summary

    @staticmethod
    def build_from_log_line(log_line: str, hst_log_start_time: float):
        """
        build time range histogram from singe hdr log file line
        log line example:
        #[BaseTime: 1665956621.000 (seconds since epoch)]
        #[StartTime: 1665956621.000 (seconds since epoch), Sun Oct 16 21:43:41 UTC 2022]
        "StartTimestamp","Interval_Length","Interval_Max","Interval_Compressed_Histogram"
        Tag=READ-st,0.000,4.999,20.726,HISTFAAAA9d42jVUMY/kN...
        """
        tag, start, interval_len, _, encoded_hist = log_line.split(",")
        tag = tag.split("=")[-1]
        hst_start_ts = hst_log_start_time + float(start)
        hst_end_ts = hst_start_ts + float(interval_len)
        histogram = CSHistogram()
        histogram.set_tag(tag)
        histogram.set_start_time_stamp(hst_start_ts)
        histogram.set_end_time_stamp(hst_end_ts)
        histogram.decode_and_add(encoded_hist)

        return CSRangeHistogram(start_time=hst_start_ts,
                                end_time=hst_end_ts,
                                histograms={tag: histogram})

    @staticmethod
    def get_list_of_hdr_files(pattern: str, base_path: str) -> list[str]:
        """
            find all hdr log file by pattern like glob wc
            in profided by base_path dir.
        """
        if not pattern:
            pattern = CSRangeHistogramBuilder.CS_HDR_FILE_WC
        if not base_path:
            base_path = os.path.abspath(os.path.curdir)
        hdr_files = []
        for hdr_file in glob.glob(pattern, root_dir=base_path, recursive=True):
            hdr_files.append(os.path.join(base_path, hdr_file))
        return hdr_files

    @staticmethod
    def merge_range_histograms(range_histograms: list[CSRangeHistogram]) -> CSRangeHistogram:
        """
            Merge several time range histogram to one containg summary result.
        """
        if not range_histograms:
            return CSRangeHistogram(start_time=0, end_time=0, histograms={})

        final_hst = range_histograms.pop(0)
        for hst in range_histograms:
            for tag in hst.histograms.keys():
                if tag not in final_hst.histograms:
                    final_hst.histograms[tag] = CSHistogram()
                final_hst.histograms[tag].add(hst.histograms[tag])

            final_hst.start_time = min(final_hst.start_time, hst.start_time)
            final_hst.end_time = max(final_hst.end_time, hst.end_time)

        return final_hst


class CSRangeHistogramSummary:
    """
    Class to convert raw time range histograms
    built with CSRangeHistogramBuider's methods
    to dataclass or dict for better presentation
    data
    """

    def __init__(self, histograms: CSRangeHistogram | list[CSRangeHistogram],
                 tag_type: CSHistogramTagTypes = CSHistogramTagTypes.LATENCY):
        if not isinstance(histograms, list):
            histograms = [histograms]

        self._range_histograms = histograms

        if tag_type == CSHistogramTagTypes.THROUGHPUT:
            self.cs_histogram_type_tags = CSHistogramThroughputTags
        else:
            self.cs_histogram_type_tags = CSHistogramTags

        self.cs_operation_tag_mapping = {
            "write": {self.cs_histogram_type_tags.WRITE: "WRITE"},
            "read": {self.cs_histogram_type_tags.READ: "READ"},
            "mixed": {
                self.cs_histogram_type_tags.WRITE: "WRITE",
                self.cs_histogram_type_tags.READ: "READ"
            }
        }

    @staticmethod
    def convert_raw_histogram(histogram: CSHistogram,
                              base_start_ts: float = 0.0,
                              base_end_ts: float = 0.0) -> "HistorgramSummary":
        percentiles_data = {}

        for perc, value in histogram.get_percentile_to_value_dict(PERCENTILES).items():
            percentiles_data[generate_percentile_name(perc)] = round(value / 1_000_000, 2)

        return HistorgramSummary(
            start_time=histogram.get_start_time_stamp() or base_start_ts,
            end_time=histogram.get_end_time_stamp() or base_end_ts,
            stddev=histogram.get_stddev(),
            **percentiles_data)

    def get_summary_for_operation(self, workload: str) -> list[dict[str, Any]]:
        return self._parse_range_histogram_for_workload(workload)

    def _parse_range_histogram_for_workload(self, workload: str) -> list[dict[str, Any]]:
        converted_histograms = []
        if workload not in self.cs_operation_tag_mapping:
            return converted_histograms

        for hst in self._range_histograms:
            summary = {}
            for tag, operation in self.cs_operation_tag_mapping[workload].items():
                if tag.value in hst.histograms.keys():
                    parsed_summary = self.convert_raw_histogram(hst.histograms[tag.value], hst.start_time, hst.end_time)
                    summary.update({operation: asdict(parsed_summary)})
            if summary:
                converted_histograms.append(summary)
        return converted_histograms
