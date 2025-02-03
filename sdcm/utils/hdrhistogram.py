import glob
import os.path
import sys
import logging
import multiprocessing
from typing import Any
from enum import Enum
from dataclasses import asdict, dataclass, make_dataclass
from concurrent.futures.process import ProcessPoolExecutor

from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogReader

LOGGER = logging.getLogger(__file__)

PROCESS_LIMIT = multiprocessing.cpu_count()
TIME_INTERVAL = 600
PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]


class CSHistogramTags(Enum):
    WRITE = "WRITE-rt"
    READ = "READ-rt"


class CSHistogramTagTypes(Enum):
    LATENCY = 0
    THROUGHPUT = 1


class CSWorkloadTypes(Enum):
    WRITE = "write"
    READ = "read"
    MIXED = "mixed"


def make_cs_range_histogram_summary(  # pylint: disable=too-many-arguments,unused-argument
        workload: CSWorkloadTypes, pattern: str = "", base_path="", start_time: int | float = 0, end_time: int | float = sys.maxsize,
        absolute_time: bool = True, tag_type: CSHistogramTagTypes = CSHistogramTagTypes.LATENCY) -> list[dict[str, dict[str, int]]]:
    """
    Build Range Histogram Summary with time interval (start_time, end_time)
    from provided hdr log file.
    For timestamps is used absolute time in ms since epoch start
    """
    builder = _CSRangeHistogramBuilder(workload, tag_type, start_time, end_time)
    if pattern:
        builder.cs_files_pattern = pattern
    builder.absolute_time = absolute_time
    return builder.build_histogram_summary(base_path)


def make_cs_range_histogram_summary_by_interval(  # pylint: disable=too-many-arguments,unused-argument
        workload: CSWorkloadTypes, path: str, start_time: int | float, end_time: int | float, interval=TIME_INTERVAL,
        absolute_time=True, tag_type: CSHistogramTagTypes = CSHistogramTagTypes.LATENCY) -> list[dict[str, dict[str, int]]]:
    """
    Build set of time range histograms (as list) from
    single file or files search by pattern in provided
    dir path for time range (start_time, end_time) with
    interval 'interval'. each Time range histogram will
    have results with time duration 'interval'.
    """
    builder = _CSRangeHistogramBuilder(workload, tag_type, start_time, end_time)
    builder.absolute_time = absolute_time
    return builder.build_histograms_summary_with_interval(path, interval)


def make_cs_range_histogram_summary_from_log_line(
        workload: CSWorkloadTypes, log_line: str, hst_log_start_time: float,
        tag_type: CSHistogramTagTypes = CSHistogramTagTypes.LATENCY) -> dict[str, dict[str, int]]:
    """
    Build time range histogram Summary from singe hdr log file line
    log line example:
    #[BaseTime: 1665956621.000 (seconds since epoch)]
    #[StartTime: 1665956621.000 (seconds since epoch), Sun Oct 16 21:43:41 UTC 2022]
    "StartTimestamp","Interval_Length","Interval_Max","Interval_Compressed_Histogram"
    Tag=READ-st,0.000,4.999,20.726,HISTFAAAA9d42jVUMY/kN...
    """
    builder = _CSRangeHistogramBuilder(workload, tag_type)
    return builder.build_from_log_line(log_line, hst_log_start_time)


class _CSHistogramThroughputTags(Enum):
    WRITE = "WRITE-st"
    READ = "READ-st"


_CSHISTOGRAM_TAGS_MAPPING = {
    CSHistogramTagTypes.LATENCY: {
        CSWorkloadTypes.WRITE: [CSHistogramTags.WRITE],
        CSWorkloadTypes.READ: [CSHistogramTags.READ],
        CSWorkloadTypes.MIXED: [
            CSHistogramTags.WRITE,
            CSHistogramTags.READ
        ]
    },
    CSHistogramTagTypes.THROUGHPUT: {
        CSWorkloadTypes.WRITE: [_CSHistogramThroughputTags.WRITE],
        CSWorkloadTypes.READ: [_CSHistogramThroughputTags.READ],
        CSWorkloadTypes.MIXED: [
            _CSHistogramThroughputTags.WRITE,
            _CSHistogramThroughputTags.READ
        ]
    }
}


@dataclass
class _HistorgramSummaryBase:
    start_time: str
    end_time: str
    stddev: float


def _generate_percentile_name(percentile: int):
    return f"percentile_{percentile}".replace(".", "_")


_HistorgramSummary = make_dataclass("HistorgramSummary",
                                    [(_generate_percentile_name(perc), float) for perc in PERCENTILES],
                                    bases=(_HistorgramSummaryBase,))


class _CSHistogram(HdrHistogram):
    LOWEST = 1
    HIGHEST = 24 * 3600_000_000_000
    SIGNIFICANT = 3

    def __init__(self, *args, **kwargs):
        super().__init__(lowest_trackable_value=_CSHistogram.LOWEST,
                         highest_trackable_value=_CSHistogram.HIGHEST,
                         significant_figures=_CSHistogram.SIGNIFICANT, *args, **kwargs)


@dataclass
class _CSRangeHistogram:
    start_time: float
    end_time: float
    hdr_tag: str | None
    histogram: _CSHistogram | None


class _CSRangeHistogramBuilder:
    def __init__(self, workload: CSWorkloadTypes, tag_type: CSHistogramTagTypes,
                 start_time: int = 0,
                 end_time: int = sys.maxsize):
        self.workload = workload
        self.tag_type = tag_type
        if self.workload and self.tag_type:
            self.hdr_tags = [w.value for w in _CSHISTOGRAM_TAGS_MAPPING[tag_type][workload]]
        self.start_time = start_time
        self.end_time = end_time

# defaults
        self.cs_files_pattern = "*/cs-hdr-*.hdr"
        self.absolute_time = True

    def build_histogram_summary(self, path: str) -> list[dict[str, dict[str, int]]]:
        """
        Build Range Histogram Summary from provided hdr logs files path
        """
        with ProcessPoolExecutor(max_workers=len(self.hdr_tags)) as executor:
            futures = [
                executor.submit(self.build_histogram_summary_by_tag, path, tag) for tag in self.hdr_tags]
        scan_results = {}
        for future in futures:
            if result := future.result():
                scan_results.update(result)
        return [scan_results]

    def build_histograms_summary_with_interval(self, path: str, interval=TIME_INTERVAL) -> list[dict[str, dict[str, int]]]:  # pylint: disable=too-many-locals
        """
        Build Several Range Histogram Summaries from provided hdr logs files path splitted by interval
        """
        start_ts = int(self.start_time)
        end_ts = int(self.end_time)
        if end_ts - start_ts < TIME_INTERVAL:
            window_step = int(end_ts - start_ts)
        else:
            window_step = interval or TIME_INTERVAL

        futures = []
        interval_num = 0
        start_intervals = range(start_ts, end_ts, window_step)

        max_workers = len(start_intervals)*len(self.hdr_tags)
        max_workers = PROCESS_LIMIT if max_workers > PROCESS_LIMIT else max_workers

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for start_interval in start_intervals:
                end_interval = end_ts if start_interval + window_step > end_ts else start_interval + window_step
                for hdr_tag in self.hdr_tags:
                    futures.append(executor.submit(self._build_histograms_summary_with_interval_by_tag,
                                                   path, hdr_tag, start_interval, end_interval, interval_num))
                interval_num += 1
            results = {}
            for future in futures:
                if res := future.result():
                    if res["interval_num"] not in results:
                        results[res["interval_num"]] = {}
                    results[res["interval_num"]].update(res["result"])

        keys = list(results.keys())
        keys.sort()
        summary = []
        for key in keys:
            summary.append(results[key])
        return summary

    def build_from_log_line(self, log_line: str, hst_log_start_time: float) -> dict[str, dict[str, int]] | None:
        """
        Build Range Histogram Summary from provided log_line
        """
        tag, start, interval_len, _, encoded_hist = log_line.split(",")
        tag = tag.split("=")[-1]
        if tag not in self.hdr_tags:
            raise TypeError(f"build_from_log_line: log_line has tag {tag} but expected one of {self.hdr_tags}")
        hst_start_ts = hst_log_start_time + float(start)
        hst_end_ts = hst_start_ts + float(interval_len)
        histogram = _CSHistogram()
        histogram.set_tag(tag)
        histogram.set_start_time_stamp(hst_start_ts)
        histogram.set_end_time_stamp(hst_end_ts)
        histogram.decode_and_add(encoded_hist)

        histogram = _CSRangeHistogram(start_time=hst_start_ts,
                                      end_time=hst_end_ts,
                                      histogram=histogram,
                                      hdr_tag=tag)

        return _CSRangeHistogramBuilder._get_summary_for_operation_by_hdr_tag(histogram)

    def _build_histogram_from_file(self, hdr_file: str, hdr_tag: str) -> _CSRangeHistogram:
        if not os.path.exists(hdr_file):
            LOGGER.error("File doesn't exists: %s", hdr_file)
            return _CSRangeHistogram(start_time=0, end_time=0, histogram=None, hdr_tag=None)

        hdr_reader = HistogramLogReader(hdr_file, _CSHistogram())
        histogram = _CSHistogram()
        histogram.set_tag(hdr_tag)
        while True:
            next_hist = hdr_reader.get_next_interval_histogram(range_start_time_sec=self.start_time,
                                                               range_end_time_sec=self.end_time,
                                                               absolute=self.absolute_time)
            if not next_hist:
                break
            tag = next_hist.get_tag()
            if tag == hdr_tag:
                if histogram.get_start_time_stamp() == 0:
                    histogram.set_start_time_stamp(next_hist.get_start_time_stamp())
                histogram.add(next_hist)

        return _CSRangeHistogram(start_time=self.start_time,
                                 end_time=self.end_time,
                                 histogram=histogram, hdr_tag=histogram.get_tag())

    def _get_list_of_hdr_files(self, base_path: str) -> list[str]:
        """
            find all hdr log file by pattern like glob wc
            in profided by base_path dir.
        """
        if not base_path:
            base_path = os.path.abspath(os.path.curdir)
        hdr_files = []
        for hdr_file in glob.glob(self.cs_files_pattern, root_dir=base_path, recursive=True):
            hdr_files.append(os.path.join(base_path, hdr_file))
        return hdr_files

    @staticmethod
    def _merge_range_histograms(range_histograms: list[_CSRangeHistogram]) -> _CSRangeHistogram:
        """
            Merge several time range histogram to one containg summary result.
        """
        if not range_histograms:
            return _CSRangeHistogram(start_time=0, end_time=0, histogram=None, hdr_tag=None)

        final_hst = range_histograms.pop(0)
        for hst in range_histograms:
            assert final_hst.hdr_tag == hst.hdr_tag, "histograms with different hdr tags cannot be merged"
            final_hst.histogram.add(hst.histogram)

            final_hst.start_time = min(final_hst.start_time, hst.start_time)
            final_hst.end_time = max(final_hst.end_time, hst.end_time)
        return final_hst

    def _build_histogram_from_dir(self, base_path: str, hdr_tag: str, ) -> _CSRangeHistogram:
        """
            search in dir from 'base_path' with provided pattern or
            default global pattern 'CS_HDR_FILE_WC' hdr log files
            and build Range Histogram with time interval (start_time, end_time)
            For timestamps is used absolute time in ms since epoch start
        """
        collected_histograms: list[_CSRangeHistogram] = []
        hdr_files = self._get_list_of_hdr_files(base_path)
        for hdr_file in hdr_files:
            file_range_histogram = self._build_histogram_from_file(hdr_file, hdr_tag)
            collected_histograms.append(file_range_histogram)
        return self._merge_range_histograms(collected_histograms)

    @staticmethod
    def _get_summary_for_operation_by_hdr_tag(histogram: _CSRangeHistogram) -> dict[str, dict[str, int]] | None:
        if parsed_summary := _CSRangeHistogramBuilder._convert_raw_histogram(histogram.histogram, histogram.start_time,
                                                                             histogram.end_time):
            return {histogram.hdr_tag[:-3]: asdict(parsed_summary)}
        return None

    @staticmethod
    def _convert_raw_histogram(histogram: _CSHistogram,
                               base_start_ts: float = 0.0,
                               base_end_ts: float = 0.0) -> "_HistorgramSummary":
        percentiles_data = {}
        if percentiles := histogram.get_percentile_to_value_dict(PERCENTILES):
            for perc, value in percentiles.items():
                percentiles_data[_generate_percentile_name(perc)] = round(value / 1_000_000, 2)

            return _HistorgramSummary(
                start_time=histogram.get_start_time_stamp() or base_start_ts,
                end_time=histogram.get_end_time_stamp() or base_end_ts,
                stddev=histogram.get_stddev(),
                **percentiles_data)
        return None

    def build_histogram_summary_by_tag(self, path: str, hdr_tag: str) -> dict[str, dict[str, int]] | None:
        if os.path.exists(path) and os.path.isfile(path):
            histogram = self._build_histogram_from_file(
                hdr_file=path, hdr_tag=hdr_tag)
        elif os.path.exists(path) and os.path.isdir(path):
            histogram = self._build_histogram_from_dir(
                base_path=path, hdr_tag=hdr_tag)
        else:
            return None

        return _CSRangeHistogramBuilder._get_summary_for_operation_by_hdr_tag(histogram)

    @staticmethod
    def _build_histograms_summary_with_interval_by_tag(path: str, hdr_tag: str, start_interval: int,
                                                       end_interval: int, interval_num: int) -> dict[str, Any] | None:

        result = _CSRangeHistogramBuilder(None, None, start_interval,
                                          end_interval).build_histogram_summary_by_tag(path, hdr_tag)
        if result:
            return {"interval_num": interval_num, "result": result}
        return None
