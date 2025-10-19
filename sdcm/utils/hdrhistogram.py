import concurrent
import glob
import os.path
import time
import logging
import multiprocessing
import traceback
from typing import Any
from dataclasses import asdict, dataclass, make_dataclass
from concurrent.futures.process import ProcessPoolExecutor

from hdrh.histogram import HdrHistogram
from hdrh.log import HistogramLogReader

LOGGER = logging.getLogger(__file__)

PROCESS_LIMIT = multiprocessing.cpu_count()
TIME_INTERVAL = 600
PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]
FUTURE_RESULT_TIMEOUT = 60  # seconds


def make_hdrhistogram_summary(
        hdr_tags: list[str], stress_operation: str,
        start_time: int | float, end_time: int | float,
        pattern: str = "", base_path="", absolute_time: bool = True) -> list[dict[str, dict[str, int]]]:
    """
    Build time range HDR Histogram summary with time interval (start_time, end_time)
    from provided hdr log file.
    For timestamps is used absolute time in ms since epoch start
    """
    builder = _HdrRangeHistogramBuilder(
        hdr_tags=hdr_tags,
        stress_operation=stress_operation,
        start_time=start_time,
        end_time=end_time,
    )
    if pattern:
        builder.hdrh_files_pattern = pattern
    builder.absolute_time = absolute_time
    return builder.build_histogram_summary(base_path)


def make_hdrhistogram_summary_by_interval(
        hdr_tags: list[str], stress_operation: str,
        path: str,
        start_time: int | float, end_time: int | float,
        interval: int | float = TIME_INTERVAL, absolute_time: bool = True) -> list[dict[str, dict[str, int]]]:
    """
    Build set of time range HDR histograms (as list) from
    single file or files search by pattern in provided
    dir path for time range (start_time, end_time) with
    interval 'interval'. each Time range histogram will
    have results with time duration 'interval'.
    """
    builder = _HdrRangeHistogramBuilder(
        hdr_tags=hdr_tags,
        stress_operation=stress_operation,
        start_time=start_time,
        end_time=end_time,
    )
    builder.absolute_time = absolute_time
    return builder.build_histograms_summary_with_interval(path, interval)


def make_hdrhistogram_summary_from_log_line(
        hdr_tags: list[str], stress_operation: str,
        log_line: str, hst_log_start_time: float) -> dict[str, dict[str, int]]:
    """
    Build time range HDR histogram summary from a single hdr log file line. Example:

    #[BaseTime: 1665956621.000 (seconds since epoch)]
    #[StartTime: 1665956621.000 (seconds since epoch), Sun Oct 16 21:43:41 UTC 2022]
    "StartTimestamp","Interval_Length","Interval_Max","Interval_Compressed_Histogram"
    Tag=READ-st,0.000,4.999,20.726,HISTFAAAA9d42jVUMY/kN...
    """
    now, time_deviation = time.time(), 60 * 60 * 24
    builder = _HdrRangeHistogramBuilder(
        hdr_tags=hdr_tags,
        stress_operation=stress_operation,
        start_time=now - time_deviation,
        end_time=now + time_deviation,
    )
    return builder.build_from_log_line(log_line, hst_log_start_time)


@dataclass
class _HistorgramSummaryBase:
    start_time: str
    end_time: str
    stddev: float


def _generate_percentile_name(percentile: int):
    return f"percentile_{percentile}".replace(".", "_")


_HistorgramSummary = make_dataclass("HistorgramSummary",
                                    [(_generate_percentile_name(perc), float)
                                     for perc in PERCENTILES] + [("throughput", int)],
                                    bases=(_HistorgramSummaryBase,))


class _HdrHistogram(HdrHistogram):
    LOWEST = 1
    HIGHEST = 24 * 3600_000_000_000
    SIGNIFICANT = 3

    def __init__(self, *args, **kwargs):
        super().__init__(lowest_trackable_value=_HdrHistogram.LOWEST,
                         highest_trackable_value=_HdrHistogram.HIGHEST,
                         significant_figures=_HdrHistogram.SIGNIFICANT, *args, **kwargs)


@dataclass
class _HdrRangeHistogram:
    start_time: float
    end_time: float
    hdr_tag: str | None
    histogram: _HdrHistogram | None


class _HdrRangeHistogramBuilder:
    def __init__(self, hdr_tags: list[str], stress_operation: str,
                 start_time: int | float, end_time: int | float,
                 hdr_file_pattern: str = "*/hdrh-*.hdr"):
        self.hdr_tags = hdr_tags
        self.stress_operation = stress_operation.upper().strip()
        self.start_time = start_time
        self.end_time = end_time
        self.hdrh_files_pattern = hdr_file_pattern
        self.absolute_time = True

    def build_histogram_summary(self, path: str) -> list[dict[str, dict[str, int]]]:
        """
        Build Range Histogram Summary from provided hdr logs files path
        """

        LOGGER.debug(f'Building histogram summary for {path} with tags {self.hdr_tags}')
        try:
            with ProcessPoolExecutor(max_workers=len(self.hdr_tags)) as executor:
                futures = [
                    executor.submit(self.build_histogram_summary_by_tag, path, tag) for tag in self.hdr_tags]
                scan_results = {}
                for e, future in enumerate(concurrent.futures.as_completed(futures, timeout=120)):
                    result = future.result()
                    LOGGER.debug(f"Got result for {e} future for tag {self.hdr_tags[e]}")
                    if result:
                        scan_results.update(result)
            return [scan_results]
        except Exception as e:
            LOGGER.error(f"Error building histogram summary for {path} with tags {self.hdr_tags}: {e}")
            raise
        finally:
            LOGGER.debug(f'Finished building histogram summary for {path} with tags {self.hdr_tags}')

    def build_histograms_summary_with_interval(self, path: str,
                                               interval=TIME_INTERVAL) -> list[dict[str, dict[str, int]]]:
        """
        Build Several Range Histogram Summaries from provided hdr logs files path splitted by interval
        """

        LOGGER.debug(f'Building histogram summary for {path} with tags {self.hdr_tags}')
        try:
            start_ts = int(self.start_time)
            end_ts = int(self.end_time)
            if end_ts - start_ts < interval:
                window_step = int(end_ts - start_ts)
            else:
                window_step = interval or TIME_INTERVAL

            futures = []
            interval_num = 0
            start_intervals = range(start_ts, end_ts, window_step)
            LOGGER.debug(
                f'Building histogram summary for {path} with tags {self.hdr_tags} and intervals {len(start_intervals)}')

            max_workers = len(start_intervals)*len(self.hdr_tags)
            max_workers = PROCESS_LIMIT if max_workers > PROCESS_LIMIT else max_workers

            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                for start_interval in start_intervals:
                    end_interval = end_ts if start_interval + window_step > end_ts else start_interval + window_step
                    for hdr_tag in self.hdr_tags:
                        futures.append(executor.submit(self._build_histograms_summary_with_interval_by_tag,
                                                       path, hdr_tag, start_interval, end_interval, interval_num))
                    LOGGER.debug(
                        f"Submitted future for tags {self.hdr_tags} and interval {interval_num} out of {len(start_intervals)}")
                    interval_num += 1
                results = {}
                for e, future in enumerate(concurrent.futures.as_completed(futures, timeout=120)):
                    res = future.result(timeout=FUTURE_RESULT_TIMEOUT)  # Will raise TimeoutError after 60 seconds
                    LOGGER.debug(
                        f"Got result for {e} future for tag {self.hdr_tags[e % len(self.hdr_tags)]} and interval {e // len(self.hdr_tags)}")
                    if res:
                        LOGGER.debug("Got result for future: interval_num=%s, result_keys=%s", res.get("interval_num"),
                                     list(res.get("result", {}).keys()))
                        if res["interval_num"] not in results:
                            results[res["interval_num"]] = {}
                        results[res["interval_num"]].update(res["result"])
                        LOGGER.debug("Updated results for future: interval_num=%s", res.get("interval_num"))

            LOGGER.debug("Finalised results for path %s", path)
            keys = list(results.keys())
            LOGGER.debug("Finalised results keys for path %s: %s", path, keys)
            keys.sort()
            summary = []
            for key in keys:
                LOGGER.debug("Adding results for key %s", key)
                summary.append(results[key])
                LOGGER.debug("Added results for key %s", key)
            return summary
        except TimeoutError:
            LOGGER.error(f"TimeoutError getting future result for {path} with tags {self.hdr_tags}")
            raise
        except Exception as e:
            LOGGER.error(f"Error building histogram summary for {path} with tags {self.hdr_tags}: {e}")
            raise
        finally:
            LOGGER.debug(f'Finished building histogram summary for {path} with tags {self.hdr_tags}')

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
        histogram = _HdrHistogram()
        histogram.set_tag(tag)
        histogram.set_start_time_stamp(hst_start_ts)
        histogram.set_end_time_stamp(hst_end_ts)
        histogram.decode_and_add(encoded_hist)

        histogram = _HdrRangeHistogram(
            start_time=hst_start_ts, end_time=hst_end_ts, histogram=histogram, hdr_tag=tag)

        return self._get_summary_for_operation_by_hdr_tag(histogram)

    def _build_histogram_from_file(self, hdr_file: str, hdr_tag: str) -> _HdrRangeHistogram | None:
        def analyze_hdr_file():
            """
            :return: tuple(tag_not_found, file_with_correct_time_interval)
            tag_not_found: bool - row with HDR tag (like "WRITE-st") is not found in the file
            file_with_correct_time_interval: bool - HDR file keep the data for different time interval
            """
            hdr_reader = HistogramLogReader(hdr_file, _HdrHistogram())
            if not (next_hist := hdr_reader.get_next_interval_histogram(range_start_time_sec=self.start_time,
                                                                        range_end_time_sec=self.end_time,
                                                                        absolute=self.absolute_time)):
                LOGGER.debug(
                    f'no histogram entry found in file {hdr_file} starting from {self.start_time} ending at {self.end_time} absolute_time={self.absolute_time}')
                return True, False

            tag_not_found = True
            file_with_correct_time_interval = True
            index_errors = 0
            line_index = 5
            while next_hist:
                tag = next_hist.get_tag()
                # The tag in the HDR file for the stress command with the user profile is in lowercase.
                # Modify the tag validation to perform a case-insensitive comparison.
                if tag.lower() == hdr_tag.lower():
                    if tag_not_found:
                        LOGGER.debug(f'found histogram entry with tag {hdr_tag} in file {hdr_file}')
                    if histogram.get_start_time_stamp() == 0:
                        histogram.set_start_time_stamp(next_hist.get_start_time_stamp())
                    histogram.add(next_hist)
                    tag_not_found = False

                while next_hist:
                    line_index += 1
                    try:
                        next_hist = hdr_reader.get_next_interval_histogram(range_start_time_sec=self.start_time,
                                                                           range_end_time_sec=self.end_time,
                                                                           absolute=self.absolute_time)
                    except IndexError:
                        LOGGER.warning(f'IndexError in file {hdr_file} near line {line_index}, skipping this line')
                        index_errors += 1
                        continue
                    break
            if index_errors > 0:
                LOGGER.warning(f'{index_errors} lines were ignored in file {hdr_file} due to IndexError')
            LOGGER.debug(
                f'Finished reading file {hdr_file} with tag {hdr_tag}, tag_not_found={tag_not_found} file_with_correct_time_interval={file_with_correct_time_interval}')
            return tag_not_found, file_with_correct_time_interval

        if not os.path.exists(hdr_file):
            LOGGER.error("File doesn't exists: %s", hdr_file)
            return _HdrRangeHistogram(start_time=0, end_time=0, histogram=None, hdr_tag=None)

        LOGGER.debug("Parsing file: %s tag %s", hdr_file, hdr_tag)
        histogram = _HdrHistogram()
        histogram.set_tag(hdr_tag)
        try:
            _, file_with_correct_time_interval = analyze_hdr_file()
        except Exception as e:
            LOGGER.error("Failed to parse file %s with tag %s: %s", hdr_file, hdr_tag, e)
            LOGGER.error(traceback.format_exc())
            raise
        if not file_with_correct_time_interval:
            LOGGER.debug(
                f'The file {hdr_file} does not include the time interval from `{self.start_time}` to `{self.end_time}`')
            # Keep this message for future debug
            LOGGER.debug("The file '%s' does not include the time interval from `%s` to `%s`",
                         hdr_file, self.start_time, self.end_time)
            return None

        LOGGER.debug(f'Collected histogram from file {hdr_file} with tag {hdr_tag}: {histogram}')
        # Keep this message for future debug
        LOGGER.debug("Collect data from the file '%s' (time interval from `%s` to `%s`)",
                     hdr_file, self.start_time, self.end_time)
        if histogram.get_start_time_stamp() == 0:
            return None

        return _HdrRangeHistogram(
            start_time=self.start_time, end_time=self.end_time, histogram=histogram, hdr_tag=histogram.get_tag())

    def _get_list_of_hdr_files(self, base_path: str) -> list[str]:
        """
            find all hdr log file by pattern like glob wc
            in profided by base_path dir.
        """
        if not base_path:
            base_path = os.path.abspath(os.path.curdir)
        LOGGER.debug(f'Looking for hdr files in {base_path} with pattern {self.hdrh_files_pattern}')
        hdr_files = []
        for hdr_file in glob.glob(self.hdrh_files_pattern, root_dir=base_path, recursive=True):
            try:
                size = os.stat(os.path.join(base_path, hdr_file)).st_size
            except:
                size = -1
            LOGGER.debug(f'Found hdr file {os.path.join(base_path, hdr_file)} in {base_path}, size {size}')
            hdr_files.append(os.path.join(base_path, hdr_file))
        return hdr_files

    @staticmethod
    def _merge_range_histograms(range_histograms: list[_HdrRangeHistogram]) -> _HdrRangeHistogram:
        """
            Merge several time range histogram to one containg summary result.
        """
        if not range_histograms:
            return _HdrRangeHistogram(start_time=0, end_time=0, histogram=None, hdr_tag=None)

        final_hst = range_histograms.pop(0)
        for hst in range_histograms:
            assert final_hst.hdr_tag == hst.hdr_tag, "histograms with different hdr tags cannot be merged"
            final_hst.histogram.add(hst.histogram)

            final_hst.start_time = min(final_hst.start_time, hst.start_time)
            final_hst.end_time = max(final_hst.end_time, hst.end_time)
        LOGGER.debug(f'Merged histograms with tag {final_hst.hdr_tag} result {final_hst}')
        return final_hst

    def _build_histogram_from_dir(self, base_path: str, hdr_tag: str, ) -> _HdrRangeHistogram:
        """
            search in dir from 'base_path' with provided pattern or
            default global pattern 'CS_HDR_FILE_WC' hdr log files
            and build Range Histogram with time interval (start_time, end_time)
            For timestamps is used absolute time in ms since epoch start
        """
        collected_histograms: list[_HdrRangeHistogram] = []
        hdr_files = self._get_list_of_hdr_files(base_path)
        for hdr_file in hdr_files:
            if os.stat(hdr_file).st_size == 0:
                LOGGER.error("File %s is empty", hdr_file)
                continue

            file_range_histogram = self._build_histogram_from_file(hdr_file, hdr_tag)
            LOGGER.debug(f'Collected histogram from file {hdr_file} with tag {hdr_tag}: {file_range_histogram}')
            if file_range_histogram:
                collected_histograms.append(file_range_histogram)
        return self._merge_range_histograms(collected_histograms)

    def _get_workload_type_by_hdr_tag(self, hdr_tag):
        # NOTE: different benchmarking tools have completly different approaches for HDR tag usages.
        #
        # 1) 'cassandra-stress' uses "WRITE-rt" and "READ-rt" tags for coordinated omission fixed latencies.
        #    It is when "-rate 'fixed=100/s'" is specified.
        #    In all other cases it's tags are coordinated omission affected latencies with
        #    the 'WRITE-st' and 'READ-st' tags.
        # 2) 'latte' may have arbitrary tag names, they are based on the user-defined rune function names.
        #    Examples: 'fn--write', 'fn--write-batch', 'fn--get', 'fn--get-many', 'fn--read'.
        # 3) 'scylla-bench' has identical tag names for reads and writes - 'co-fixed' and 'raw'.
        #    It doesn't have 'mixed' workload type, so it's mode should be used for detecting the tag data type.
        # 4) 'ycsb', it supports HDR histograms. YCSB has been upgraded with patch to enable tagging.
        #    It uses SCAN READ SCAN UPDATE INSERT DELETE WRITE tags - each tag will create a different hdr file.
        #    First two ifs will handle all ycsb tags and return correct workload type.

        hdr_tag = hdr_tag.lower().strip()
        LOGGER.debug(f'Checking hdr_tag {hdr_tag} for workload type detection')
        if any(w_word in hdr_tag for w_word in ("write", "insert", "update", "delete")):
            return "WRITE"
        elif any(r_word in hdr_tag for r_word in ("read", "select", "get", "count", "scan")):
            return "READ"
        elif self.stress_operation in ("WRITE", "READ"):
            # branch for the scylla-bench case with its 'co-fixed' and 'raw' tags
            return self.stress_operation
        # NOTE: following exception raising is not expected in the properly configured test scenarios
        raise ValueError(f"Failed to detect the workload type for the following hdr_tag: {hdr_tag}")

    def _get_summary_for_operation_by_hdr_tag(self, histogram: _HdrRangeHistogram) -> dict[str, dict[str, int]] | None:
        if histogram.histogram and (parsed_summary := self._convert_raw_histogram(
                histogram.histogram, histogram.start_time, histogram.end_time)):
            actual_workload_type = self._get_workload_type_by_hdr_tag(histogram.hdr_tag)
            return {f"{actual_workload_type}--{histogram.hdr_tag}": asdict(parsed_summary)}
        return None

    @staticmethod
    def _convert_raw_histogram(histogram: _HdrHistogram,
                               base_start_ts: float = 0.0,
                               base_end_ts: float = 0.0) -> "_HistorgramSummary":
        percentiles_data = {}
        if percentiles := histogram.get_percentile_to_value_dict(PERCENTILES):
            for perc, value in percentiles.items():
                percentiles_data[_generate_percentile_name(perc)] = round(value / 1_000_000, 2)
            percentiles_data["throughput"] = round(histogram.get_total_count(
            ) / ((histogram.get_end_time_stamp() - histogram.get_start_time_stamp()) / 1000))
            return _HistorgramSummary(
                start_time=histogram.get_start_time_stamp() or base_start_ts,
                end_time=histogram.get_end_time_stamp() or base_end_ts,
                stddev=histogram.get_stddev(),
                **percentiles_data)
        return None

    def build_histogram_summary_by_tag(self, path: str, hdr_tag: str) -> dict[str, dict[str, int]] | None:
        LOGGER.debug(f'Running build_histogram_summary_by_tag for tag {hdr_tag} in path {path}')
        if os.path.exists(path) and os.path.isfile(path):
            histogram = self._build_histogram_from_file(hdr_file=path, hdr_tag=hdr_tag)
            if not histogram:
                LOGGER.info(f"File {path} didn't generate histogram for tag {hdr_tag}")
                return None
        elif os.path.exists(path) and os.path.isdir(path):
            histogram = self._build_histogram_from_dir(base_path=path, hdr_tag=hdr_tag)
        else:
            LOGGER.info(f"No histogram for path {path}  - not a file or directory")
            return None

        val = self._get_summary_for_operation_by_hdr_tag(histogram)
        LOGGER.debug(f'Path {path} generated histogram for tag {hdr_tag}: {val}')
        return val

    def _build_histograms_summary_with_interval_by_tag(
            self, path: str, hdr_tag: str,
            start_interval: int, end_interval: int,
            interval_num: int) -> dict[str, Any] | None:
        result = _HdrRangeHistogramBuilder(
            hdr_tags=[hdr_tag],
            stress_operation=self.stress_operation,
            start_time=start_interval,
            end_time=end_interval,
        ).build_histogram_summary_by_tag(path, hdr_tag)
        if result:
            return {"interval_num": interval_num, "result": result}
        return None
