"""Tests for leaving the warm-up of every HDR file out of the HDR summary (SCT-1402)."""

import pytest
from hdrh.histogram import HdrHistogram

from sdcm.utils.hdrhistogram import make_hdrhistogram_summary

BASE_TIME = 1_700_000_000
WARMUP_LATENCY_NS = 50_000_000
STEADY_LATENCY_NS = 1_000_000
COUNT_PER_INTERVAL = 1000
TAG = "WRITE-st"
SUMMARY_KEY = f"WRITE--{TAG}"


def _write_hdr_file(path, start_offset_sec, intervals, warmup_intervals, interval_sec=1, jitter_sec=0.0):
    """
    Write one cassandra-stress like HDR log: slow during the warm-up, fast after it.

    jitter_sec is taken off every interval start but the first, as cassandra-stress does
    (3.000, 12.999, 22.999...).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as hdr_file:
        hdr_file.write(f"#[StartTime: {BASE_TIME + start_offset_sec}.000 (seconds since epoch), test]\n")
        hdr_file.write('"StartTimestamp","Interval_Length","Interval_Max","Interval_Compressed_Histogram"\n')
        for interval in range(intervals):
            histogram = HdrHistogram(1, 24 * 3600 * 1_000_000_000, 3)
            latency = WARMUP_LATENCY_NS if interval < warmup_intervals else STEADY_LATENCY_NS
            histogram.record_value(latency, int(COUNT_PER_INTERVAL * interval_sec))
            start = interval * interval_sec - (jitter_sec if interval else 0)
            hdr_file.write(
                f"Tag={TAG},{start:.3f},{interval_sec:.3f},{latency / 1_000_000:.3f},{histogram.encode().decode()}\n"
            )


def _summary(base_path, warmup_sec):
    result = make_hdrhistogram_summary(
        hdr_tags=[TAG],
        stress_operation="write",
        start_time=BASE_TIME - 60,
        end_time=BASE_TIME + 600,
        base_path=str(base_path),
        warmup_sec=warmup_sec,
    )
    return result[0][SUMMARY_KEY]


@pytest.fixture
def one_loader(tmp_path):
    _write_hdr_file(tmp_path / "loader-1" / "hdrh-1.hdr", start_offset_sec=0, intervals=120, warmup_intervals=30)
    return tmp_path


def test_no_warmup_keeps_the_whole_window(one_loader):
    summary = _summary(one_loader, warmup_sec=0)

    assert summary["start_time"] == BASE_TIME * 1000
    assert summary["percentile_99"] == WARMUP_LATENCY_NS / 1_000_000
    assert summary["throughput"] == COUNT_PER_INTERVAL


def test_warmup_is_left_out_of_the_summary(one_loader):
    summary = _summary(one_loader, warmup_sec=30)

    assert summary["start_time"] == (BASE_TIME + 30) * 1000
    assert summary["end_time"] == (BASE_TIME + 120) * 1000
    assert summary["percentile_99_999"] == STEADY_LATENCY_NS / 1_000_000
    # the throughput is computed over the trimmed window, not the whole step
    assert summary["throughput"] == COUNT_PER_INTERVAL


def test_warmup_does_not_change_the_hdr_file(one_loader):
    hdr_file = one_loader / "loader-1" / "hdrh-1.hdr"
    content = hdr_file.read_text(encoding="utf-8")

    _summary(one_loader, warmup_sec=30)

    assert hdr_file.read_text(encoding="utf-8") == content


def test_each_file_is_trimmed_from_its_own_first_interval(tmp_path):
    # the second loader starts its load 10s later: its warm-up ends 10s later too
    _write_hdr_file(tmp_path / "loader-1" / "hdrh-1.hdr", start_offset_sec=0, intervals=120, warmup_intervals=30)
    _write_hdr_file(tmp_path / "loader-2" / "hdrh-2.hdr", start_offset_sec=10, intervals=120, warmup_intervals=30)

    summary = _summary(tmp_path, warmup_sec=30)

    assert summary["start_time"] == (BASE_TIME + 30) * 1000
    assert summary["end_time"] == (BASE_TIME + 130) * 1000
    assert summary["percentile_99_999"] == STEADY_LATENCY_NS / 1_000_000


def test_short_file_is_kept_whole(tmp_path):
    # 50s of data is not more than twice the 30s warm-up: trimming would leave too little
    _write_hdr_file(tmp_path / "loader-1" / "hdrh-1.hdr", start_offset_sec=0, intervals=50, warmup_intervals=30)

    summary = _summary(tmp_path, warmup_sec=30)

    assert summary["start_time"] == BASE_TIME * 1000
    assert summary["percentile_99"] == WARMUP_LATENCY_NS / 1_000_000


def test_file_with_nothing_after_the_warmup_is_kept_whole(tmp_path):
    # one 90s interval starts inside the 30s warm-up: trimming it would leave no summary at all
    _write_hdr_file(
        tmp_path / "loader-1" / "hdrh-1.hdr", start_offset_sec=0, intervals=1, warmup_intervals=1, interval_sec=90
    )

    summary = _summary(tmp_path, warmup_sec=30)

    assert summary["start_time"] == BASE_TIME * 1000
    assert summary["percentile_99"] == WARMUP_LATENCY_NS / 1_000_000


def test_interval_jitter_does_not_extend_the_warmup(tmp_path):
    # cassandra-stress: 10s intervals stamped 0.000, 9.999, 19.999, 29.999... - the one 29.999s in is past the
    # 30s warm-up, not an interval more of it
    _write_hdr_file(
        tmp_path / "loader-1" / "hdrh-1.hdr",
        start_offset_sec=0,
        intervals=18,
        warmup_intervals=3,
        interval_sec=10,
        jitter_sec=0.001,
    )

    summary = _summary(tmp_path, warmup_sec=30)

    assert summary["start_time"] == (BASE_TIME + 29.999) * 1000
    assert summary["percentile_99_999"] == STEADY_LATENCY_NS / 1_000_000


def test_interval_half_a_second_before_the_warmup_end_is_left_out(tmp_path):
    # 0.5s intervals: the one 29.5s in still holds warm-up data, unlike cassandra-stress' 1ms early stamps
    _write_hdr_file(
        tmp_path / "loader-1" / "hdrh-1.hdr", start_offset_sec=0, intervals=240, warmup_intervals=60, interval_sec=0.5
    )

    summary = _summary(tmp_path, warmup_sec=30)

    assert summary["start_time"] == (BASE_TIME + 30) * 1000
    assert summary["percentile_99_999"] == STEADY_LATENCY_NS / 1_000_000
