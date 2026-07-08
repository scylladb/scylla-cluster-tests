# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import os
import shutil
import subprocess
import threading

import pytest

pytestmark = pytest.mark.skipif(not shutil.which("zstd"), reason="zstd binary not found")

LOG_ARCHIVE_SCRIPT = os.path.join(os.path.dirname(__file__), "../../sdcm/log_archive.sh")


def generate_fake_sct_log(path, num_lines=10000, line_size=200):
    """Generate a fake sct.log with timestamped lines matching SCT log format."""
    with open(path, "w") as f:
        for i in range(num_lines):
            hour = (i // 3600) % 24
            minute = (i // 60) % 60
            second = i % 60
            padding = "x" * (line_size - 50)
            f.write(f"<1> t:2026-05-24T{hour:02d}:{minute:02d}:{second:02d},{i % 1000:03d} {padding}\n")


@pytest.fixture()
def large_log_file(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=50000, line_size=220)
    return log_file


def test_splits_file_into_expected_chunks(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=1000, line_size=200)

    file_size = os.path.getsize(log_file)
    chunk_size = file_size // 3

    result = subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    zst_files = sorted(tmp_path.glob("*.sct.log.zst"))
    assert len(zst_files) >= 3
    assert len(zst_files) <= 4


def test_output_files_are_valid_zstd(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=500, line_size=100)

    file_size = os.path.getsize(log_file)
    chunk_size = file_size // 2

    subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "test.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )

    zst_files = list(tmp_path.glob("*.test.log.zst"))
    assert len(zst_files) >= 2

    for zst_file in zst_files:
        result = subprocess.run(
            ["zstd", "-t", str(zst_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, f"Invalid zstd file: {zst_file}"


def test_decompressed_content_matches_original(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=300, line_size=100)

    original_content = log_file.read_text()
    file_size = os.path.getsize(log_file)
    chunk_size = file_size // 2

    subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )

    zst_files = sorted(tmp_path.glob("*.sct.log.zst"))
    reconstructed = ""
    for zst_file in zst_files:
        result = subprocess.run(
            ["zstd", "-d", "--stdout", str(zst_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0
        reconstructed += result.stdout

    assert reconstructed == original_content


def test_timestamp_in_output_filenames(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=200, line_size=100)

    file_size = os.path.getsize(log_file)
    chunk_size = file_size // 2

    subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )

    zst_files = list(tmp_path.glob("*.sct.log.zst"))
    for zst_file in zst_files:
        name = zst_file.name.replace(".sct.log.zst", "")
        assert "2026_05_24" in name, f"Expected timestamp in filename: {zst_file.name}"


def test_handles_single_chunk_when_file_smaller_than_chunk_size(tmp_path):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=100, line_size=50)

    file_size = os.path.getsize(log_file)
    chunk_size = file_size * 2

    result = subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0

    zst_files = list(tmp_path.glob("*.sct.log.zst"))
    assert len(zst_files) == 1


@pytest.mark.parametrize("num_chunks", [2, 4, 8])
def test_various_chunk_counts(tmp_path, num_chunks):
    log_file = tmp_path / "sct.log"
    generate_fake_sct_log(str(log_file), num_lines=2000, line_size=100)

    file_size = os.path.getsize(log_file)
    chunk_size = file_size // num_chunks

    result = subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0

    zst_files = list(tmp_path.glob("*.sct.log.zst"))
    assert len(zst_files) >= num_chunks
    assert len(zst_files) <= num_chunks + 1


def test_full_split_compress_and_data_integrity(tmp_path, large_log_file):
    original_content = large_log_file.read_bytes()
    file_size = len(original_content)
    chunk_size = file_size // 4

    result = subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(large_log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"Script failed: {result.stderr}"

    zst_files = sorted(tmp_path.glob("*.sct.log.zst"))
    assert len(zst_files) >= 4

    reconstructed = b""
    for zst_file in zst_files:
        decomp = subprocess.run(
            ["zstd", "-d", "--stdout", str(zst_file)],
            capture_output=True,
            check=False,
        )
        assert decomp.returncode == 0, f"Failed to decompress {zst_file}"
        reconstructed += decomp.stdout

    assert reconstructed == original_content


def test_disk_efficiency_original_not_duplicated(tmp_path, large_log_file):
    file_size = os.path.getsize(large_log_file)
    chunk_size = file_size // 3

    subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(large_log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )

    part_files = list(tmp_path.glob("chunk_*"))
    assert len(part_files) == 0, f"Raw chunks should be removed after compression: {part_files}"


def test_many_chunks_produce_valid_archives(tmp_path, large_log_file):
    file_size = os.path.getsize(large_log_file)
    chunk_size = file_size // 8

    subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(large_log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=True,
    )

    zst_files = list(tmp_path.glob("*.sct.log.zst"))
    assert len(zst_files) >= 8

    for zst_file in zst_files:
        result = subprocess.run(["zstd", "-t", str(zst_file)], capture_output=True, text=True, check=False)
        assert result.returncode == 0, f"Corrupt archive: {zst_file}"


def test_peak_work_dir_size_bounded_to_single_chunk(tmp_path, large_log_file):
    """Assert peak temp-dir size stays bounded — no raw chunks accumulate.

    This is the core regression test for the streaming rewrite: the old approach
    wrote all split chunks to WORK_DIR before compressing (peak = full file size),
    while the new --filter approach pipes directly (peak ~ 0).
    """

    file_size = os.path.getsize(large_log_file)
    chunk_size = file_size // 4

    work_dir = tmp_path / "work"
    work_dir.mkdir()

    max_observed = [0]
    stop_event = threading.Event()

    def monitor_work_dir():
        while not stop_event.is_set():
            try:
                total = sum(f.stat().st_size for f in work_dir.iterdir() if f.is_file())
                max_observed[0] = max(max_observed[0], total)
            except (FileNotFoundError, OSError):
                pass
            stop_event.wait(0.005)

    monitor = threading.Thread(target=monitor_work_dir, daemon=True)
    monitor.start()

    env = os.environ.copy()
    env["LOG_ARCHIVE_WORK_DIR"] = str(work_dir)

    result = subprocess.run(
        ["bash", LOG_ARCHIVE_SCRIPT, str(large_log_file), str(chunk_size), "sct.log"],
        cwd=str(tmp_path),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    stop_event.set()
    monitor.join(timeout=2)

    assert result.returncode == 0, f"Script failed: {result.stderr}"

    # With streaming, the work dir should never hold raw chunks.
    # Allow a small margin for filesystem metadata / directory entries.
    assert max_observed[0] < chunk_size, (
        f"Peak work-dir usage was {max_observed[0]} bytes — expected < {chunk_size} (one chunk). "
        f"Raw chunks are being written to disk instead of streamed."
    )
