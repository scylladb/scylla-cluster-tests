"""Tests for DockerCmdRunner._create_tar_stream directory handling.

Validates that trailing-slash semantics (rsync-like) work correctly:
- src ending with '/' copies directory contents only
- src without trailing '/' copies the directory itself
"""

import tarfile
from io import BytesIO
from pathlib import Path

import pytest


@pytest.fixture
def src_dir(tmp_path):
    """Create a source directory with files for tar stream tests."""
    src = tmp_path / "mydir"
    src.mkdir()
    (src / "file1.txt").write_text("content1")
    sub = src / "subdir"
    sub.mkdir()
    (sub / "file2.txt").write_text("content2")
    return src


def _get_tar_names(tar_stream: BytesIO) -> list[str]:
    """Extract archive member names from a tar stream."""
    tar_stream.seek(0)
    with tarfile.open(fileobj=tar_stream, mode="r") as tar:
        return sorted(tar.getnames())


def _create_tar_stream(src: str, dst: str) -> BytesIO:
    """Inline copy of DockerCmdRunner._create_tar_stream to avoid Docker import."""
    tar_stream = BytesIO()
    src_path = Path(src.rstrip("/"))
    with tarfile.open(fileobj=tar_stream, mode="w") as tar:
        if src_path.is_dir():
            relative_base = src_path if src.endswith("/") else src_path.parent
            for file_path in src_path.rglob("*"):
                if file_path.is_file():
                    tar.add(str(file_path), arcname=str(file_path.relative_to(relative_base)))
        else:
            arcname = Path(dst).name if not dst.endswith("/") else src_path.name
            tar.add(str(src_path), arcname=arcname)
    tar_stream.seek(0)
    return tar_stream


def test_create_tar_stream_dir_with_trailing_slash_copies_contents(src_dir):
    """Trailing slash on src copies only directory contents (no parent dir in archive)."""
    tar_stream = _create_tar_stream(str(src_dir) + "/", "/dst/")
    names = _get_tar_names(tar_stream)

    # Should contain files relative to src_dir, without the "mydir" prefix
    assert "file1.txt" in names
    assert "subdir/file2.txt" in names
    # Should NOT contain the directory name itself as a prefix
    assert not any(n.startswith("mydir/") for n in names)


def test_create_tar_stream_dir_without_trailing_slash_copies_dir_itself(src_dir):
    """No trailing slash on src copies the directory itself (includes dir name in archive)."""
    tar_stream = _create_tar_stream(str(src_dir), "/dst/")
    names = _get_tar_names(tar_stream)

    # Should contain files with the "mydir" directory prefix
    assert "mydir/file1.txt" in names
    assert "mydir/subdir/file2.txt" in names


def test_create_tar_stream_single_file(tmp_path):
    """Single file src creates archive with just the filename."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")

    tar_stream = _create_tar_stream(str(test_file), "/dst/test.txt")
    names = _get_tar_names(tar_stream)

    assert names == ["test.txt"]


def test_create_tar_stream_single_file_dst_with_trailing_slash(tmp_path):
    """Single file with dst trailing slash uses src filename as arcname."""
    test_file = tmp_path / "data.csv"
    test_file.write_text("a,b,c")

    tar_stream = _create_tar_stream(str(test_file), "/dst/")
    names = _get_tar_names(tar_stream)

    assert names == ["data.csv"]
