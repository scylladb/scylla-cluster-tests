import os
import shutil
import subprocess
import sys
import tarfile

import pytest

from sdcm.utils.sstable.load_utils import untar_cmd


@pytest.mark.parametrize("has_tar", [pytest.param(True, id="tar"), pytest.param(False, id="no-tar-python-fallback")])
def test_untar_cmd_extracts(tmp_path, has_tar):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "md-1-big-Data.db").write_text("data")
    archive = tmp_path / "sstables.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(tmp_path / "src" / "md-1-big-Data.db", arcname="md-1-big-Data.db")
    (tmp_path / "dst").mkdir()
    # NOTE: a PATH holding only 'sh' (and 'tar' when wanted), and the local python in place of scylla's
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("sh", "tar", "gzip") if has_tar else ("sh",):
        os.symlink(shutil.which(tool), bin_dir / tool)
    cmd = untar_cmd(str(archive), str(tmp_path / "dst")).replace("/opt/scylladb/python3/bin/python3", sys.executable)
    subprocess.run(cmd, shell=True, check=True, executable=shutil.which("sh"), env={"PATH": str(bin_dir)})
    assert (tmp_path / "dst" / "md-1-big-Data.db").read_text() == "data"
