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

"""Integration tests for `grafana_screenshot_panels` against a real monitoring stack.

External services: Docker (the scylla-monitoring stack -- Grafana, the Grafana image
renderer and Prometheus), plus GitHub to download the monitoring sources.

`GrafanaScreenShot` resolves a configured panel through three things only a real
scylla-monitoring Grafana can answer:

* whether the configured titles actually match a dashboard and a panel -- they are substring
  matches against titles SCT does not own, and one of the two panels sits inside a collapsed
  row, where a non-recursive lookup would find nothing;
* whether the numeric ``panelId`` the search yields is the one the render endpoint wants;
* whether ``/render/d-solo/...`` answers with a PNG at the requested size, which is the
  whole point of the feature and is served by the renderer container, not by Grafana.

The stack is the same one `BaseMonitorSet.start_scylla_monitoring` runs on a monitor node:
the `monitor_branch` sources from `defaults/test_default.yaml`, started by their own
``start-all.sh`` with the renderer enabled. The whole module shares one stack and runs on a
single xdist worker -- the renderer container and the docker network have fixed names, so two
stacks cannot coexist. Grafana and its renderer are ~4GB of images together, which is a large
bite out of the SCT runner this shares with every other docker integration test, so the
fixture gives back whatever it had to pull.
"""

import logging
import os
import struct
import subprocess
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
import yaml

from sdcm.logcollector import GrafanaScreenShot, MonitoringStack
from sdcm.remote import LocalCmdRunner
from sdcm.utils.common import get_free_port
from sdcm.wait import wait_for

LOGGER = logging.getLogger(__name__)

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group("scylla-monitoring-stack")]

CONFIG_FILE = "unit_tests/test_data/grafana_screenshot_panels/detailed-panels.yaml"
#: The dashboard the config file names, and its panels keyed by the slug they land in the
#: filename as -- ``TOMBSTONE_PANEL`` is the one inside a collapsed row.
DASHBOARD_TITLE = "Detailed"
LSA_PANEL = "LSA total memory"
TOMBSTONE_PANEL = "Tombstones found in SSTables"
#: Each panel's `resolution` from that file: the size its screenshot must come back at.
PANEL_RESOLUTIONS = {"lsa_total_memory": (1200, 500), "tombstones_found_in_sstables": (1600, 600)}

_REPO_ROOT = Path(__file__).parent.parent.parent


def _monitor_branch() -> str:
    """The scylla-monitoring branch SCT installs on a monitor node."""
    defaults = yaml.safe_load((_REPO_ROOT / "defaults" / "test_default.yaml").read_text(encoding="utf-8"))
    return os.environ.get("SCT_MONITOR_BRANCH") or defaults["monitor_branch"]


def _png_size(path: Path) -> tuple[int, int]:
    """(width, height) from a PNG's IHDR, so image sizes can be asserted without Pillow."""
    header = path.read_bytes()[:24]
    assert header[:8] == b"\x89PNG\r\n\x1a\n", f"{path} is not a PNG"
    return struct.unpack(">II", header[16:24])


@pytest.fixture(name="monitoring_sources", scope="module")
def fixture_monitoring_sources(tmp_path_factory) -> Path:
    """The scylla-monitoring sources, laid out the way SCT lays them out on a monitor node."""
    branch = _monitor_branch()
    install_base = tmp_path_factory.mktemp("sct-monitoring")
    archive = install_base / f"{branch}.zip"

    response = requests.get(f"https://github.com/scylladb/scylla-monitoring/archive/{branch}.zip", timeout=300)
    response.raise_for_status()
    archive.write_bytes(response.content)
    with zipfile.ZipFile(archive) as zipped:
        for entry in zipped.infolist():
            extracted = Path(zipped.extract(entry, install_base / "tmp"))
            # zipfile drops the mode, and start-all.sh calls the other scripts as executables
            if mode := entry.external_attr >> 16:
                extracted.chmod(mode)
    # same rename download_scylla_monitoring() does, so get_monitoring_version() finds the dir
    (install_base / "tmp" / f"scylla-monitoring-{branch}").rename(install_base / "scylla-monitoring-src")

    install_path = install_base / "scylla-monitoring-src"
    (install_path / "monitor_version").write_text(f"{branch}:master", encoding="utf-8")
    return install_path


@pytest.fixture(name="grafana_port", scope="module")
def fixture_grafana_port(monitoring_sources) -> int:
    """A running scylla-monitoring stack with the renderer enabled; yields the Grafana port."""
    grafana_port, prometheus_port = get_free_port(), get_free_port()
    data_dir = monitoring_sources.parent / "scylla-monitoring-data"
    data_dir.mkdir(exist_ok=True)
    targets = monitoring_sources / "config" / "scylla_servers.yml"
    targets.parent.mkdir(exist_ok=True)
    targets.write_text("- targets: []", encoding="utf-8")
    # UA.sh phones home on every start; the monitor node setup blanks it too
    (monitoring_sources / "UA.sh").write_text("", encoding="utf-8")

    def _run(script: str, timeout: int) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", script, "-g", str(grafana_port), "-p", str(prometheus_port)] + extra_args(script),
            cwd=monitoring_sources,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    def extra_args(script: str) -> list:
        if script == "./kill-all.sh":
            return []
        return [
            # alertmanager and loki are not part of what a screenshot needs; the renderer is
            "--no-loki",
            "--no-alertmanager",
            "-s",
            str(targets),
            "-d",
            str(data_dir),
            "-v",
            "master",
            "-c",
            "GF_USERS_DEFAULT_THEME=dark",
        ]

    containers = [f"agraf-{grafana_port}", f"aprom-{prometheus_port}", "agrafrender"]

    def teardown():
        _run("./kill-all.sh", timeout=300)
        # kill-all.sh is best-effort: it leaves a container behind if it never got far enough
        # to start it, and a leaked one blocks the next run on its fixed name
        _docker("rm", "-f", *containers)

    teardown()  # a stack leaked by an aborted run would block startup
    images_before = _local_images()
    started = _run("./start-all.sh", timeout=1800)
    try:
        assert started.returncode == 0, f"start-all.sh failed:\n{started.stdout}\n{started.stderr}"
        _wait_for_grafana(grafana_port)
        yield grafana_port
    finally:
        LOGGER.debug("tearing down the monitoring stack on grafana port %s", grafana_port)
        pulled = _images_of(containers) - images_before
        teardown()
        # the stack is ~4GB of images, and the SCT runner this shares with every other docker
        # integration test only has 50GB -- leaving them behind fills it and the rest of the
        # suite dies on ENOSPC. Only what this fixture pulled goes, so a machine that already
        # had them keeps its cache.
        if pulled:
            LOGGER.debug("removing images pulled for the monitoring stack: %s", sorted(pulled))
            _docker("rmi", *sorted(pulled))


def _docker(*args: str) -> str:
    """Run a docker command, ignoring failures -- every call site is best-effort cleanup."""
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=False).stdout


def _local_images() -> set:
    return {_image_ref(line) for line in _docker("images", "--format", "{{.Repository}}:{{.Tag}}").split()}


def _images_of(containers: list) -> set:
    return {_image_ref(line) for line in _docker("inspect", "--format", "{{.Config.Image}}", *containers).split()}


def _image_ref(name: str) -> str:
    """`docker images` prints `grafana/grafana:13.2.0`; a container that the monitoring scripts
    started reports `docker.io/grafana/grafana:13.2.0`. Compare them on the same spelling."""
    return name.removeprefix("docker.io/")


def _wait_for_grafana(port: int) -> None:
    def healthy():
        try:
            return requests.get(f"http://localhost:{port}/api/health", timeout=5).ok
        except requests.RequestException:
            return False

    wait_for(healthy, step=2, timeout=300, throw_exc=True, text="Waiting for the monitoring stack's Grafana")


@pytest.fixture(name="monitor_node")
def fixture_monitor_node(monitoring_sources, grafana_port):
    """A stand-in for a monitor node, pointing the collector at the local stack.

    Only what `GrafanaScreenShot` reads: the Grafana address, and a remoter plus an install
    path so `get_monitoring_version()` reads the real `monitor_version` file off disk.
    """
    return SimpleNamespace(
        name="monitor-node-1",
        grafana_address="127.0.0.1",
        logdir=None,
        remoter=LocalCmdRunner(),
        parent_cluster=SimpleNamespace(monitor_install_path_base=monitoring_sources.parent),
    )


@pytest.fixture(name="screenshot_entity")
def fixture_screenshot_entity(params, grafana_port) -> GrafanaScreenShot:
    entity = GrafanaScreenShot(name="grafana-screenshot", test_start_time=time.time() - 3600)
    entity.set_params(params)
    entity.grafana_port = grafana_port
    return entity


@pytest.mark.sct_config(files=CONFIG_FILE)
@pytest.mark.parametrize("panel_slug", list(PANEL_RESOLUTIONS))
def test_configured_panel_is_rendered_at_its_resolution(screenshot_entity, monitor_node, tmp_path, panel_slug):
    """Each panel named in the test YAML is fetched from Grafana and saved at its configured size."""
    screenshots = [Path(path) for path in screenshot_entity.collect(monitor_node, local_dst=str(tmp_path))]

    panel_shots = [shot for shot in screenshots if panel_slug in shot.name]
    assert panel_shots, f"no screenshot was taken for {panel_slug}, got {[shot.name for shot in screenshots]}"
    assert _png_size(panel_shots[0]) == PANEL_RESOLUTIONS[panel_slug], (
        "the panel was not rendered at the resolution from the config -- the d-solo request lost it"
    )


@pytest.mark.sct_config(files=CONFIG_FILE)
def test_configured_panel_is_added_to_the_default_dashboards(screenshot_entity, monitor_node, tmp_path):
    """Configuring a panel adds to the dashboards SCT always captures, it does not replace them."""
    screenshots = [Path(path) for path in screenshot_entity.collect(monitor_node, local_dst=str(tmp_path))]

    assert [shot for shot in screenshots if "overview" in shot.name], (
        f"the default Overview dashboard stopped being captured, got {[s.name for s in screenshots]}"
    )
    # a whole dashboard is rendered at full height, so it must not come back at a panel's size
    tallest_panel = max(height for _, height in PANEL_RESOLUTIONS.values())
    for shot in screenshots:
        if "overview" in shot.name:
            assert _png_size(shot)[1] > tallest_panel, "the full dashboard was clipped to a panel height"


def test_unknown_panel_is_skipped_without_losing_the_other_screenshots(
    params, screenshot_entity, monitor_node, tmp_path
):
    """A panel title that matches nothing must be logged and skipped, not abort the collection."""
    params["grafana_screenshot_panels"] = [
        {"dashboard_title": DASHBOARD_TITLE, "panel_title": "no such panel exists"},
    ]

    screenshots = screenshot_entity.collect(monitor_node, local_dst=str(tmp_path))

    assert not [shot for shot in screenshots if "no_such_panel" in shot], "a missing panel produced a screenshot"
    assert screenshots, "one unresolvable panel wiped out the default dashboard screenshots"


@pytest.mark.parametrize("panel_title", [LSA_PANEL, TOMBSTONE_PANEL, "no such panel"])
def test_panel_is_found_by_title_in_a_real_dashboard(grafana_port, panel_title):
    """The lookup must find both a top-level panel and one in a collapsed row, and only those."""
    dashboard = MonitoringStack.get_dashboard_by_title(grafana_ip="127.0.0.1", port=grafana_port, title=DASHBOARD_TITLE)
    assert dashboard, f"the {DASHBOARD_TITLE!r} dashboard is not in the monitoring stack"

    panel_id = MonitoringStack.get_panel_by_title(
        grafana_ip="127.0.0.1", port=grafana_port, dashboard_uid=dashboard["uid"], panel_title=panel_title
    )

    if panel_title == "no such panel":
        assert panel_id is None, f"a title matching nothing resolved to panel {panel_id}"
    else:
        assert isinstance(panel_id, int), f"panel {panel_title!r} was not found in {DASHBOARD_TITLE!r}"
