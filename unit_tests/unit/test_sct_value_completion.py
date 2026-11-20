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

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.shell_completion import ShellComplete

import sct
from sdcm.utils.sct_cmd_helpers import _clouds_in_context


@pytest.fixture(autouse=True)
def completion_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))


def complete(*words, incomplete=""):
    return ShellComplete(sct.cli, {}, "hydra", "_HYDRA_COMPLETE").get_completions(list(words), incomplete)


def values(*words, incomplete=""):
    return [item.value for item in complete(*words, incomplete=incomplete)]


def test_test_ids_come_from_local_runs_newest_first(tmp_path, monkeypatch):
    for run_dir, test_id in [
        ("20260101-000000-000001", "aaaa-old"),
        ("20260301-000000-000001", "bbbb-new"),
        ("20260301-000000-000002-collect-logs", "bbbb-new"),  # same run, another command's logdir
        ("20260201-000000-000001", "abcd-mid"),
    ]:
        (tmp_path / "runs" / run_dir).mkdir(parents=True)
        (tmp_path / "runs" / run_dir / "test_id").write_text(f"{test_id}\n")
    monkeypatch.setenv("_SCT_LOGDIR", str(tmp_path / "runs"))

    assert values("clean-resources", "--test-id") == ["bbbb-new", "abcd-mid", "aaaa-old"]
    assert values("investigate", "show-logs", incomplete="a") == ["abcd-mid", "aaaa-old"]


def test_instance_types_follow_the_cloud_on_the_command_line():
    gce_types = values("create-runner-instance", "-c", "gce", "--instance-type", incomplete="n2-highmem-")

    assert "n2-highmem-8" in gce_types
    assert all(t.startswith("n2-highmem-") for t in gce_types)
    assert "i4i.large" not in values("create-runner-instance", "-c", "gce", "--instance-type")


def test_regions_are_fetched_once_then_served_from_disk():
    with patch("sdcm.utils.sct_cmd_helpers.get_all_regions", return_value=["us-east1", "europe-west1"]) as fetch:
        first = values("create-runner-image", "-c", "gce", "-r", incomplete="us")
        second = values("create-runner-image", "-c", "gce", "-r")

    assert first == ["us-east1"]
    assert second == ["us-east1", "europe-west1"]
    fetch.assert_called_once_with("gce")


def test_region_lookup_failure_offers_nothing_and_is_not_retried_on_every_tab():
    with patch("sdcm.utils.sct_cmd_helpers.get_all_regions", side_effect=RuntimeError("no credentials")) as fetch:
        assert values("create-runner-image", "-c", "azure", "-r") == []
        assert values("create-runner-image", "-c", "azure", "-r") == []

    fetch.assert_called_once_with("azure")


def test_each_backend_given_narrows_the_clouds():
    ctx = SimpleNamespace(params={"backend": ("aws", "k8s-gke")})

    assert _clouds_in_context(ctx) == ["aws", "gce"]


def test_config_files_complete_as_paths():
    assert [(i.type, i.value) for i in complete("conf", incomplete="test-cases/")] == [("file", "test-cases/")]
