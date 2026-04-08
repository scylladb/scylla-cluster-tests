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
# Copyright (c) 2024 ScyllaDB

from unittest.mock import patch

import pytest

from sdcm.gemini_thread import GeminiStressThread
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy


class MockCluster:
    """Minimal cluster stub providing the CQL IPs that GeminiStressThread needs."""

    def __init__(self, ips=None):
        self.nodes = []
        self._ips = ips or ["10.0.0.1"]

    def get_node_cql_ips(self):
        return self._ips


STRESS_CMD = "--duration=1m --mode=write --concurrency=5 --warmup=0"


@pytest.fixture(name="gemini_thread_unit")
def fixture_gemini_thread_unit(params):
    """Create a GeminiStressThread without touching real Docker (no integration mark)."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=None,
            timeout=60,
            params=params,
        )
    return thread


def test_docker_image_name_is_not_none(gemini_thread_unit):
    """GeminiStressThread.docker_image_name must never be None.

    Regression test: a params.copy() path in build_gemini_thread was dropping
    the stress_image dict, causing RemoteDocker to receive None as image_name
    which produced ``docker run ... None tail -f /dev/null``.
    """
    assert gemini_thread_unit.docker_image_name is not None, (
        "docker_image_name is None — SCTConfiguration.stress_image.gemini was not resolved. "
        "Check that load_docker_images_defaults() runs and that params is an SCTConfiguration "
        "(not a plain dict) when passed to GeminiStressThread."
    )
    assert "gemini" in gemini_thread_unit.docker_image_name.lower(), (
        f"Expected 'gemini' in docker_image_name, got: {gemini_thread_unit.docker_image_name!r}"
    )


@pytest.mark.parametrize(
    "oracle_ips,expect_oracle_flag",
    [
        pytest.param(["10.0.0.2"], True, id="with_oracle"),
        pytest.param(None, False, id="without_oracle"),
    ],
)
def test_generate_gemini_command_oracle_flag(params, oracle_ips, expect_oracle_flag):
    """--oracle-cluster flag must be present iff an oracle cluster is provided."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])
    oracle_cluster = MockCluster(oracle_ips) if oracle_ips else None

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=oracle_cluster,
            timeout=60,
            params=params,
        )

    cmd = thread.generate_gemini_command()
    if expect_oracle_flag:
        assert "--oracle-cluster=" in cmd, f"Expected --oracle-cluster in command: {cmd}"
    else:
        assert "--oracle-cluster" not in cmd, f"--oracle-cluster must not appear without oracle: {cmd}"


def test_generate_gemini_command_required_flags(gemini_thread_unit):
    """generate_gemini_command must include the essential gemini flags."""
    cmd = gemini_thread_unit.generate_gemini_command()

    assert "--test-cluster=" in cmd, f"Missing --test-cluster in: {cmd}"
    assert "--outfile=/" in cmd, f"Missing --outfile in: {cmd}"
    assert "--summary-file=/" in cmd, f"Missing --summary-file in: {cmd}"
    assert "--seed=" in cmd, f"Missing --seed in: {cmd}"


def test_generate_gemini_command_stress_cmd_flags_forwarded(gemini_thread_unit):
    """Flags from stress_cmd must appear verbatim in the generated command."""
    cmd = gemini_thread_unit.generate_gemini_command()

    assert "--duration=1m" in cmd, f"--duration not forwarded: {cmd}"
    assert "--mode=write" in cmd, f"--mode not forwarded: {cmd}"
    assert "--concurrency=5" in cmd, f"--concurrency not forwarded: {cmd}"


def test_generate_gemini_command_schema_path_injected(params):
    """When schema_path is supplied, --schema must appear in the command."""
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = MockCluster(["10.0.0.1"])

    with patch("sdcm.utils.docker_remote.RemoteDocker.pull_image"):
        thread = GeminiStressThread(
            loaders=loader_set,
            stress_cmd=STRESS_CMD,
            test_cluster=test_cluster,
            oracle_cluster=None,
            timeout=60,
            params=params,
        )

    cmd = thread.generate_gemini_command(schema_path="/tmp/my_schema.json")
    assert '--schema="/tmp/my_schema.json"' in cmd, f"--schema flag not injected: {cmd}"


def test_generate_gemini_command_no_schema_by_default(gemini_thread_unit):
    """Without schema_path, --schema must not appear in the generated command."""
    cmd = gemini_thread_unit.generate_gemini_command()
    assert "--schema=" not in cmd, f"--schema should not appear by default: {cmd}"
