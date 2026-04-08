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
# Copyright (c) 2020 ScyllaDB

import logging
from pathlib import Path

# Single source-of-truth for directory roots used across unit-test fixtures.
# Only this module is allowed to reference __file__ for path construction;
# all other unit-test files must consume the `test_data_dir` / `data_dir`
# fixtures defined below.
UNIT_TESTS_DIR: Path = Path(__file__).parent

from sdcm.utils.mp_start import ensure_start_method

ensure_start_method()

import pytest

from sdcm import sct_config
from sdcm.prometheus import start_metrics_server
from sdcm.provision import provisioner_factory
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry
from sdcm.sct_provision import region_definition_builder
from sdcm.utils.subtest_utils import SUBTESTS_FAILURES

from unit_tests.lib.fake_events import make_fake_events
from unit_tests.lib.fake_provisioner import FakeProvisioner
from unit_tests.lib.fake_region_definition_builder import FakeDefinitionBuilder


pytest_plugins = ["pytester"]


@pytest.fixture(scope="module")
def events():
    with make_fake_events() as device:
        yield device


@pytest.fixture(scope="function")
def events_function_scope():
    with make_fake_events() as device:
        yield device


@pytest.fixture(scope="session")
def prom_address():
    yield start_metrics_server()


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return the path to unit_tests/test_data/."""
    return UNIT_TESTS_DIR / "test_data"


@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Return the path to the top-level data_dir/ directory."""
    return UNIT_TESTS_DIR.parent / "data_dir"


@pytest.fixture(scope="session", autouse=True)
def fake_provisioner():
    provisioner_factory.register_provisioner(backend="fake", provisioner_class=FakeProvisioner)


@pytest.fixture(scope="session", autouse=True)
def fake_region_definition_builder():
    region_definition_builder.register_builder(backend="fake", builder_class=FakeDefinitionBuilder)


@pytest.fixture(scope="function", name="params")
def fixture_params(request: pytest.FixtureRequest, monkeypatch):
    if sct_config_marker := request.node.get_closest_marker("sct_config"):
        config_files = sct_config_marker.kwargs.get("files")
        monkeypatch.setenv("SCT_CONFIG_FILES", config_files)

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    params = sct_config.SCTConfiguration()
    params.update(
        dict(
            authenticator="PasswordAuthenticator",
            authenticator_user="cassandra",
            authenticator_password="cassandra",
            authorizer="CassandraAuthorizer",
        )
    )
    yield params


@pytest.fixture(scope="function", autouse=True)
def fixture_cleanup_continuous_events_registry():
    ContinuousEventsRegistry().cleanup_registry()


def pytest_sessionfinish():
    # pytest's capsys is enabled by default, see
    # https://docs.pytest.org/en/7.1.x/how-to/capture-stdout-stderr.html.
    # but pytest closes its internal stream for capturing the stdout and
    # stderr, so we are not able to write the logger anymore once the test
    # session finishes. see https://github.com/pytest-dev/pytest/issues/5577,
    # to silence the warnings, let's just prevent logging from raising
    # exceptions.
    logging.raiseExceptions = False


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report: pytest.TestReport):
    """
    Hook to log subtest failures and their reports,
    so it can be accessed later during teardown or in fixtures.

    in this one we handle the subtests failures only, and change some of them to passed
    """
    if report.when == "call" and getattr(report, "context", None):
        if report.failed:
            SUBTESTS_FAILURES[report.nodeid].append(report)
