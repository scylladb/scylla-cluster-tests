import threading
from unittest.mock import MagicMock

import pytest

from unit_tests.unit.nemesis.execute_nemesis import (
    CustomTestNemesis,
    FailingTestNemesis,
    KillTestNemesis,
    SkippingTestNemesis,
    TestNemesisRunner,
)
from unit_tests.unit.nemesis.fake_cluster import PARAMS, FakeTester


@pytest.fixture
def nemesis_runner(events):
    """Create a NemesisRunner for testing with mocked external dependencies."""
    termination_event = threading.Event()
    tester = FakeTester(params=PARAMS)
    tester.db_cluster.check_cluster_health = MagicMock()
    tester.db_cluster.test_config = MagicMock()
    runner = TestNemesisRunner(tester, termination_event, nemesis_selector="flag_a")
    return runner


@pytest.fixture
def nemesis(nemesis_runner):
    """CustomTestNemesis whose disrupt() returns normally."""
    return CustomTestNemesis(runner=nemesis_runner)


@pytest.fixture
def skipping_nemesis(nemesis_runner):
    """SkippingTestNemesis whose disrupt() raises UnsupportedNemesis."""
    return SkippingTestNemesis(runner=nemesis_runner)


@pytest.fixture
def failing_nemesis(nemesis_runner):
    """FailingTestNemesis whose disrupt() raises RuntimeError."""
    return FailingTestNemesis(runner=nemesis_runner)


@pytest.fixture
def kill_nemesis(nemesis_runner):
    """KillTestNemesis whose disrupt() raises KillNemesis."""
    return KillTestNemesis(runner=nemesis_runner)
