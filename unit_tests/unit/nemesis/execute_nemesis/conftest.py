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
    """Provide a CustomTestNemesis that disrupts successfully."""
    return CustomTestNemesis(runner=nemesis_runner)


@pytest.fixture
def skipping_nemesis(nemesis_runner):
    """Provide a SkippingTestNemesis that raises UnsupportedNemesis."""
    return SkippingTestNemesis(runner=nemesis_runner)


@pytest.fixture
def failing_nemesis(nemesis_runner):
    """Provide a FailingTestNemesis that raises RuntimeError."""
    return FailingTestNemesis(runner=nemesis_runner)


@pytest.fixture
def kill_nemesis(nemesis_runner):
    """Provide a KillTestNemesis that raises KillNemesis."""
    return KillTestNemesis(runner=nemesis_runner)
