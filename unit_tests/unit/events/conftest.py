from unittest.mock import patch

import pytest

from sdcm.test_config import TestConfig
from unit_tests.lib.events_utils import EventsUtilsMixin


class MainEventsContext(EventsUtilsMixin):
    """Keep event process state isolated from other tests using EventsUtilsMixin.

    EventsUtilsMixin stores its runtime state on class attributes, so this dedicated
    subclass gives the fixture its own namespace instead of sharing mutable state
    with other helpers such as RealEventsTest.
    """


@pytest.fixture(autouse=True)
def tester_obj():
    """Provide a tester object to code reading it off the TestConfig singleton.

    TestConfig keeps the tester object on a class attribute and set_tester_obj() assigns it only once,
    so a test that sets it can never unset it and leaks into every test that runs afterwards.
    Patching the accessor gives the same value without touching the singleton state.
    """
    with patch.object(TestConfig, "tester_obj", return_value="abc") as mock:
        yield mock


@pytest.fixture
def main_events_context():
    MainEventsContext.setup_events_processes(
        events_device=False,
        events_main_device=True,
        registry_patcher=False,
    )
    try:
        yield MainEventsContext()
    finally:
        MainEventsContext.teardown_events_processes()
