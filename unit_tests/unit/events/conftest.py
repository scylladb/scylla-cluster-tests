import pytest

from unit_tests.lib.events_utils import EventsUtilsMixin


class MainEventsContext(EventsUtilsMixin):
    """Keep event process state isolated from other tests using EventsUtilsMixin.

    EventsUtilsMixin stores its runtime state on class attributes, so this dedicated
    subclass gives the fixture its own namespace instead of sharing mutable state
    with other helpers such as RealEventsTest.
    """


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
