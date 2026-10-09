import time

import pytest

from sdcm.sct_events.events_processes import EventsProcessesRegistry, get_events_process
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
from unit_tests.lib.events_utils import EventsUtilsMixin


class MainEventsContext(EventsUtilsMixin):
    """Keep event process state isolated from other tests using EventsUtilsMixin.

    EventsUtilsMixin stores its runtime state on class attributes, so this dedicated
    subclass gives the fixture its own namespace instead of sharing mutable state
    with other helpers such as RealEventsTest.
    """


@pytest.fixture
def events_processes_registry(tmp_path):
    """Provide an events registry logging to a per-test directory."""
    return EventsProcessesRegistry(log_dir=str(tmp_path))


@pytest.fixture
def main_events_context():
    """Run a real main events device in an isolated events registry for one test."""
    MainEventsContext.setup_events_processes(
        events_device=False,
        events_main_device=True,
        registry_patcher=False,
    )
    try:
        yield MainEventsContext()
    finally:
        MainEventsContext.teardown_events_processes()


@pytest.fixture
def start_events_subscriber(main_events_context):
    """Factory starting subscribers on main_events_context's registry and returning the process registered as `process_id`.

    main_events_context stops every subscriber on teardown, before the main device it reads from.
    """

    def start(start_function, process_id):
        start_function(_registry=main_events_context.events_processes_registry)
        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)
        return get_events_process(process_id, _registry=main_events_context.events_processes_registry)

    return start
