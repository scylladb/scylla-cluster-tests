import pytest

from unit_tests.lib.events_utils import EventsUtilsMixin


class MainEventsContext(EventsUtilsMixin):
    pass


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
