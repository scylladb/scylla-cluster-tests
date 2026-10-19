import uuid

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry, ContinuousEventRegistryException
from sdcm.sct_events.database import (
    get_pattern_to_event_to_func_mapping,
    CompactionEvent,
    IndexSpecialColumnErrorEvent,
    ScyllaServerStatusEvent,
    DatabaseLogEvent,
)
from sdcm.sct_events.loaders import GeminiStressEvent
from sdcm.sct_events.nemesis import DisruptionEvent
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events.system import InfoEvent


@pytest.fixture
def continuous_registry() -> ContinuousEventsRegistry:
    """A ContinuousEventsRegistry that collects the continuous events begun by the test."""
    return ContinuousEventsRegistry()


@pytest.fixture
def info_event() -> InfoEvent:
    """A non-continuous InfoEvent, for checking the registry accepts only continuous events."""
    return InfoEvent(message="This is a mock InfoEvent")


def test_add_event(continuous_registry: ContinuousEventsRegistry):
    """A continuous event joins the registry when it begins, so its end can be matched later."""
    event = NodetoolEvent(nodetool_command="mock cmd").begin_event(publish=False)

    assert event in continuous_registry.continuous_events


def test_add_multiple_events(continuous_registry: ContinuousEventsRegistry):
    """Every begun continuous event is kept in the registry as its own entry."""
    pre_insertion_item_count = len(list(continuous_registry.continuous_events))
    number_of_insertions = 10
    for _ in range(number_of_insertions):
        GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()
    assert pre_insertion_item_count + number_of_insertions == len(list(continuous_registry.continuous_events))


def test_add_nemesis_sub_context_to_continuous_event():
    """A continuous event names the nemeses running at its begin and at its end, so failures map to disruptions."""
    event = ScyllaServerStatusEvent(node=str(uuid.uuid1()), publish_event=False).begin_event(publish=False)
    # event not during nemesis
    assert event.subcontext == []

    nemesis1 = DisruptionEvent(node=uuid.uuid1(), nemesis_name="test1", publish_event=False).begin_event()
    GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()
    DisruptionEvent(node=uuid.uuid1(), nemesis_name="test2", publish_event=False).begin_event()
    NodetoolEvent(node=uuid.uuid1(), nodetool_command="mock cmd", publish_event=False).begin_event()
    GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()

    event = ScyllaServerStatusEvent(node=str(uuid.uuid1()), severity=Severity.ERROR).begin_event(publish=False)
    # begin event - 2 running nemeses

    assert event.msgfmt == (
        "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id} "
        "during_nemesis=test1,test2 node={0.node}"
    )

    # first nemesis finished
    nemesis1.end_event(publish=False)
    # stress events finishes during second nemesis
    event.end_event(publish=False)

    # end event - 1 running nemesis
    assert event.msgfmt == (
        "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id} "
        "during_nemesis=test2 duration={0.duration_formatted} node={0.node}"
    )

    # Validate that the event subcontext is serialized
    assert event.to_json()


def test_add_nemesis_sub_context_to_information_event():
    """An information event names the nemeses running when it is created."""
    event = IndexSpecialColumnErrorEvent(message="This is a mock Event")
    # event not during nemesis
    assert event.subcontext == []

    number_of_insertions = 2
    for i in range(number_of_insertions):
        DisruptionEvent(node=uuid.uuid1(), nemesis_name=f"test{i}", publish_event=False).begin_event()
        GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()

    event = IndexSpecialColumnErrorEvent(message="This is a mock Event")

    assert event.msgfmt == (
        "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id} "
        "during_nemesis=test0,test1: message={0.message}"
    )

    # Validate that the event subcontext is serialized
    assert event.to_json()


def test_add_nemesis_sub_context_to_db_log_event():
    """A DB log event and its clones start with no subcontext and name the nemeses running when cloned."""
    event = DatabaseLogEvent.ABORTING_ON_SHARD()
    # event not during nemesis
    assert event.subcontext == []

    # cloned events should have empty subcontext as well
    cloned_event = event.clone()
    assert cloned_event.subcontext == []

    number_of_insertions = 2
    for i in range(number_of_insertions):
        DisruptionEvent(node=uuid.uuid1(), nemesis_name=f"test{i}", publish_event=False).begin_event()
        GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()

    event = DatabaseLogEvent.ABORTING_ON_SHARD().clone()
    event.add_info(node="n1", line="kernel: Linux version", line_number=0)
    assert event.msgfmt == (
        "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id} "
        "during_nemesis=test0,test1: type={0.type} regex={0.regex} "
        "line_number={0.line_number} node={0.node}\n{0.line}"
    )

    # Validate that the event subcontext is serialized
    assert event.to_json()


def test_adding_a_non_continuous_event_raises_error(continuous_registry: ContinuousEventsRegistry, info_event):
    """The registry rejects a non-continuous event, since it could never be ended and removed."""
    with pytest.raises(ContinuousEventRegistryException):
        continuous_registry.add_event(info_event)


@pytest.fixture
def populated_registry(
    continuous_registry: ContinuousEventsRegistry,
) -> ContinuousEventsRegistry:
    """A registry already holding 100 NodetoolEvents, so lookups must pick the right event among many."""
    for _ in range(100):
        NodetoolEvent(nodetool_command="mock cmd", publish_event=False).begin_event(publish=False)

    return continuous_registry


@pytest.fixture
def read_events_from_file(test_data_dir):
    """Feed every line of a test-data log through the DB log event patterns, as the DB log reader does."""

    def read_events(file_name: str):
        with (test_data_dir / file_name).open(encoding="utf-8") as sct_log:
            for line in sct_log.readlines():
                db_event_pattern_func_map = get_pattern_to_event_to_func_mapping(node="node1")
                for item in db_event_pattern_func_map:
                    event_match = item.pattern.search(line)
                    if not event_match:
                        continue
                    try:
                        item.period_func(match=event_match)
                    except RuntimeError as rex:
                        # Ignore the fact that the event is not published. It still will be created
                        if "You should create default EventsProcessRegistry first" not in str(rex):
                            raise

    return read_events


@pytest.mark.skip(reason="https://trello.com/c/Mu3lGc7C/4828-disable-compaction-and-repair-continuous-events")
def test_get_compact_events_by_continues_hash_from_log(
    populated_registry: ContinuousEventsRegistry, read_events_from_file
):
    """Compaction start/end log lines open and close a CompactionEvent findable by its continuous hash."""
    read_events_from_file("compaction_event_start.log")

    continues_hash = CompactionEvent.get_continuous_hash_from_dict(
        {
            "node": "node1",
            "shard": "2",
            "table": "system.local",
            "compaction_process_id": "edc49670-2a65-11ec-a8b8-b62621e7624c",
        }
    )
    found_events = populated_registry.find_continuous_events_by_hash(continues_hash)

    read_events_from_file("compaction_event_end.log")

    assert not populated_registry.find_continuous_events_by_hash(continues_hash), "Event was not removed from registry"
    assert found_events
    found_event = found_events[-1]
    assert found_event
    assert isinstance(found_event, CompactionEvent)
    assert found_event.node == "node1"
    assert found_event.shard == 2
    assert found_event.table == "system.local"
    assert found_event.compaction_process_id == "edc49670-2a65-11ec-a8b8-b62621e7624c"
