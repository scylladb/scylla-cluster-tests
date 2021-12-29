# pylint: disable=no-self-use

import uuid
from pathlib import Path
from typing import Generator

import pytest

from sdcm.sct_events.continuous_event import ContinuousEventsRegistry, ContinuousEventRegistryException
from sdcm.sct_events.database import get_pattern_to_event_to_func_mapping, CompactionEvent
from sdcm.sct_events.loaders import GeminiStressEvent
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events.system import InfoEvent


class TestContinuousEventsRegistry:
    @pytest.fixture(scope="function")
    def registry(self):
        yield ContinuousEventsRegistry()

    @pytest.fixture(scope="function")
    def nodetool_event(self) -> Generator[NodetoolEvent, None, None]:
        yield NodetoolEvent(nodetool_command="mock_cmd", publish_event=False)

    @pytest.fixture(scope="function")
    def gemini_stress_event(self) -> Generator[GeminiStressEvent, None, None]:
        yield GeminiStressEvent(node="mock_node", cmd="gemini mock cmd", publish_event=False)

    @pytest.fixture(scope="function")
    def info_event(self) -> Generator[InfoEvent, None, None]:
        yield InfoEvent(message="This is a mock InfoEvent")

    @pytest.fixture(scope="function")
    def populated_registry(self,
                           registry: ContinuousEventsRegistry) -> Generator[ContinuousEventsRegistry, None, None]:
        for _ in range(100):
            NodetoolEvent(nodetool_command="mock cmd", publish_event=False)

        yield registry

    def test_add_event(self,
                       registry: ContinuousEventsRegistry):
        # continuous events are added to the registry on event instantiation
        event = NodetoolEvent(nodetool_command="mock cmd").begin_event(publish=False)

        assert event in registry.continuous_events

    def test_add_multiple_events(self,
                                 registry: ContinuousEventsRegistry):
        pre_insertion_item_count = len(list(registry.continuous_events))
        number_of_insertions = 10
        for _ in range(number_of_insertions):
            GeminiStressEvent(node=uuid.uuid1(), cmd="gemini hello", publish_event=False).begin_event()

        assert pre_insertion_item_count + number_of_insertions == len(list(registry.continuous_events))

    def test_adding_a_non_continuous_event_raises_error(self,
                                                        registry: ContinuousEventsRegistry,
                                                        info_event):
        with pytest.raises(ContinuousEventRegistryException):
            registry.add_event(info_event)

    def _read_events_from_file(self, file_name: str):
        with Path(__file__).parent.joinpath(file_name).open(encoding="utf-8") as sct_log:
            for line in sct_log.readlines():
                db_event_pattern_func_map = get_pattern_to_event_to_func_mapping(node='node1')
                for item in db_event_pattern_func_map:
                    event_match = item.pattern.search(line)
                    if not event_match:
                        continue
                    try:
                        item.period_func(match=event_match)
                    except RuntimeError as rex:
                        # Ignore the fact that the event is not published. It still will be created
                        if 'You should create default EventsProcessRegistry first' not in str(rex):
                            raise

    def test_get_compact_events_by_continues_hash_from_log(self, populated_registry: ContinuousEventsRegistry):
        self._read_events_from_file("test_data/compaction_event_start.log")

        continues_hash = CompactionEvent.get_continuous_hash_from_dict({
            'node': 'node1',
            'shard': '2',
            'table': 'system.local',
            'compaction_process_id': 'edc49670-2a65-11ec-a8b8-b62621e7624c'
        })
        found_events = populated_registry.find_continuous_events_by_hash(continues_hash)

        self._read_events_from_file("test_data/compaction_event_end.log")

        assert not populated_registry.find_continuous_events_by_hash(continues_hash), \
            "Event was not removed from registry"
        assert found_events
        found_event = found_events[-1]
        assert found_event
        assert isinstance(found_event, CompactionEvent)
        assert found_event.node == 'node1'
        assert found_event.shard == 2
        assert found_event.table == 'system.local'
        assert found_event.compaction_process_id == 'edc49670-2a65-11ec-a8b8-b62621e7624c'
