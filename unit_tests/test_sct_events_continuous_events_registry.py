# pylint: disable=no-self-use

import random
from typing import Generator
from pathlib import Path

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.base import EventPeriod
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry, ContinuousEventRegistryException
from sdcm.sct_events.database import FullScanEvent, get_pattern_to_event_to_func_mapping
from sdcm.sct_events.loaders import GeminiStressEvent
from sdcm.sct_events.nodetool import NodetoolEvent


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
    def full_scan_event(self) -> Generator[FullScanEvent, None, None]:
        full_scan_event = FullScanEvent
        yield full_scan_event.start(db_node_ip="124.5.2.1", ks_cf="mock_cf")

    @pytest.fixture(scope="function")
    def populated_registry(self,
                           registry: ContinuousEventsRegistry) -> Generator[ContinuousEventsRegistry, None, None]:
        for _ in range(100):
            NodetoolEvent(nodetool_command="mock cmd", publish_event=False)

        yield registry

    def test_add_event(self,
                       registry: ContinuousEventsRegistry):
        # continuous events are added to the registry on event instantiation
        event = NodetoolEvent(nodetool_command="mock cmd")

        assert event in registry.continuous_events

    def test_add_multiple_events(self,
                                 registry: ContinuousEventsRegistry):
        pre_insertion_item_count = len(registry.continuous_events)
        number_of_insertions = 10
        for _ in range(number_of_insertions):
            GeminiStressEvent(node="2323432", cmd="gemini hello", publish_event=False)

        assert pre_insertion_item_count + number_of_insertions == len(registry.continuous_events)

    def test_adding_a_non_continuous_event_raises_error(self,
                                                        registry: ContinuousEventsRegistry,
                                                        full_scan_event: FullScanEvent):
        with pytest.raises(ContinuousEventRegistryException):
            registry.add_event(full_scan_event)

    def test_get_event_by_id(self,
                             populated_registry: ContinuousEventsRegistry):
        some_event = random.choice(populated_registry.continuous_events)
        some_event_id = some_event.event_id
        found_event = populated_registry.get_event_by_id(some_event_id)

        assert found_event.event_id == some_event.event_id

    def test_get_events_by_type(self,
                                populated_registry: ContinuousEventsRegistry):
        gemini_events_in_registry_count = len([e for e in populated_registry.continuous_events
                                               if isinstance(e, GeminiStressEvent)])
        found_gemini_events = populated_registry.get_events_by_type(event_type=GeminiStressEvent)

        assert len(found_gemini_events) == gemini_events_in_registry_count
        for event in found_gemini_events:
            assert isinstance(event, GeminiStressEvent)

    def test_get_events_by_period_type(self,
                                       populated_registry: ContinuousEventsRegistry,
                                       nodetool_event):
        count_of_begun_events_pre = len(populated_registry.get_events_by_period(period_type=EventPeriod.BEGIN))
        nodetool_event.begin_event()
        found_events = populated_registry.get_events_by_period(period_type=EventPeriod.BEGIN)

        assert len(found_events) == count_of_begun_events_pre + 1

    def test_get_events_by_attr(self,
                                populated_registry: ContinuousEventsRegistry,
                                nodetool_event):
        nodetool_event.nodetool_command = 'test_get_events_by_attr'
        nodetool_event.event_id = 'dc4c854c-6bb5-4689-9af6-a9aae225611a'
        nodetool_event.begin_event()
        registry_filter = populated_registry.get_registry_filter()
        found_events = registry_filter.filter_by_attr(base="NodetoolEvent",
                                                      severity=Severity.NORMAL,
                                                      period_type=EventPeriod.BEGIN.value,
                                                      nodetool_command='test_get_events_by_attr')

        assert len(found_events.get_filtered()) == 1
        assert found_events.get_filtered()[0] == nodetool_event

    def test_get_compact_events_by_attr_from_log(self, populated_registry: ContinuousEventsRegistry):
        with Path(__file__).parent.joinpath("test_data/compaction_event.log").open(encoding="utf-8") as sct_log:
            for line in sct_log.readlines():
                db_event_pattern_func_map = get_pattern_to_event_to_func_mapping(node='node1')
                for item in db_event_pattern_func_map:
                    event_match = item.pattern.search(line)
                    if event_match:
                        try:
                            item.period_func(match=event_match)
                        except RuntimeError as rex:
                            # Ignore the fact that the event is not published. It still will be created
                            if 'You should create default EventsProcessRegistry first' in str(rex):
                                pass

        registry_filter = populated_registry.get_registry_filter()
        found_events = registry_filter.filter_by_attr(base="CompactionEvent",
                                                      severity=Severity.NORMAL,
                                                      period_type=EventPeriod.END.value,
                                                      table='system.local').get_filtered()

        assert len(found_events) == 2, f"Found events: {found_events}"
        assert sorted([event.compaction_process_id for event in found_events]) == \
            ['7c58a350-2a65-11ec-b5b3-d14f790022cc', 'edc49670-2a65-11ec-a8b8-b62621e7624c']
