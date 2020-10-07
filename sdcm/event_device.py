import atexit
import logging
import json

from sdcm.services.event_device import EventsDevice
from sdcm.services.event_file_logger import EventsFileLogger
from sdcm.services.grafana_annotator import GrafanaAnnotator
from sdcm.services.sct_events_analyzer import EventsAnalyzer
from sdcm.services.grafana_aggregator import GrafanaEventAggragator
from sdcm.services.base import get_from_all_sources, get_from_instance, ListOfServices
from sdcm.sct_events import DbEventsFilter, SctEvent


LOGGER = logging.getLogger(__name__)


class EventsProcessor:
    def __init__(self, log_dir, test_mode: bool = False, setup_info=None):
        self.log_dir = log_dir
        self.test_mode = test_mode
        self.event_device = EventsDevice(log_dir)
        self.event_file_logger = EventsFileLogger(
            log_dir,
            events_device=self.event_device,
            flush_logs=test_mode
        )
        self.grafana_event_aggregator = GrafanaEventAggragator()
        self.grafana_annotator = GrafanaAnnotator(
            events_device=self.event_device,
            aggregator=self.grafana_event_aggregator)
        self.event_analyzer = EventsAnalyzer(
            events_device=self.event_device,
            setup_info=setup_info)
        self._filters = []

    def start(self):
        self.event_device.start()
        SctEvent.set_main_device(main_device=self.event_device, file_logger=self.event_file_logger)
        self.event_file_logger.start()
        self.grafana_event_aggregator.start()
        self.grafana_annotator.start()
        self.event_analyzer.start()
        atexit.register(self.stop)

    def check_if_working(self):
        try:
            self.event_device.wait_till_event_loop_is_working(number_of_events=20)
        except RuntimeError:
            LOGGER.error("event loop failed to deliver 20 test events with no loss")
            raise

    def apply_default_filters(self):
        self._filters.append(DbEventsFilter(type='BACKTRACE', line='Rate-limit: supressed'))
        self._filters.append(DbEventsFilter(type='BACKTRACE', line='Rate-limit: suppressed'))

    @property
    def services(self) -> ListOfServices:
        return get_from_instance(self)

    def stop(self, timeout: int = None):
        self.services.gradually_stop(timeout).alive().kill()
        self.services.cleanup()


def start_events_device(log_dir, setup_info=None, test_mode=False) -> EventsProcessor:
    ep = EventsProcessor(log_dir=log_dir, setup_info=setup_info, test_mode=test_mode)
    ep.start()
    ep.check_if_working()
    ep.apply_default_filters()
    return ep


def stop_and_cleanup_all_services():
    services = get_from_all_sources()
    services.gradually_stop(60)
    services.alive().kill()
    services.cleanup()


def set_grafana_url(url):
    get_from_all_sources().get('GrafanaEventAggragator').set_grafana_url(url)

