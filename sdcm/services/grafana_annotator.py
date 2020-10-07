from sdcm.services.base import DetachThreadService
from sdcm.services.grafana_aggregator import GrafanaEventAggragator
from sdcm.services.event_device import EventsDevice


class GrafanaAnnotator(DetachThreadService):
    stop_priority = 60
    _interval = 0

    def __init__(self, events_device: EventsDevice, aggregator: GrafanaEventAggragator):
        self._events_device = events_device
        self._aggregator = aggregator
        super().__init__()

    def _service_body(self):
        for event_class, message_data in self._events_device.subscribe_events(stop_event=self._termination_event):
            event_type = getattr(message_data, 'type', None)
            tags = [event_class, message_data.severity.name, 'events']
            if event_type:
                tags += [event_type]
            annotate_data = {
                "time": int(message_data.timestamp * 1000.0),
                "tags": tags,
                "isRegion": False,
                "text": str(message_data)
            }
            self._aggregator.store_annotation(annotate_data)
