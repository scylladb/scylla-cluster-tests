import logging
import time
from typing import Dict

from dateutil import parser
from google.oauth2 import service_account
from googleapiclient.discovery import build

from sdcm.keystore import KeyStore
from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, EventPeriod


LOGGER = logging.getLogger(__name__)


class GceLoggingClient:  # pylint: disable=too-few-public-methods

    def __init__(self):
        raw_credentials = KeyStore().get_logging_gcp_credentials()
        self.credentials = service_account.Credentials.from_service_account_info(raw_credentials)
        self.project_id = raw_credentials['project_id']

    def get_error_logs(self, since: float):
        """Gets 'hostError' and 'automaticRestart' logs entries from GCE since the time provided in argument.
        Returns list of entries in a form of dictionaries. See example output in unit tests."""
        since = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(since))
        with build('logging', 'v2', credentials=self.credentials, cache_discovery=False) as service:
            return self._get_log_entries(service, since)

    def _get_log_entries(self, service, since, page_token=None):
        body = {
            "resourceNames": [
                f"projects/{self.project_id}"
            ],
            "filter": f'protoPayload.methodName="compute.instances.automaticRestart" '
                      f'OR protoPayload.methodName="compute.instances.hostError" timestamp > "{since}"'
        }
        if page_token:
            body.update({"page_token": page_token})
        ret = service.entries().list(body=body).execute()
        entries = ret.get('entries', [])
        if page_token := ret.get("nextPageToken"):
            entries.extend(self._get_log_entries(service, since, page_token))
        return entries


class GceInstanceEvent(SctEvent):

    def __init__(self,
                 gce_log_entry: Dict,
                 severity=Severity.ERROR):
        self.date = str(parser.parse(gce_log_entry["timestamp"]).astimezone())
        self.node = gce_log_entry["protoPayload"]["resourceName"].split("/")[-1]
        self.method = gce_log_entry["protoPayload"]["methodName"]
        self.message = gce_log_entry["protoPayload"]["status"]["message"]
        self.period_type = EventPeriod.INFORMATIONAL.value
        super().__init__(severity=severity)

    @property
    def msgfmt(self):
        return super().msgfmt + ": {0.method} on node {0.node} at {0.date}: {0.message}"


def log_gce_errors_as_events(test_id: str, test_start_time: float, logging_client: GceLoggingClient):
    """Create error events for specific GCE log entries like 'hostError' or 'automaticRestart' to show it in SCT reports."""
    test_id_short = str(test_id)[:8]
    LOGGER.debug("Getting GCE log events for test_id %s starting from timestamp %s...", test_id_short, test_start_time)
    try:
        for entry in logging_client.get_error_logs(since=test_start_time):
            if test_id_short in entry["protoPayload"]["resourceName"]:
                GceInstanceEvent(entry).publish()
        LOGGER.debug("GCE log events collected.")
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error("Error during getting GCE logs: %s", exc)
