import logging

from sdcm.sct_events import Severity
from sdcm.sct_events.teardown_validators import ValidatorEvent
from sdcm.teardown_validators.base import TeardownValidator

LOGGER = logging.getLogger(__name__)


class DiagnosisReportValidator(TeardownValidator):
    """
    This validator verifies that diagnosis dump functionality works as expected.
    """
    validator_name = 'diagnosis_report'

    def validate(self):
        LOGGER.info("Starting diagnosis report validation")
        test_node = self.tester.db_cluster.nodes[0]
        try:
            test_node.generate_coredump_file(restart_scylla=True, generate_diagnosis=True)
            LOGGER.info("Diagnosis dump report validation passed")
            self.tester.get_test_status = lambda: 'SUCCESS'
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Diagnosis validation failed: %s", exc)
            ValidatorEvent(
                message=f'Diagnosis report validation failed: {exc}', severity=Severity.ERROR
            ).publish()
            self.tester.get_test_status = lambda: 'FAILED'
