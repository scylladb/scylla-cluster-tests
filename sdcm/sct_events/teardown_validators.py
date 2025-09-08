from sdcm.sct_events import Severity
from sdcm.sct_events.base import InformationalEvent


class ValidatorEvent(InformationalEvent):
    def __init__(self, message: str, severity: Severity = Severity.NORMAL):
        super().__init__(severity)
        self.message = message

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"


class ScrubValidationErrorEvent(InformationalEvent):
    def __init__(self, node_name: str, sstables_link: str, severity: Severity = Severity.ERROR):
        super().__init__(severity=severity)

        self.message = f"Nodetool scrub in validation mode found invalid sstables on node {node_name}.See more details in db logs and quarantined sstables link: {sstables_link}"

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"
