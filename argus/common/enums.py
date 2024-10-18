from enum import Enum


class NemesisStatus(str, Enum):
    STARTED = "started"
    RUNNING = "running"
    FAILED = "failed"
    SKIPPED = "skipped"
    SUCCEEDED = "succeeded"
    TERMINATED = "terminated"


class TestStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    FAILED = "failed"
    TEST_ERROR = "test_error"
    ERROR = "error"
    PASSED = "passed"
    ABORTED = "aborted"
    NOT_PLANNED = "not_planned"
    NOT_RUN = "not_run"


class TestInvestigationStatus(str, Enum):
    NOT_INVESTIGATED = "not_investigated"
    IN_PROGRESS = "in_progress"
    INVESTIGATED = "investigated"
    IGNORED = "ignored"


class ResourceState(str, Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    TERMINATED = "terminated"
