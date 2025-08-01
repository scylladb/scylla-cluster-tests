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


class PytestStatus(str, Enum):
    ERROR = "error"
    PASSED = "passed"
    FAILURE = "failure"
    SKIPPED = "skipped"
    XFAILED = "xfailed"
    XPASS = "xpass"
    # Following statuses reflect test & teardown state
    # Example: passed & error means the test passed
    # but the tearDown stage errored
    PASSED_ERROR = "passed & error"
    FAILURE_ERROR = "failure & error"
    SKIPPED_ERROR = "skipped & error"
    ERROR_ERROR = "error & error"


class TestInvestigationStatus(str, Enum):
    NOT_INVESTIGATED = "not_investigated"
    IN_PROGRESS = "in_progress"
    INVESTIGATED = "investigated"
    IGNORED = "ignored"


class ResourceState(str, Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    TERMINATED = "terminated"
