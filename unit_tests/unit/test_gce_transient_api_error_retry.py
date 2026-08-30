"""SCT-417: a transient GCE API error while polling a long-running operation must not fail the test.

GCE returned `503 Authentication backend unavailable` on the `GET .../operations/<id>` poll that
`set_keep_alive()` issues right after a node was created, which aborted the whole rolling-upgrade run.
Covers the retry added to `wait_for_extended_operation()` and to the `gce_set_labels()` call around it.
"""

from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

import pytest
from google.api_core import exceptions, extended_operation
from google.api_core.future import polling
from google.cloud import compute_v1

from sdcm.utils import gce_utils
from sdcm.utils.gce_utils import (
    GCE_OPERATION_RETRY_TIMEOUT,
    GCE_SET_LABELS_RETRIES,
    gce_operation_poll_retry,
    gce_set_labels,
    wait_for_extended_operation,
)

TRANSIENT_ERRORS = [
    exceptions.TooManyRequests("429 Rate limit exceeded."),
    exceptions.InternalServerError("500 Internal error."),
    exceptions.BadGateway("502 Bad gateway."),
    exceptions.ServiceUnavailable("503 Authentication backend unavailable."),
    exceptions.GatewayTimeout("504 Gateway timeout."),
]


@dataclass
class FakeOperation:
    """The minimal shape `ExtendedOperation` expects from the wrapped operation message."""

    name: str = "operation-1779953504333-652dbb857aefa"
    status: str = "RUNNING"
    error_code: int = 0
    error_message: str = ""
    done: bool = False
    warnings: list = field(default_factory=list)


@pytest.fixture(autouse=True)
def no_backoff_sleeps(monkeypatch):
    """Keep the retries instant - the backoff values themselves are asserted separately."""
    monkeypatch.setattr(gce_utils, "GCE_OPERATION_RETRY_INITIAL_BACKOFF", 0.0)
    monkeypatch.setattr(gce_utils, "GCE_OPERATION_RETRY_MAX_BACKOFF", 0.0)


def make_operation(refresh_results: list) -> tuple[extended_operation.ExtendedOperation, MagicMock]:
    """Build a real ExtendedOperation whose poll raises/returns `refresh_results` in order.

    Using the real future (rather than a mock) is what makes these tests meaningful: the retry is
    handed to `operation.result()` and applied by api_core deep inside the polling loop.
    """
    refresh = MagicMock(side_effect=refresh_results)

    def poll(retry=None):
        # mirrors the generated GCE client: the retry policy handed down by `result()` wraps the
        # polling RPC itself, it is not applied by ExtendedOperation
        return retry(refresh)() if retry else refresh()

    # the real polling policy (so "operation not complete yet" keeps its own semantics), only without waits
    instant_polling = polling.DEFAULT_POLLING.with_delay(initial=0.0, maximum=0.0, multiplier=1.0).with_timeout(30.0)
    operation = extended_operation.ExtendedOperation.make(poll, lambda: None, FakeOperation(), polling=instant_polling)
    return operation, refresh


def done_operation(error_code: int = 0, error_message: str = "") -> FakeOperation:
    return FakeOperation(status="DONE", error_code=error_code, error_message=error_message, done=True)


@pytest.mark.parametrize("transient_error", TRANSIENT_ERRORS, ids=lambda err: type(err).__name__)
def test_transient_poll_error_is_retried_until_the_operation_completes(transient_error) -> None:
    operation, refresh = make_operation([transient_error, transient_error, done_operation()])

    assert wait_for_extended_operation(operation, "instance creation", timeout=30) is None
    assert refresh.call_count == 3


def test_non_transient_poll_error_fails_immediately() -> None:
    operation, refresh = make_operation([exceptions.Forbidden("403 Permission denied."), done_operation()])

    with pytest.raises(exceptions.Forbidden):
        wait_for_extended_operation(operation, "instance creation", timeout=30)
    assert refresh.call_count == 1


def test_exhausted_transient_retry_reports_the_underlying_gce_error() -> None:
    error = exceptions.ServiceUnavailable("503 Authentication backend unavailable.")
    operation, _ = make_operation([error] * 50)

    with pytest.raises(TimeoutError) as exc_info:
        # a 0s retry deadline lets the first poll run and then refuses to retry it
        with patch.object(gce_utils, "GCE_OPERATION_RETRY_TIMEOUT", 0.0):
            wait_for_extended_operation(operation, "setting labels on db-node-1", timeout=30)

    message = str(exc_info.value)
    assert "setting labels on db-node-1" in message
    assert "transient GCE API error" in message
    assert "Authentication backend unavailable" in message


def test_operation_failing_with_an_error_code_still_raises() -> None:
    operation, _ = make_operation([done_operation(error_code=400, error_message="Invalid machine type.")])

    with pytest.raises(exceptions.BadRequest, match="Invalid machine type."):
        wait_for_extended_operation(operation, "instance creation", timeout=30)


def test_successful_operation_is_not_retried() -> None:
    operation, refresh = make_operation([done_operation()])

    assert wait_for_extended_operation(operation, "instance creation", timeout=30) is None
    assert refresh.call_count == 1


@pytest.mark.parametrize(
    ("operation_timeout", "expected"),
    [
        (None, GCE_OPERATION_RETRY_TIMEOUT),
        (30, 30),
        (600, GCE_OPERATION_RETRY_TIMEOUT),
    ],
)
def test_retry_deadline_never_outlives_the_operation_timeout(operation_timeout, expected) -> None:
    assert gce_operation_poll_retry(operation_timeout).timeout == expected


@pytest.mark.parametrize("transient_error", TRANSIENT_ERRORS, ids=lambda err: type(err).__name__)
def test_retry_predicate_matches_only_transient_errors(transient_error) -> None:
    predicate = gce_operation_poll_retry()._predicate

    assert predicate(transient_error)
    assert not predicate(exceptions.Forbidden("403 Permission denied."))
    assert not predicate(exceptions.NotFound("404 Instance not found."))


def make_instance(fingerprint: str) -> compute_v1.Instance:
    return compute_v1.Instance(name="db-node-0-1", labels={"keep-action": "terminate"}, label_fingerprint=fingerprint)


@patch("sdcm.utils.gce_utils.wait_for_extended_operation", return_value=None)
def test_set_labels_retries_transient_error_with_a_fresh_fingerprint(mock_wait) -> None:
    """The SCT-417 scenario: the node exists, only its keep-alive labelling failed transiently."""
    instances_client = MagicMock()
    instances_client.set_labels.side_effect = [
        exceptions.ServiceUnavailable("503 Authentication backend unavailable."),
        MagicMock(),
    ]
    instances_client.get.return_value = make_instance("fingerprint-2")

    gce_set_labels(
        instances_client=instances_client,
        instance=make_instance("fingerprint-1"),
        new_labels={"keep": "12"},
        project="sct-project-1",
        zone="us-central1-c",
    )

    assert instances_client.set_labels.call_count == 2
    instances_client.get.assert_called_once_with(project="sct-project-1", zone="us-central1-c", instance="db-node-0-1")
    # the replay must not reuse the fingerprint the failed request was built with
    fingerprints = [
        call.kwargs["instances_set_labels_request_resource"].label_fingerprint
        for call in instances_client.set_labels.call_args_list
    ]
    assert fingerprints == ["fingerprint-1", "fingerprint-2"]
    # existing labels are preserved across the retry
    assert dict(instances_client.set_labels.call_args.kwargs["instances_set_labels_request_resource"].labels) == {
        "keep-action": "terminate",
        "keep": "12",
    }
    mock_wait.assert_called_once()


@patch("sdcm.utils.gce_utils.wait_for_extended_operation", return_value=None)
def test_set_labels_replays_a_stale_fingerprint_rejection(mock_wait) -> None:
    """A 503 whose request did reach GCE comes back as 412 on the replay - re-read and try again."""
    instances_client = MagicMock()
    instances_client.set_labels.side_effect = [
        exceptions.PreconditionFailed("412 Labels fingerprint does not match."),
        MagicMock(),
    ]
    instances_client.get.return_value = make_instance("fingerprint-2")

    gce_set_labels(
        instances_client=instances_client,
        instance=make_instance("fingerprint-1"),
        new_labels={"keep": "12"},
        project="sct-project-1",
        zone="us-central1-c",
    )

    assert instances_client.set_labels.call_count == 2
    mock_wait.assert_called_once()


@patch("sdcm.utils.gce_utils.wait_for_extended_operation", return_value=None)
def test_set_labels_gives_up_after_the_configured_attempts(mock_wait) -> None:
    instances_client = MagicMock()
    instances_client.set_labels.side_effect = exceptions.ServiceUnavailable("503 Authentication backend unavailable.")
    instances_client.get.return_value = make_instance("fingerprint-2")

    with pytest.raises(exceptions.ServiceUnavailable):
        gce_set_labels(
            instances_client=instances_client,
            instance=make_instance("fingerprint-1"),
            new_labels={"keep": "12"},
            project="sct-project-1",
            zone="us-central1-c",
        )

    assert instances_client.set_labels.call_count == GCE_SET_LABELS_RETRIES
    mock_wait.assert_not_called()


@patch("sdcm.utils.gce_utils.wait_for_extended_operation", return_value=None)
def test_set_labels_does_not_retry_a_non_transient_error(mock_wait) -> None:
    instances_client = MagicMock()
    instances_client.set_labels.side_effect = exceptions.Forbidden("403 Permission denied.")

    with pytest.raises(exceptions.Forbidden):
        gce_set_labels(
            instances_client=instances_client,
            instance=make_instance("fingerprint-1"),
            new_labels={"keep": "12"},
            project="sct-project-1",
            zone="us-central1-c",
        )

    assert instances_client.set_labels.call_count == 1
    instances_client.get.assert_not_called()
    mock_wait.assert_not_called()
