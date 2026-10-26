"""Tests for sdcm.utils.topology_ops.FailedDecommissionOperationMonitoring."""

import contextlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import KillNemesis
from sdcm.utils.topology_ops import FailedDecommissionOperationMonitoring

HOST_ID = "33ca16aa-f63e-404d-be65-2605589a1691"
DECOMMISSIONING = SimpleNamespace(node_state="decommissioning", topology_request=None)
LEFT = SimpleNamespace(node_state="left", topology_request=None)
NORMAL = SimpleNamespace(node_state="normal", topology_request=None)
RUNNING = (DECOMMISSIONING, DECOMMISSIONING, LEFT)
NOT_RUNNING = (NORMAL,)


@pytest.fixture(autouse=True)
def fake_wait_for():
    """Run the waited-for predicate a bounded number of times instead of sleeping between polls."""

    def wait_for(func, **_):
        for _ in range(10):
            if func():
                return
        raise TimeoutError("decommission did not settle")

    with patch("sdcm.utils.topology_ops.wait_for", side_effect=wait_for) as mock:
        yield mock


@pytest.fixture
def make_monitor():
    """Build a monitor whose system.topology query returns the given rows, one per call (the last one repeats)."""

    def _make(*topology_rows):
        rows = list(topology_rows)
        session = MagicMock()
        session.execute.side_effect = lambda *_: MagicMock(
            one=MagicMock(return_value=rows.pop(0) if len(rows) > 1 else rows[0])
        )
        target_node = MagicMock(host_id=HOST_ID)
        target_node.parent_cluster.cql_connection_patient_exclusive.return_value.__enter__.return_value = session
        return FailedDecommissionOperationMonitoring(target_node=target_node, verification_node=MagicMock())

    return _make


@pytest.mark.parametrize(
    "row, expected",
    [
        pytest.param(DECOMMISSIONING, True, id="running"),
        pytest.param(SimpleNamespace(node_state="normal", topology_request="leave"), True, id="queued"),
        pytest.param(NORMAL, False, id="rolled-back-or-not-started"),
        pytest.param(LEFT, False, id="left"),
        pytest.param(None, False, id="no-row"),
    ],
)
def test_is_node_decommissioning_raft_topology_row_returns_state(make_monitor, row, expected):
    assert make_monitor(row).is_node_decommissioning() is expected


@pytest.mark.parametrize(
    "rows, error, waits, verifies, propagates",
    [
        # SCT-512: nodetool died with the reboot, the coordinator resumes and completes the decommission
        pytest.param(RUNNING, None, True, False, False, id="no-error-running"),
        pytest.param(NOT_RUNNING, None, False, False, False, id="no-error-not-running"),
        pytest.param(RUNNING, RuntimeError, True, True, False, id="error-running"),
        pytest.param(NOT_RUNNING, RuntimeError, False, True, False, id="error-not-running"),
        pytest.param(RUNNING, KillNemesis, True, True, False, id="kill-nemesis-running"),
        pytest.param(NOT_RUNNING, KillNemesis, False, False, True, id="kill-nemesis-not-running"),
    ],
)
def test_exit_decommission_state_and_body_error_decide_wait_verify_and_suppress(
    make_monitor, fake_wait_for, rows, error, waits, verifies, propagates
):
    monitor = make_monitor(*rows)
    with pytest.raises(error) if propagates else contextlib.nullcontext(), monitor:
        if error:
            raise error()
    assert fake_wait_for.called is waits
    assert monitor.db_cluster.verify_decommission.called is verifies
    if verifies:
        monitor.db_cluster.verify_decommission.assert_called_once_with(monitor.target_node)
