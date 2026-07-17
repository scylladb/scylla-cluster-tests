from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.provision.provisioner import ProvisionUnrecoverableError
from sdcm.sct_events import Severity
from sdcm.utils.decorators import _find_hdr_tags, critical_on_capacity_issues, skip_on_capacity_issues


HDR_TAGS1 = ["foohdr1", "foohdr2"]
HDR_TAGS2 = ["barhdr1", "barhdr2"]


def test__find_hdr_tags_in_dict():
    assert _find_hdr_tags({"foo": HDR_TAGS1, "hdr_tags": HDR_TAGS2}) == HDR_TAGS2


def test__find_hdr_tags_in_object_attr():
    obj_with_hdr_tags = type("FakeStressQueue", (), {"fake_hdr_tags": HDR_TAGS1, "hdr_tags": HDR_TAGS2})
    res = _find_hdr_tags(obj_with_hdr_tags)
    assert res == HDR_TAGS2


def test__find_hdr_tags_in_tuple():
    obj_with_hdr_tags = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    params = ("foo", 5, obj_with_hdr_tags)
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_in_tuple_of_lists():
    obj_with_hdr_tags1 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    obj_with_hdr_tags2 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS2})
    params = ("foo", 5, (["a", "b"], [obj_with_hdr_tags1, obj_with_hdr_tags2]))
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_error():
    try:
        _find_hdr_tags({"no_hdr_tags": ["foo"]})
    except ValueError:
        pass
    else:
        assert False, "Expected 'ValueError'"


def _raise_stuck_vm_give_up():
    raise ProvisionUnrecoverableError("Azure VM(s) node-x stuck in provisioning, giving up after 3 recovery attempts")


def test_skip_on_capacity_issues_converts_stuck_vm_give_up_to_nemesis_skip():
    """Stuck VM recovery give-up on a balanced cluster must skip the nemesis, not fail it."""
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=True):
        with pytest.raises(UnsupportedNemesis, match="Capacity Issue"):
            skip_on_capacity_issues(db_cluster=MagicMock())(_raise_stuck_vm_give_up)()


def test_critical_on_capacity_issues_publishes_critical_on_stuck_vm_give_up():
    """Stuck VM recovery give-up in a must-succeed topology change must raise a critical event."""
    with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
        with pytest.raises(ProvisionUnrecoverableError):
            critical_on_capacity_issues(_raise_stuck_vm_give_up)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once()
