import pytest

from sdcm.utils.decorators import _find_hdr_tags


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


def test__find_hdr_tags_falls_back_when_the_kwarg_is_none():
    """A caller with an optional 'hdr_tags' argument always forwards it, so an unset one arrives
    as an explicit None. That must not shadow the tags the stress threads set on themselves."""
    stress_queue = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    res = _find_hdr_tags({"stress_cmds": ["cassandra-stress write"], "hdr_tags": None}, (["result"], [stress_queue]))
    assert res == HDR_TAGS1


def test__find_hdr_tags_falls_back_when_an_object_attr_is_none():
    obj_with_none = type("FakeStressQueue", (), {"hdr_tags": None})
    obj_with_hdr_tags = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS2})
    assert _find_hdr_tags(obj_with_none, obj_with_hdr_tags) == HDR_TAGS2


def test__find_hdr_tags_keeps_returning_an_empty_list():
    """A nemesis carries an empty 'hdr_tags' until a test populates it, and a cycle reported
    without HDR histograms is a valid outcome - only None is unusable to the caller."""
    nemesis = type("FakeNemesis", (), {"hdr_tags": []})
    assert _find_hdr_tags(nemesis) == []


def test__find_hdr_tags_error_on_a_none_kwarg_with_nothing_to_fall_back_to():
    """Never return the None itself: the caller iterates over what it gets back."""
    with pytest.raises(ValueError, match="Failed to find 'hdr_tags'"):
        _find_hdr_tags({"hdr_tags": None}, ([], []))
