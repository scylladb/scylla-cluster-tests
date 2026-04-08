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
