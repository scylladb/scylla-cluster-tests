# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import pytest

from sdcm.utils.nested_env_key import nested_env_subkey, split_earliest_nested_key


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        pytest.param("sizing_db.vcpu", ("sizing_db", "vcpu"), id="dot-notation"),
        pytest.param("sizing_db__vcpu", ("sizing_db", "vcpu"), id="double-underscore-notation"),
        # When both separators appear, the earliest one wins -- in either direction.
        pytest.param("sizing_db.vcpu__extra", ("sizing_db", "vcpu__extra"), id="dot-before-double-underscore"),
        pytest.param("sizing_db__vcpu.extra", ("sizing_db", "vcpu.extra"), id="double-underscore-before-dot"),
        pytest.param("sizing_db", None, id="no-separator"),
        # A lone "_" (as found in most field names, e.g. instance_type_db) must never be
        # mistaken for the "__" separator.
        pytest.param("instance_type_db", None, id="single-underscore-is-not-a-separator"),
    ],
)
def test_split_earliest_nested_key(key, expected):
    assert split_earliest_nested_key(key) == expected


@pytest.mark.parametrize(
    ("env_key", "prefix", "sep", "expected"),
    [
        pytest.param("SCT_STRESS_IMAGE.ycsb", "SCT_STRESS_IMAGE", ".", "ycsb", id="dot-notation"),
        pytest.param("SCT_STRESS_IMAGE__ycsb", "SCT_STRESS_IMAGE", "__", "ycsb", id="double-underscore-notation"),
        # Anchoring on `prefix + sep` keeps a field whose name is a prefix of another
        # field's name (sizing_db vs. sizing_db_oracle) from claiming the other's key.
        pytest.param("SCT_SIZING_DB_ORACLE__vcpu", "SCT_SIZING_DB", "__", None, id="prefix-is-anchored"),
        pytest.param(
            "SCT_INSTANCE_TYPE_DB", "SCT_INSTANCE_TYPE", "__", None, id="single-underscore-is-not-a-separator"
        ),
        # Only the first nesting level is captured; the trailing part is dropped.
        pytest.param("SCT_STRESS_IMAGE__foo__bar", "SCT_STRESS_IMAGE", "__", "foo", id="only-first-level"),
        pytest.param("SCT_STRESS_IMAGE.foo.bar", "SCT_STRESS_IMAGE", ".", "foo", id="only-first-level-dot"),
        pytest.param("SCT_OTHER_FIELD__vcpu", "SCT_STRESS_IMAGE", "__", None, id="unrelated-prefix"),
    ],
)
def test_nested_env_subkey(env_key, prefix, sep, expected):
    assert nested_env_subkey(env_key, prefix, sep) == expected
