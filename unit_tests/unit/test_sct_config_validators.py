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

"""Contract tests for the input converters in `sdcm.sct_config.types`."""

import pytest

from sdcm.sct_config.types import strtobool


@pytest.mark.parametrize("value", ["y", "Yes", "T", "true", "ON", "1", " true "])
def test_strtobool_accepts_truthy_spellings(value):
    assert strtobool(value) is True


@pytest.mark.parametrize("value", ["n", "No", "F", "false", "OFF", "0", " false "])
def test_strtobool_accepts_falsy_spellings(value):
    assert strtobool(value) is False


@pytest.mark.parametrize("value", ["maybe", "", "2", "truthy"])
def test_strtobool_rejects_anything_else(value):
    """Same contract as the distutils version it replaces -- ValueError, not a silent False."""
    with pytest.raises(ValueError, match="invalid truth value"):
        strtobool(value)
