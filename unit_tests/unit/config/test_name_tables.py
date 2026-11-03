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

"""Option-name tables name root options, so an option moved into a section cannot silently drop out of them."""

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_config import defaults
from sdcm.utils.cloud_backends import SIZING_ROLE_PARAMS

TABLE_NAMES = {
    "REQUIRED_PARAMS": set(defaults.REQUIRED_PARAMS),
    "STRESS_CMD_PARAMS": set(defaults.STRESS_CMD_PARAMS),
    "AMI_ID_PARAMS": set(defaults.AMI_ID_PARAMS),
    "BACKEND_IMAGE_FIELD": set(defaults.BACKEND_IMAGE_FIELD.values()),
    "BACKEND_REQUIRED_PARAMS": {name for names in defaults.BACKEND_REQUIRED_PARAMS.values() for name in names},
    "PER_PROVIDER_MULTI_REGION_PARAMS": {
        name for names in defaults.PER_PROVIDER_MULTI_REGION_PARAMS.values() for name in names
    },
    "XCLOUD_PER_PROVIDER_REQUIRED_PARAMS": {
        name
        for providers in defaults.XCLOUD_PER_PROVIDER_REQUIRED_PARAMS.values()
        for names in providers.values()
        for name in names
    },
    "SIZING_ROLE_PARAMS": {name for roles in SIZING_ROLE_PARAMS.values() for name in roles.values()},
}


@pytest.mark.parametrize("table", sorted(TABLE_NAMES))
def test_every_table_name_is_a_root_option(table):
    unknown = sorted(TABLE_NAMES[table] - SCTConfiguration.model_fields.keys())

    assert not unknown, f"{table} names options that are not root fields: {unknown}"


def test_backend_required_params_naming_unknown_option_fails(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    conf = SCTConfiguration()

    with pytest.raises(ValueError, match=r"Required params of docker name unknown options: \['nemesis_interval'\]"):
        conf._check_backend_defaults("docker", ["user_prefix", "nemesis_interval"])
