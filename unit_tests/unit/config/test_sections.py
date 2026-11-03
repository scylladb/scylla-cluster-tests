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

"""Config sections: how a later layer merges into a section, and what a section accepts."""

import pydantic
import pytest
import yaml

from sdcm import sct_config
from sdcm.sct_config.helpers import merged_into_model
from sdcm.sct_config.sections import ConfigSection
from sdcm.sct_config.types import SctField


class RetrySection(ConfigSection):
    attempts: int | None = SctField(description="Attempts")
    backoff: int | None = SctField(description="Seconds between attempts")


class SampleSection(ConfigSection):
    port: int | None = SctField(description="Port")
    level: str | None = SctField(description="Log level")
    args: str | None = SctField(description="Extra arguments", appendable=True)
    hosts: list[str] | None = SctField(description="Hosts", appendable=True)
    retry: RetrySection = SctField(default_factory=RetrySection, description="Retries")


def merge(current: SampleSection, update: dict) -> SampleSection:
    return SampleSection(**merged_into_model(current, update))


def test_later_layer_merges_section_field_by_field():
    merged = merge(SampleSection(port=1, level="info"), {"port": 2})

    assert (merged.port, merged.level) == (2, "info")


def test_nested_section_merges_field_by_field():
    merged = merge(SampleSection(retry={"attempts": 3, "backoff": 5}), {"retry": {"backoff": 1}})

    assert (merged.retry.attempts, merged.retry.backoff) == (3, 1)


def test_list_is_replaced():
    merged = merge(SampleSection(hosts=["a", "b"]), {"hosts": ["c"]})

    assert merged.hosts == ["c"]


@pytest.mark.parametrize(
    ("current", "update", "expected"),
    [
        pytest.param({"args": "--a"}, {"args": "++ --b"}, ("--a --b", None), id="string"),
        pytest.param({"hosts": ["a"]}, {"hosts": ["++", "b"]}, (None, ["a", "b"]), id="list"),
    ],
)
def test_append_on_appendable_field(current, update, expected):
    merged = merge(SampleSection(**current), update)

    assert (merged.args, merged.hosts) == expected


def test_append_on_field_that_is_not_appendable_fails():
    with pytest.raises(AssertionError, match="Option port is not appendable"):
        merge(SampleSection(port=1), {"port": "++2"})


def test_yaml_string_for_nested_section_is_parsed_then_merged():
    merged = merge(SampleSection(retry={"attempts": 3}), {"retry": "{backoff: 2}"})

    assert (merged.retry.attempts, merged.retry.backoff) == (3, 2)


def test_unknown_key_in_section_fails():
    with pytest.raises(pydantic.ValidationError, match="prot"):
        merge(SampleSection(), {"prot": 1})


def test_assignment_to_section_field_is_validated():
    section = SampleSection()

    with pytest.raises(pydantic.ValidationError):
        section.port = "not a port"


def test_env_var_merges_into_model_typed_option_from_config_file(monkeypatch, tmp_path):
    layer = tmp_path / "layer.yaml"
    layer.write_text(yaml.safe_dump({"mgmt_agent_backup_config": {"checkers": 5}}))
    monkeypatch.setenv("SCT_CONFIG_FILES", str(["unit_tests/test_configs/minimal_test_case.yaml", str(layer)]))
    monkeypatch.setenv("SCT_MGMT_AGENT_BACKUP_CONFIG__TRANSFERS", "7")

    conf = sct_config.SCTConfiguration()

    assert (conf.mgmt_agent_backup_config.checkers, conf.mgmt_agent_backup_config.transfers) == (5, 7)
