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

"""The `nemesis` section loaded through `SCTConfiguration`: defaults, config files, overlays and env vars."""

import pytest
import yaml

from sdcm import sct_config

MINIMAL_YAML = "unit_tests/test_configs/minimal_test_case.yaml"
RUNNERS_YAML = "unit_tests/test_configs/nemesis_runners.yaml"


@pytest.fixture(name="config_files")
def config_files_fixture(monkeypatch, tmp_path):
    """Load the minimal test case, then the given files or YAML mappings, in that order."""

    def load(*layers):
        paths = [MINIMAL_YAML]
        for index, layer in enumerate(layers):
            if isinstance(layer, dict):
                path = tmp_path / f"layer_{index}.yaml"
                path.write_text(yaml.safe_dump(layer))
                paths.append(str(path))
            else:
                paths.append(layer)
        monkeypatch.setenv("SCT_CONFIG_FILES", str(paths))
        return sct_config.SCTConfiguration()

    return load


def test_nested_nemesis_loads_from_config_file(config_files):
    conf = config_files(RUNNERS_YAML)

    assert conf.nemesis.interval == 7
    assert [runner.class_name for runner in conf.nemesis.runners] == [
        "SisyphusMonkey",
        "SisyphusMonkey",
        "CategoricalMonkey",
        "ManagerRcloneBackup",
    ]


def test_partial_section_keeps_the_defaults(config_files):
    conf = config_files({"nemesis": {"interval": 30}})

    assert conf.nemesis.interval == 30
    assert conf.nemesis.during_prepare is True, "from defaults/test_default.yaml"
    assert conf.nemesis.grow_shrink.add_node_cnt == 3, "from defaults/test_default.yaml"
    assert [runner.class_name for runner in conf.nemesis.runners] == ["NoOpMonkey"]


def test_later_layer_merges_nested_section_field_by_field(config_files):
    conf = config_files(
        {"nemesis": {"seed": 42, "grow_shrink": {"add_node_cnt": 5, "instance_type": "i4i.large"}}},
        {"nemesis": {"interval": 30, "grow_shrink": {"add_node_cnt": 1}}},
    )

    assert (conf.nemesis.interval, conf.nemesis.seed) == (30, 42)
    assert (conf.nemesis.grow_shrink.add_node_cnt, conf.nemesis.grow_shrink.instance_type) == (1, "i4i.large")


def test_later_layer_replaces_runners(config_files):
    conf = config_files(RUNNERS_YAML, {"nemesis": {"runners": [{"NoOpMonkey": {}}]}})

    assert [runner.class_name for runner in conf.nemesis.runners] == ["NoOpMonkey"]
    assert conf.nemesis.interval == 7


def test_append_on_section_fields(config_files):
    conf = config_files({"nemesis": {"selector": "++not disruptive", "runners": ["++", {"SisyphusMonkey": {}}]}})

    assert conf.nemesis.selector == "not disruptive"
    assert [runner.class_name for runner in conf.nemesis.runners] == ["NoOpMonkey", "SisyphusMonkey"]


def test_append_on_section_field_from_env(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS__SELECTOR", "++ and not disruptive")

    conf = config_files({"nemesis": {"selector": "topology_changes"}})

    assert conf.nemesis.selector == "topology_changes and not disruptive"


def test_append_on_section_list_from_env(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS__RUNNERS", '["++", {"SisyphusMonkey": {}}]')

    conf = config_files()

    assert [runner.class_name for runner in conf.nemesis.runners] == ["NoOpMonkey", "SisyphusMonkey"]


@pytest.mark.parametrize("env_name", ["SCT_NEMESIS_INTERVAL", "SCT_SLA"])
def test_flat_nemesis_env_var_fails(config_files, monkeypatch, env_name):
    monkeypatch.setenv(env_name, "1")

    with pytest.raises(ValueError, match=env_name):
        config_files()._check_unexpected_sct_variables()


def test_append_on_section_field_that_is_not_appendable_fails(config_files):
    with pytest.raises(AssertionError, match="Option interval is not appendable"):
        config_files({"nemesis": {"interval": "++5"}})


def test_env_vars_override_nested_nemesis(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS__INTERVAL", "11")
    monkeypatch.setenv("SCT_NEMESIS__RUNNERS", '[{"NoOpMonkey": {"interval": 1}}]')

    conf = config_files(RUNNERS_YAML)

    assert (conf.nemesis.interval, conf.nemesis.seed) == (11, 42)
    assert [(runner.class_name, runner.interval) for runner in conf.nemesis.runners] == [("NoOpMonkey", 1)]


def test_env_vars_set_nested_grow_shrink_options(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS__DURING_PREPARE", "false")
    monkeypatch.setenv("SCT_NEMESIS__GROW_SHRINK", '{"add_node_cnt": 5, "instance_type": "i4i.xlarge"}')

    conf = config_files(RUNNERS_YAML)

    assert conf.nemesis.during_prepare is False
    assert (conf.nemesis.grow_shrink.add_node_cnt, conf.nemesis.grow_shrink.instance_type) == (5, "i4i.xlarge")
    assert conf.nemesis.grow_shrink.double_load_duration == 0, "from defaults/test_default.yaml"


def test_env_var_sets_whole_nested_nemesis_option_from_json(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS", '{"during_prepare": false, "interval": 11}')

    conf = config_files(RUNNERS_YAML)

    assert (conf.nemesis.during_prepare, conf.nemesis.interval) == (False, 11)
    assert len(conf.nemesis.runners) == 4


def test_model_typed_option_merges_field_by_field(config_files, monkeypatch):
    monkeypatch.setenv("SCT_MGMT_AGENT_BACKUP_CONFIG__TRANSFERS", "7")

    conf = config_files({"mgmt_agent_backup_config": {"checkers": 5}})

    assert (conf.mgmt_agent_backup_config.checkers, conf.mgmt_agent_backup_config.transfers) == (5, 7)


@pytest.mark.parametrize("flat_name", ["nemesis_interval", "sla"])
def test_flat_nemesis_option_in_yaml_fails(config_files, flat_name):
    with pytest.raises(ValueError, match=f"Unknown configuration key='{flat_name}'"):
        config_files({flat_name: 5})


def test_typo_inside_section_fails(config_files):
    with pytest.raises(ValueError, match="intervl"):
        config_files({"nemesis": {"intervl": 5}})


def test_categorical_monkey_dist_naming_unknown_nemesis_fails_config_load(config_files, monkeypatch):
    monkeypatch.setenv("SCT_NEMESIS__RUNNERS", '[{"CategoricalMonkey": {"dist": {"nodetool_decommission": 1}}}]')

    with pytest.raises(ValueError, match=r"`dist` names unknown nemesis classes: \['nodetool_decommission'\]"):
        config_files(RUNNERS_YAML)


@pytest.mark.parametrize(
    "nemesis,seeds_num,valid",
    [
        pytest.param({"runners": [{"SisyphusMonkey": {"filter_seeds": True}}]}, 3, False, id="runner_filters_seeds"),
        pytest.param({"filter_seeds": True, "runners": [{"SisyphusMonkey": {}}]}, 3, False, id="default_filters"),
        pytest.param(
            {"filter_seeds": True, "runners": [{"SisyphusMonkey": {"filter_seeds": False}}]},
            3,
            True,
            id="runner_overrides_default",
        ),
        pytest.param({"filter_seeds": True, "runners": [{"NoOpMonkey": {}}]}, 3, True, id="no_op_runner"),
        pytest.param({"runners": [{"SisyphusMonkey": {"filter_seeds": True}}]}, 1, True, id="non_seed_nodes_exist"),
    ],
)
def test_filter_seeds_needs_a_non_seed_node(config_files, monkeypatch, nemesis, seeds_num, valid):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    conf = config_files({"nemesis": nemesis, "seeds_num": seeds_num, "add_node_cnt": 0})

    if valid:
        conf._validate_nemesis_can_run_on_non_seed()
    else:
        with pytest.raises(AssertionError, match="cannot run when `filter_seeds` is true"):
            conf._validate_nemesis_can_run_on_non_seed()
