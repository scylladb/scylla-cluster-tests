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
"""Pipeline generation for the nemesis jobs."""

from pathlib import Path

from sdcm.nemesis.generator import NemesisJobGenerator


def make_generator(base_dir: Path) -> NemesisJobGenerator:
    """A generator with just enough state for the config-list helpers."""
    instance = NemesisJobGenerator.__new__(NemesisJobGenerator)
    instance.base_dir = base_dir
    return instance


def test_a_network_profile_picks_up_the_layout_of_its_backend(tmp_path):
    layout = tmp_path / "configurations" / "azure" / "network_config" / "two_interfaces.yaml"
    layout.parent.mkdir(parents=True)
    layout.write_text("azure_network_interfaces: []\n", encoding="utf-8")

    paired = make_generator(tmp_path).with_backend_network_layouts(
        ["configurations/nemesis/BlockNetworkMonkey.yaml", "configurations/network_config/two_interfaces.yaml"],
        "azure",
    )

    assert paired == [
        "configurations/nemesis/BlockNetworkMonkey.yaml",
        "configurations/network_config/two_interfaces.yaml",
        str(layout),
    ]


def test_a_backend_without_a_layout_keeps_the_profile_alone(tmp_path):
    """AWS builds its interfaces from the profile itself, so there is nothing to pair."""
    configs = ["configurations/network_config/two_interfaces.yaml"]

    assert make_generator(tmp_path).with_backend_network_layouts(configs, "aws") == configs


def test_configs_outside_network_config_are_left_alone(tmp_path):
    layout = tmp_path / "configurations" / "azure" / "network_config" / "sla_config.yaml"
    layout.parent.mkdir(parents=True)
    layout.write_text("azure_network_interfaces: []\n", encoding="utf-8")
    configs = ["configurations/nemesis/additional_configs/sla_config.yaml"]

    assert make_generator(tmp_path).with_backend_network_layouts(configs, "azure") == configs
