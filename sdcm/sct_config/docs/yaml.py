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

"""The option reference as one YAML listing."""

from sdcm.sct_config.docs import GroupDoc, OptionDoc, Renderer


class YamlRenderer(Renderer[str]):
    """One YAML listing: help as comments, section fields nested under their section."""

    def render(self, groups: list[GroupDoc]) -> str:
        return "".join(
            f"### {group.title}\n\n" + "".join(self.entry(option) for option in group.options) for group in groups
        )

    @staticmethod
    def entry(option: OptionDoc) -> str:
        """The YAML of one option: its help as comments, then its key with the default."""
        indent = "  " * option.depth
        text = "".join(f"{indent}# {line}\n" for line in option.help_lines if line)
        value = "" if option.is_section else f" {option.default_text}"
        return f"{text}{indent}{option.name}:{value}\n\n"
