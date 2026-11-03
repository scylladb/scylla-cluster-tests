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

"""`utils/migrate_config_section.py` rewrites the flat nemesis options of a YAML file into the `nemesis` section."""

from textwrap import dedent

import pytest

from utils.migrate_config_section import rewrite_nemesis


def rewrite(text: str, path: str = "test.yaml") -> str:
    return rewrite_nemesis(dedent(text).lstrip(), path)


def test_single_runner_takes_its_options_and_keeps_other_keys_and_comments():
    rewritten = rewrite("""
        test_duration: 60

        # run every disruptive nemesis
        nemesis_class_name: 'SisyphusMonkey'
        nemesis_selector: "disruptive"  # all of them
        # a quiet cluster in between
        nemesis_interval: 5
        nemesis_during_prepare: false
        n_db_nodes: 3
    """)

    assert (
        rewritten
        == dedent("""
        test_duration: 60

        # run every disruptive nemesis
        nemesis:
          runners:
            - SisyphusMonkey:
                selector: "disruptive"  # all of them
                # a quiet cluster in between
                interval: 5
          during_prepare: false
        n_db_nodes: 3
    """).lstrip()
    )


def test_single_runner_listed_after_its_options_takes_them():
    rewritten = rewrite("""
        nemesis_seed: [12]
        nemesis_class_name: ['SisyphusMonkey']
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - SisyphusMonkey:
                seed: 12
    """).lstrip()
    )


def test_appended_runner_options_become_defaults():
    rewritten = rewrite("""
        nemesis_class_name: ['++', 'SisyphusMonkey']
        nemesis_interval: 5
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - "++"
            - SisyphusMonkey: {}
          interval: 5
    """).lstrip()
    )


def test_single_fixed_order_runner_takes_selector_as_order_and_leaves_multiply_factor_a_default():
    rewritten = rewrite("""
        nemesis_class_name: 'FixedOrderMonkey'
        nemesis_selector: 'DecommissionMonkey, NoCorruptRepairMonkey'
        nemesis_multiply_factor: 3
        nemesis_interval: 5
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - FixedOrderMonkey:
                order: ["DecommissionMonkey", "NoCorruptRepairMonkey"]
                interval: 5
          multiply_factor: 3
    """).lstrip()
    )


def test_file_stacked_with_nemesis_overlays_keeps_every_option_in_the_section():
    rewritten = rewrite(
        """
        nemesis_class_name: 'SisyphusMonkey'
        nemesis_selector: 'disruptive'
        nemesis_interval: 5
    """,
        "/checkout/test-cases/longevity/longevity-twcs-48h.yaml",
    )

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - SisyphusMonkey: {}
          selector: 'disruptive'
          interval: 5
    """).lstrip()
    )


def test_parallel_runners_take_their_own_selector_and_seed():
    rewritten = rewrite("""
        nemesis_class_name: ['SisyphusMonkey', 'SisyphusMonkey']
        # the second runner keeps away from compaction
        nemesis_selector: ["topology_changes", "schema_changes and not disruptive"]
        nemesis_seed: [253, 328]
        nemesis_during_prepare: false
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          # the second runner keeps away from compaction
          runners:
            - SisyphusMonkey:
                selector: "topology_changes"
                seed: 253
            - SisyphusMonkey:
                selector: "schema_changes and not disruptive"
                seed: 328
          during_prepare: false
    """).lstrip()
    )


def test_equal_values_per_runner_become_the_default():
    rewritten = rewrite("""
        nemesis_class_name: [SisyphusMonkey, SisyphusMonkey]
        nemesis_selector: ["MajorCompactionMonkey", "MajorCompactionMonkey"]
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - SisyphusMonkey: {}
            - SisyphusMonkey: {}
          selector: "MajorCompactionMonkey"
    """).lstrip()
    )


def test_overlay_options_become_defaults():
    rewritten = rewrite("""
        nemesis_selector: '++ and not disruptive'
        nemesis_interval: 2
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          selector: '++ and not disruptive'
          interval: 2
    """).lstrip()
    )


def test_scattered_options_are_gathered_at_the_first_one():
    rewritten = rewrite("""
        nemesis_add_node_cnt: 3

        stress_cmd: "cassandra-stress write"

        nemesis_grow_shrink_instance_type: 'i4i.large'
        sla: true
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          grow_shrink:
            add_node_cnt: 3
            instance_type: 'i4i.large'
          sla: true

        stress_cmd: "cassandra-stress write"
    """).lstrip()
    )


def test_fixed_order_selector_becomes_its_order():
    rewritten = rewrite("""
        nemesis_class_name: ['SisyphusMonkey', 'FixedOrderMonkey']
        nemesis_selector: ["topology_changes", "DecommissionMonkey, NoCorruptRepairMonkey"]
    """)

    assert (
        rewritten
        == dedent("""
        nemesis:
          runners:
            - SisyphusMonkey:
                selector: "topology_changes"
            - FixedOrderMonkey:
                order: ["DecommissionMonkey", "NoCorruptRepairMonkey"]
    """).lstrip()
    )


def test_file_without_flat_options_is_left_alone():
    assert rewrite("nemesis:\n  interval: 5\n") is None


def test_per_runner_values_without_runners_fail():
    with pytest.raises(ValueError, match="nemesis_selector lists one value per runner"):
        rewrite("""
            nemesis_selector: ["disruptive", "not disruptive"]
        """)
