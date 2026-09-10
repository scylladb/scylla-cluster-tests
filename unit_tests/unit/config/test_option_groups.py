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

"""Guards for how configuration options are grouped and documented.

The mixins under `sdcm/sct_config/mixins/` are what make the generated documentation browsable by
domain, so a misfiled or undocumented option is a documentation bug, not just untidy source. These
tests keep the conventions from drifting back:

- every option lives in exactly one mixin,
- an option's name prefix agrees with the group it is in,
- every option has a description that says more than its own name.

The original `sct_config.py` had drifted on all three counts: options were appended to whatever
section happened to be last (37 unrelated options had accumulated under `# minicloud params`), and
19 options had no description at all.
"""

import re

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_config.config import is_ignored_field
from sdcm.sct_config.mixins import CONFIG_GROUPS


def _module(mixin):
    return mixin.__module__.rsplit(".", 1)[-1]


DOCUMENTED_OPTIONS = {
    name: field
    for name, field in SCTConfiguration.model_fields.items()
    if not (field.exclude or is_ignored_field(field))
}

OPTION_GROUP = {name: _module(mixin) for mixin in CONFIG_GROUPS for name in mixin.model_fields}

# name prefix -> the group an option with that prefix belongs in
PREFIX_CONVENTION = {
    "minicloud_": "minicloud",
    "xcloud_": "xcloud",
    "gce_": "gce",
    "azure_": "azure",
    "oci_": "oci",
    "eks_": "kubernetes",
    "gke_": "kubernetes",
    "k8s_": "kubernetes",
    "mgmt_": "manager",
    "manager_": "manager",
    "emr_": "emr",
    "migrator_": "spark_migrator",
    "jepsen_": "jepsen",
    "vector_store_": "vector_store",
    "nemesis_": "nemesis",
    "perf_": "performance",
    "keystore_": "common",
    "post_behavior_": "logs",
    "run_scylla_doctor": "scylla_doctor",
    "scylla_doctor_": "scylla_doctor",
}

# Options whose name prefix says one thing and whose job says another. Each needs a reason.
PREFIX_EXCEPTIONS = {
    # cassandra-stress settings scoped to the grow-cluster test, not the aux Cassandra cluster
    "cassandra_stress_population_size": "grow_cluster",
    "cassandra_stress_threads": "grow_cluster",
    # a docker image for a component that is not the docker backend
    "mgmt_docker_image": "manager",
    "vector_store_docker_image": "vector_store",
    "vector_store_version": "vector_store",
    "docker_image_cassandra": "aux_db",
    # AWS-specific variant of the generic fallback option, which lives in common
    "aws_fallback_to_next_availability_zone": "aws",
    # stress rates for the Alternator API, set alongside the other stress rates
    "alternator_stress_rate": "stress",
    "alternator_write_always_lwt_stress_rate": "stress",
    # gradual-load stepping, which is a performance-test concern
    "n_stress_process": "performance",
    "stress_process_step": "performance",
    "stress_threads_start_num": "performance",
    "stress_step_duration": "performance",
    # the management-tests scylla repo, used to install Scylla for manager tests
    "scylla_repo_m": "manager",
    "scylla_mgmt_address": "manager",
    "scylla_mgmt_agent_address": "manager",
    "scylla_mgmt_agent_version": "manager",
    "scylla_mgmt_pkg": "manager",
    "scylla_mgmt_upgrade_to_repo": "manager",
    # oracle-cluster variants of scylla options, owned by the auxiliary cluster
    "oracle_scylla_version": "aux_db",
    "oracle_user_data_format_version": "aux_db",
    "append_scylla_args_oracle": "aux_db",
    # upgrade-scenario options that happen to start with a scylla/stress prefix
    "new_scylla_repo": "upgrade",
    "stress_before_upgrade": "upgrade",
    "stress_during_entire_upgrade": "upgrade",
    "stress_after_cluster_upgrade": "upgrade",
    "verify_stress_after_cluster_upgrade": "upgrade",
    "large_partition_stress_during_upgrade": "upgrade",
    "stress_before_migration": "stress",
    # scylla-side monitoring/logging plumbing
    "scylla_rsyslog_setup": "monitoring",
}


def test_every_option_belongs_to_exactly_one_mixin():
    """An option missing from every mixin would fall into the docs' "Other" bucket."""
    ungrouped = sorted(set(DOCUMENTED_OPTIONS) - set(OPTION_GROUP))
    assert not ungrouped, f"options declared on SCTConfiguration itself instead of a mixin: {ungrouped}"

    seen = {}
    duplicated = []
    for mixin in CONFIG_GROUPS:
        for name in mixin.model_fields:
            if name in seen:
                duplicated.append((name, seen[name], _module(mixin)))
            seen[name] = _module(mixin)
    assert not duplicated, f"options declared in more than one mixin: {duplicated}"


@pytest.mark.parametrize("name", sorted(DOCUMENTED_OPTIONS))
def test_option_name_prefix_agrees_with_its_group(name):
    """`gce_*` in the AWS group is how the old file ended up unbrowsable."""
    group = OPTION_GROUP.get(name)
    if name in PREFIX_EXCEPTIONS:
        assert group == PREFIX_EXCEPTIONS[name], (
            f"{name} is listed as a prefix exception for group {PREFIX_EXCEPTIONS[name]!r} "
            f"but now lives in {group!r} -- update the exception or the grouping"
        )
        return
    for prefix, expected in PREFIX_CONVENTION.items():
        if name.startswith(prefix):
            assert group == expected, (
                f"{name} is in the {group!r} group but its {prefix!r} prefix says {expected!r}. "
                f"Move it, or add it to PREFIX_EXCEPTIONS with a reason."
            )
            break


@pytest.mark.parametrize("name", sorted(DOCUMENTED_OPTIONS))
def test_option_has_a_useful_description(name):
    """A description that only restates the name tells a reader nothing and hides the right group."""
    description = " ".join((DOCUMENTED_OPTIONS[name].description or "").split())
    assert description, f"{name} has no description; it would appear blank in configuration_options.md"

    words = re.findall(r"[a-z0-9]+", description.lower())
    name_words = [w for w in name.split("_") if len(w) > 2]
    only_restates = len(words) <= len(name_words) + 2 and all(w in words for w in name_words)
    assert not only_restates, (
        f"{name} description {description!r} only restates the option name -- say what it controls, "
        f"what the accepted values mean, or which component it applies to"
    )


def test_every_group_is_non_empty_and_titled():
    """An empty or untitled mixin would render as a stray heading in the docs."""
    for mixin in CONFIG_GROUPS:
        assert mixin.model_fields, f"{mixin.__name__} declares no options"
        assert getattr(mixin, "config_group", None), f"{mixin.__name__} has no config_group title"

    titles = [mixin.config_group for mixin in CONFIG_GROUPS]
    assert len(titles) == len(set(titles)), f"duplicate group titles: {titles}"
