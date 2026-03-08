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
# Copyright (c) 2021 ScyllaDB

from copy import copy

from sdcm.provision.scylla_yaml import ScyllaYaml


def test_update_preserves_explicitly_set_default_values():
    """Test that update() marks fields as set when source had them explicitly set,
    even if the value matches the target's default."""
    source = ScyllaYaml(auth_superuser_name="cassandra")
    assert "auth_superuser_name" in source.model_fields_set

    target = ScyllaYaml()
    assert "auth_superuser_name" not in target.model_fields_set

    target.update(source)
    assert "auth_superuser_name" in target.model_fields_set
    assert target.auth_superuser_name == "cassandra"


def test_update_preserves_auth_superuser_salted_password():
    """Test that update() preserves auth_superuser_salted_password when explicitly set."""
    default_hash = ScyllaYaml().auth_superuser_salted_password
    source = ScyllaYaml(auth_superuser_salted_password=default_hash)
    assert "auth_superuser_salted_password" in source.model_fields_set

    target = ScyllaYaml()
    target.update(source)
    assert "auth_superuser_salted_password" in target.model_fields_set
    assert target.auth_superuser_salted_password == default_hash


def test_copy_preserves_non_none_defaults():
    """Test that __copy__ preserves fields with non-None defaults when explicitly set."""
    original = ScyllaYaml(auth_superuser_name="cassandra", cluster_name="test")
    assert "auth_superuser_name" in original.model_fields_set

    copied = copy(original)
    assert copied.auth_superuser_name == "cassandra"
    assert "auth_superuser_name" in copied.model_fields_set
    # exclude_unset=True should keep explicitly set fields even if they match defaults
    dumped = copied.model_dump(exclude_unset=True)
    assert "auth_superuser_name" in dumped


def test_copy_method_preserves_non_none_defaults():
    """Test that .copy() preserves fields with non-None defaults when explicitly set."""
    original = ScyllaYaml(auth_superuser_name="cassandra", cluster_name="test")
    copied = original.copy()
    assert copied.auth_superuser_name == "cassandra"
    assert "auth_superuser_name" in copied.model_fields_set


def test_auth_defaults_survive_config_setup_flow():
    """Verify auth_superuser_name and auth_superuser_salted_password
    are preserved through the config_setup serialize/deserialize cycle.

    This simulates the flow in remote_scylla_yaml():
    1. Create ScyllaYaml from remote data (empty dict simulates no auth fields in remote yaml)
    2. Update with proposed config that has auth fields explicitly set
    3. Serialize with exclude_defaults=True, exclude_unset=True, explicit=[auth fields]
    4. Verify auth fields are present in serialized output
    """
    # Simulate remote scylla.yaml (no auth fields)
    remote_yaml = ScyllaYaml()

    # Simulate proposed_scylla_yaml with auth fields explicitly set
    proposed = ScyllaYaml(
        auth_superuser_name="cassandra",
        auth_superuser_salted_password=ScyllaYaml().auth_superuser_salted_password,
    )

    # Simulate config_setup update flow
    remote_yaml.update(proposed)

    # Simulate remote_scylla_yaml serialization (matching cluster.py explicit list)
    serialized = remote_yaml.model_dump(
        exclude_none=True,
        exclude_unset=True,
        exclude_defaults=True,
        explicit=["partitioner", "commitlog_sync", "commitlog_sync_period_in_ms", "endpoint_snitch",
                  "auth_superuser_name", "auth_superuser_salted_password"],
    )

    assert "auth_superuser_name" in serialized
    assert "auth_superuser_salted_password" in serialized
    assert serialized["auth_superuser_name"] == "cassandra"


def test_round_trip_serialize_deserialize():
    """Verify that ScyllaYaml fields survive a serialize/deserialize round-trip
    when they were explicitly set via update()."""
    source = ScyllaYaml(
        auth_superuser_name="cassandra",
        cluster_name="test-cluster",
    )

    target = ScyllaYaml()
    target.update(source)

    # Round-trip through model_dump (exclude_unset only) + construction
    dumped = target.model_dump(exclude_unset=True)
    restored = ScyllaYaml(**dumped)

    assert restored.auth_superuser_name == "cassandra"
    assert restored.cluster_name == "test-cluster"
    assert "auth_superuser_name" in restored.model_fields_set


def test_diff_detects_explicitly_set_default_values():
    """Test that diff() detects changes when a field with a default value
    is explicitly set on one object but not the other."""
    yaml1 = ScyllaYaml()
    yaml2 = ScyllaYaml(auth_superuser_name="cassandra")

    diff = yaml1.diff(yaml2)
    assert "auth_superuser_name" in diff
