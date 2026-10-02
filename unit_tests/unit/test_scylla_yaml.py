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
import yaml

from sdcm.provision.scylla_yaml import ServerEncryptionOptions, ClientEncryptionOptions, ScyllaYaml


def test_update_with_scylla_yaml_object():
    yaml1 = ScyllaYaml(cluster_name="cluster1", redis_keyspace_replication_strategy="NetworkTopologyStrategy")
    yaml2 = ScyllaYaml(
        redis_keyspace_replication_strategy="SimpleStrategy",
        client_encryption_options=ClientEncryptionOptions(
            enabled=True,
            certificate="/tmp/123.crt",
            keyfile="/tmp/123.key",
            truststore="/tmp/trust.pem",
        ),
        server_encryption_options=ServerEncryptionOptions(
            internode_encryption="all",
            certificate="/tmp/123.crt",
            keyfile="/tmp/123.key",
            truststore="/tmp/trust.pem",
        ),
    )
    yaml3 = ScyllaYaml(client_encryption_options=ClientEncryptionOptions())
    yaml1.update(yaml2, yaml3)
    assert yaml1 == ScyllaYaml(
        cluster_name="cluster1",
        redis_keyspace_replication_strategy="SimpleStrategy",
        server_encryption_options=ServerEncryptionOptions(
            internode_encryption="all",
            certificate="/tmp/123.crt",
            keyfile="/tmp/123.key",
            truststore="/tmp/trust.pem",
        ),
        client_encryption_options=ClientEncryptionOptions(),
    )


def test_update_with_dict_object(test_data_dir):
    yaml1 = ScyllaYaml(cluster_name="cluster1", redis_keyspace_replication_strategy="NetworkTopologyStrategy")
    test_config_file = test_data_dir / "scylla_yaml_update.yaml"
    with open(test_config_file, encoding="utf-8") as test_file:
        test_config_file_yaml = yaml.safe_load(test_file)
        append_scylla_args_dict = test_config_file_yaml.get("append_scylla_yaml", {})
    yaml1.update(append_scylla_args_dict)
    assert yaml1.enable_sstables_mc_format == append_scylla_args_dict["enable_sstables_mc_format"]
    assert yaml1.enable_sstables_md_format == append_scylla_args_dict["enable_sstables_md_format"]

    assert yaml1.force_schema_commit_log == append_scylla_args_dict["force_schema_commit_log"]


def test_update_with_extra_audit_rules():
    audit_rules = [
        {
            "sinks": ["syslog"],
            "categories": ["DML"],
            "qualified_table_names": ["audit_keyspace.*"],
            "roles": ["*"],
        }
    ]
    yaml1 = ScyllaYaml()

    yaml1.update({"audit_rules": audit_rules})

    dumped_yaml = yaml1.model_dump(exclude_defaults=True, exclude_unset=True, exclude_none=True)
    assert dumped_yaml["audit_rules"] == audit_rules


def test_copy():
    original = ScyllaYaml(
        redis_keyspace_replication_strategy="SimpleStrategy",
        client_encryption_options=ClientEncryptionOptions(
            enabled=True,
            certificate="/tmp/123.crt",
            keyfile="/tmp/123.key",
            truststore="/tmp/trust.pem",
        ),
        server_encryption_options=ServerEncryptionOptions(
            internode_encryption="all",
            certificate="/tmp/123.crt",
            keyfile="/tmp/123.key",
            truststore="/tmp/trust.pem",
        ),
    )
    copy_instance = original.copy()
    assert copy_instance == original
    assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) == original.model_dump(
        exclude_unset=True, exclude_defaults=True
    )
    copy_instance.client_encryption_options.enabled = False
    assert copy_instance.client_encryption_options.enabled is False
    assert original.client_encryption_options.enabled is True
    assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) != original.model_dump(
        exclude_unset=True, exclude_defaults=True
    )

    copy_instance = original.copy()
    copy_instance.client_encryption_options = None
    assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) != original.model_dump(
        exclude_unset=True, exclude_defaults=True
    )
