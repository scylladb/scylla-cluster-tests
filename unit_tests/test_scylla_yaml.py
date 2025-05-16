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
import unittest
from pathlib import Path

import yaml

from sdcm.provision.scylla_yaml import ServerEncryptionOptions, ClientEncryptionOptions, SeedProvider, ScyllaYaml
from sdcm.provision.scylla_yaml.auxiliaries import RequestSchedulerOptions


class ScyllaYamlTest(unittest.TestCase):

    def test_server_encryption_options(self):
        instance = ServerEncryptionOptions(
            certificate='/tmp/123.crt',
            keyfile='/tmp/123.key',
        )
        assert instance.model_dump(exclude_unset=True) == {
            'certificate': '/tmp/123.crt',
            'keyfile': '/tmp/123.key',
        }

    def test_client_encryption_options(self):
        instance = ClientEncryptionOptions(
            certificate='/tmp/123.crt',
            keyfile='/tmp/123.key',
            truststore='/tmp/123.pem',
            require_client_auth=True,
        )
        assert instance.model_dump(exclude_unset=True) == {
            'certificate': '/tmp/123.crt',
            'keyfile': '/tmp/123.key',
            'truststore': '/tmp/123.pem',
            'require_client_auth': True
        }

    def test_request_scheduler_options(self):
        instance = RequestSchedulerOptions(
            throttle_limit=100,
            default_weight=1,
        )
        assert instance.model_dump(exclude_unset=True) == {
            'throttle_limit': 100,
            'default_weight': 1,
        }

    def test_seed_provider(self):
        instance = SeedProvider(
            class_name='org.apache.cassandra.locator.SimpleSeedProvider',
            parameters=[{'seeds': ['1.1.1.1', '2.2.2.2']}]
        )
        assert instance.model_dump(exclude_unset=True) == {
            'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
            'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}]
        }

    def test_scylla_yaml(self):
        instance = ScyllaYaml(
            log_to_stdout=True,
            auto_adjust_flush_quota=False,
            background_writer_scheduling_quota=1.0,
            listen_address="localhost",
            prometheus_prefix='someprefix',
            server_encryption_options=ServerEncryptionOptions(
                internode_encryption='none',
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
            ),
            client_encryption_options=ClientEncryptionOptions(
                enabled=False,
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
                require_client_auth=True,
            ),
            seed_provider=[
                SeedProvider(
                    class_name='org.apache.cassandra.locator.SimpleSeedProvider',
                    parameters=[{'seeds': ['1.1.1.1', '2.2.2.2']}]),
            ],
            force_schema_commit_log=True,
        )
        assert instance.model_dump(exclude_unset=True) == {
            'background_writer_scheduling_quota': 1.0,
            'auto_adjust_flush_quota': False,
            'listen_address': 'localhost',
            'seed_provider': [
                {
                    'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                    'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}]
                }
            ],
            'server_encryption_options': {
                'internode_encryption': 'none',
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key'
            },
            'client_encryption_options': {
                'enabled': False,
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
                'require_client_auth': True
            },
            'prometheus_prefix': 'someprefix',
            'log_to_stdout': True,
            'force_schema_commit_log': True,
        }

    @staticmethod
    def test_update_with_scylla_yaml_object():
        yaml1 = ScyllaYaml(cluster_name='cluster1', redis_keyspace_replication_strategy='NetworkTopologyStrategy')
        yaml2 = ScyllaYaml(
            redis_keyspace_replication_strategy='SimpleStrategy',
            client_encryption_options=ClientEncryptionOptions(
                enabled=True,
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
                truststore='/tmp/trust.pem',
            ),
            server_encryption_options=ServerEncryptionOptions(
                internode_encryption='all',
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
                truststore='/tmp/trust.pem',
            )
        )
        yaml3 = ScyllaYaml(client_encryption_options=ClientEncryptionOptions())
        yaml1.update(yaml2, yaml3)

        # Create expected result to compare against
        expected = ScyllaYaml(
            cluster_name='cluster1',
            redis_keyspace_replication_strategy='SimpleStrategy',
            server_encryption_options=ServerEncryptionOptions(internode_encryption='all',
                                                              certificate='/tmp/123.crt', keyfile='/tmp/123.key',
                                                              truststore='/tmp/trust.pem'),
            client_encryption_options=ClientEncryptionOptions()
        )
        assert yaml1 == expected

    @staticmethod
    def test_update_with_dict_object():
        yaml1 = ScyllaYaml(cluster_name='cluster1', redis_keyspace_replication_strategy='NetworkTopologyStrategy')
        test_config_file = Path(__file__).parent / 'test_data' / 'scylla_yaml_update.yaml'
        with open(test_config_file, encoding="utf-8") as test_file:
            test_config_file_yaml = yaml.safe_load(test_file)
            append_scylla_args_dict = test_config_file_yaml.get("append_scylla_yaml", {})
        yaml1.update(append_scylla_args_dict)
        assert yaml1.enable_sstables_mc_format == append_scylla_args_dict["enable_sstables_mc_format"]
        assert yaml1.enable_sstables_md_format == append_scylla_args_dict["enable_sstables_md_format"]

        assert yaml1.force_schema_commit_log == append_scylla_args_dict["force_schema_commit_log"]

    @staticmethod
    def test_copy():
        original = ScyllaYaml(
            redis_keyspace_replication_strategy='SimpleStrategy',
            client_encryption_options=ClientEncryptionOptions(
                enabled=True,
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
                truststore='/tmp/trust.pem',
            ),
            server_encryption_options=ServerEncryptionOptions(
                internode_encryption='all',
                certificate='/tmp/123.crt',
                keyfile='/tmp/123.key',
                truststore='/tmp/trust.pem',
            )
        )
        copy_instance = original.copy()
        assert copy_instance == original
        assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) == original.model_dump(
            exclude_unset=True, exclude_defaults=True)
        copy_instance.client_encryption_options.enabled = False
        assert copy_instance.client_encryption_options.enabled is False
        assert original.client_encryption_options.enabled is True
        assert copy_instance != original
        assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) != original.model_dump(
            exclude_unset=True, exclude_defaults=True)

        copy_instance = original.copy()
        copy_instance.client_encryption_options = None
        assert copy_instance != original
        assert copy_instance.model_dump(exclude_unset=True, exclude_defaults=True) != original.model_dump(
            exclude_unset=True, exclude_defaults=True)
