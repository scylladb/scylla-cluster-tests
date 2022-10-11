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

    @staticmethod
    def _run_test(object_type, init_params: dict, expected_without_defaults: dict, expected_with_defaults: dict):
        instance = object_type(**init_params)
        assert instance.dict(exclude_unset=True) == expected_without_defaults
        assert instance.dict(exclude_unset=False) == expected_with_defaults

    def test_server_encryption_options(self):
        self._run_test(
            object_type=ServerEncryptionOptions,
            init_params={
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
            },
            expected_without_defaults={
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
            },
            expected_with_defaults={
                'internode_encryption': 'none',
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
                'truststore': None,
                'priority_string': None,
                'require_client_auth': False
            }
        )

    def test_client_encryption_options(self):
        self._run_test(
            object_type=ClientEncryptionOptions,
            init_params={
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
                'truststore': '/tmp/123.pem',
                'require_client_auth': True,
            },
            expected_without_defaults={
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
                'truststore': '/tmp/123.pem',
                'require_client_auth': True
            },
            expected_with_defaults={
                'enabled': False,
                'certificate': '/tmp/123.crt',
                'keyfile': '/tmp/123.key',
                'truststore': '/tmp/123.pem',
                'priority_string': None,
                'require_client_auth': True
            }
        )

    def test_request_scheduler_options(self):
        self._run_test(
            object_type=RequestSchedulerOptions,
            init_params={
                'throttle_limit': 100,
                'default_weight': 1,
            },
            expected_without_defaults={'throttle_limit': 100, 'default_weight': 1},
            expected_with_defaults={'throttle_limit': 100, 'default_weight': 1, 'weights': 1}
        )

    def test_seed_provider(self):
        self._run_test(
            object_type=SeedProvider,
            init_params={
                'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}]
            },
            expected_without_defaults={
                'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}]},
            expected_with_defaults={
                'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}]
            }
        )

    def test_scylla_yaml(self):
        self._run_test(
            object_type=ScyllaYaml,
            init_params={
                'log_to_stdout': True,
                'auto_adjust_flush_quota': False,
                'background_writer_scheduling_quota': 1.0,
                'listen_address': "localhost",
                'prometheus_prefix': 'someprefix',
                'server_encryption_options': ServerEncryptionOptions(
                    internode_encryption='none',
                    certificate='/tmp/123.crt',
                    keyfile='/tmp/123.key',
                ),
                'client_encryption_options': ClientEncryptionOptions(
                    enabled=False,
                    certificate='/tmp/123.crt',
                    keyfile='/tmp/123.key',
                    require_client_auth=True,
                ),
                'seed_provider': [
                    SeedProvider(
                        class_name='org.apache.cassandra.locator.SimpleSeedProvider',
                        parameters=[{'seeds': ['1.1.1.1', '2.2.2.2']}]),
                ],
            },
            expected_without_defaults={
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
            },
            expected_with_defaults={
                'broadcast_address': '',
                'api_port': 10000,
                'api_address': '',
                'ssl_storage_port': 7001,
                'background_writer_scheduling_quota': 1.0,
                'auto_adjust_flush_quota': False,
                'memtable_flush_static_shares': 0,
                'compaction_static_shares': 0,
                'compaction_enforce_min_threshold': False,
                'cluster_name': '',
                'listen_address': 'localhost',
                'listen_interface': 'eth0',
                'listen_interface_prefer_ipv6': False,
                'commitlog_directory': '',
                'data_file_directories': None,
                'hints_directory': '',
                'view_hints_directory': '',
                'saved_caches_directory': '',
                'commit_failure_policy': 'stop',
                'disk_failure_policy': 'stop',
                'endpoint_snitch': 'org.apache.cassandra.locator.SimpleSnitch',
                'rpc_address': 'localhost',
                'rpc_interface': 'eth1',
                'rpc_interface_prefer_ipv6': False,
                'seed_provider': [
                    {
                        'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                        'parameters': [{'seeds': ['1.1.1.1', '2.2.2.2']}],
                    },
                ],
                'compaction_throughput_mb_per_sec': 0,
                'compaction_large_partition_warning_threshold_mb': 1000,
                'compaction_large_row_warning_threshold_mb': 10,
                'compaction_large_cell_warning_threshold_mb': 1,
                'compaction_rows_count_warning_threshold': 100000,
                'memtable_total_space_in_mb': 0,
                'concurrent_reads': 32,
                'concurrent_writes': 32,
                'concurrent_counter_writes': 32,
                'incremental_backups': False,
                'snapshot_before_compaction': False,
                'phi_convict_threshold': 8,
                'commitlog_sync': 'periodic',
                'commitlog_segment_size_in_mb': 64,
                'commitlog_sync_period_in_ms': 10000,
                'commitlog_sync_batch_window_in_ms': 10000,
                'commitlog_total_space_in_mb': -1,
                'commitlog_reuse_segments': True,
                'commitlog_use_o_dsync': True,
                'compaction_preheat_key_cache': True,
                'concurrent_compactors': 0,
                'in_memory_compaction_limit_in_mb': 64,
                'preheat_kernel_page_cache': False,
                'sstable_preemptive_open_interval_in_mb': 50,
                'defragment_memory_on_idle': False,
                'memtable_allocation_type': 'heap_buffers',
                'memtable_cleanup_threshold': 0.11,
                'file_cache_size_in_mb': 512,
                'memtable_flush_queue_size': 4,
                'memtable_flush_writers': 1,
                'memtable_heap_space_in_mb': 0,
                'memtable_offheap_space_in_mb': 0,
                'column_index_size_in_kb': 64,
                'index_summary_capacity_in_mb': 0,
                'index_summary_resize_interval_in_minutes': 60,
                'reduce_cache_capacity_to': 0.6,
                'reduce_cache_sizes_at': 0.85,
                'stream_throughput_outbound_megabits_per_sec': 400,
                'inter_dc_stream_throughput_outbound_megabits_per_sec': 0,
                'trickle_fsync': False,
                'trickle_fsync_interval_in_kb': 10240,
                'auto_bootstrap': True,
                'batch_size_warn_threshold_in_kb': 5,
                'batch_size_fail_threshold_in_kb': 50,
                'listen_on_broadcast_address': False,
                'initial_token': None,
                'num_tokens': 1,
                'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner',
                'storage_port': 7000,
                'auto_snapshot': True,
                'key_cache_keys_to_save': 0,
                'key_cache_save_period': 14400,
                'key_cache_size_in_mb': 100,
                'row_cache_keys_to_save': 0,
                'row_cache_size_in_mb': 0,
                'row_cache_save_period': 0,
                'memory_allocator': 'NativeAllocator',
                'counter_cache_size_in_mb': 0,
                'counter_cache_save_period': 7200,
                'counter_cache_keys_to_save': 0,
                'tombstone_warn_threshold': 1000,
                'tombstone_failure_threshold': 100000,
                'range_request_timeout_in_ms': 10000,
                'read_request_timeout_in_ms': 5000,
                'counter_write_request_timeout_in_ms': 5000,
                'cas_contention_timeout_in_ms': 1000,
                'truncate_request_timeout_in_ms': 60000,
                'write_request_timeout_in_ms': 2000,
                'request_timeout_in_ms': 10000,
                'cross_node_timeout': False,
                'internode_send_buff_size_in_bytes': 0,
                'internode_recv_buff_size_in_bytes': 0,
                'internode_compression': 'none',
                'inter_dc_tcp_nodelay': False,
                'streaming_socket_timeout_in_ms': 0,
                'start_native_transport': True,
                'native_transport_port': 9042,
                'native_transport_port_ssl': 9142,
                'native_transport_max_threads': 128,
                'native_transport_max_frame_size_in_mb': 256,
                'native_shard_aware_transport_port': 19042,
                'native_shard_aware_transport_port_ssl': 19142,
                'broadcast_rpc_address': None,
                'rpc_port': 9160,
                'start_rpc': True,
                'rpc_keepalive': True,
                'rpc_max_threads': 0, 'rpc_min_threads': 16,
                'rpc_recv_buff_size_in_bytes': 0,
                'rpc_send_buff_size_in_bytes': 0,
                'rpc_server_type': 'sync',
                'cache_hit_rate_read_balancing': True,
                'dynamic_snitch_badness_threshold': 0,
                'dynamic_snitch_reset_interval_in_ms': 60000,
                'dynamic_snitch_update_interval_in_ms': 100,
                'hinted_handoff_enabled': 'enabled',
                'hinted_handoff_throttle_in_kb': 1024,
                'max_hint_window_in_ms': 10800000,
                'max_hints_delivery_threads': 2,
                'batchlog_replay_throttle_in_kb': 1024,
                'request_scheduler': 'org.apache.cassandra.scheduler.NoScheduler',
                'request_scheduler_id': None,
                'request_scheduler_options': None,
                'thrift_framed_transport_size_in_mb': 15,
                'thrift_max_message_length_in_mb': 16,
                'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
                'internode_authenticator': 'disabled',
                'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer',
                'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                'permissions_validity_in_ms': 10000,
                'permissions_update_interval_in_ms': 2000,
                'permissions_cache_max_entries': 1000,
                'server_encryption_options': {
                    'internode_encryption': 'none',
                    'certificate': '/tmp/123.crt',
                    'keyfile': '/tmp/123.key',
                    'truststore': None,
                    'priority_string': None,
                    'require_client_auth': False
                },
                'client_encryption_options': {
                    'enabled': False,
                    'certificate': '/tmp/123.crt',
                    'keyfile': '/tmp/123.key',
                    'truststore': None,
                    'priority_string': None,
                    'require_client_auth': True
                },
                'enable_in_memory_data_store': False,
                'enable_cache': True,
                'enable_commitlog': True,
                'volatile_system_keyspace_for_testing': False,
                'api_ui_dir': 'swagger-ui/dist/',
                'api_doc_dir': 'api/api-doc/',
                'load_balance': 'none',
                'consistent_rangemovement': True,
                'join_ring': True,
                'load_ring_state': True,
                'replace_node': '',
                'replace_token': '',
                'replace_address': '',
                'replace_address_first_boot': '',
                'override_decommission': False,
                'enable_repair_based_node_ops': True,
                'ring_delay_ms': 30000,
                'shadow_round_ms': 300000,
                'fd_max_interval_ms': 2000,
                'fd_initial_value_ms': 2000,
                'shutdown_announce_in_ms': 2000,
                'developer_mode': False,
                'skip_wait_for_gossip_to_settle': -1,
                'force_gossip_generation': -1,
                'experimental': False,
                'experimental_features': [],
                'lsa_reclamation_step': 1,
                'prometheus_port': 9180,
                'prometheus_address': 'localhost',
                'prometheus_prefix': 'someprefix',
                'abort_on_lsa_bad_alloc': False,
                'alternator_address': '0.0.0.0',
                'murmur3_partitioner_ignore_msb_bits': 12,
                'virtual_dirty_soft_limit': 0.6,
                'sstable_summary_ratio': 0.0005,
                'large_memory_allocation_warning_threshold': 1048576,
                'enable_deprecated_partitioners': False,
                'enable_keyspace_column_family_metrics': False,
                'enable_sstable_data_integrity_check': False,
                'enable_sstable_key_validation': None,
                'cpu_scheduler': True,
                'view_building': True,
                'enable_sstables_mc_format': True,
                'enable_sstables_md_format': False,
                'enable_dangerous_direct_import_of_cassandra_counters': False,
                'enable_shard_aware_drivers': True,
                'enable_ipv6_dns_lookup': False,
                'abort_on_internal_error': False,
                'max_partition_key_restrictions_per_query': 100,
                'max_clustering_key_restrictions_per_query': 100,
                'max_memory_for_unlimited_query': 1048576,
                'initial_sstable_loading_concurrency': '4u',
                'enable_3_1_0_compatibility_mode': False,
                'enable_user_defined_functions': False,
                'user_defined_function_time_limit_ms': 10,
                'user_defined_function_allocation_limit_bytes': 1048576,
                'user_defined_function_contiguous_allocation_limit_bytes': 1048576,
                'alternator_port': 0,
                'alternator_https_port': 0,
                'alternator_enforce_authorization': False,
                'alternator_write_isolation': None,
                'alternator_streams_time_window_s': 10,
                'abort_on_ebadf': True,
                'redis_port': 0,
                'redis_ssl_port': 0,
                'redis_read_consistency_level': 'LOCAL_QUORUM',
                'redis_write_consistency_level': 'LOCAL_QUORUM',
                'redis_database_count': 16,
                'redis_keyspace_replication_strategy': 'SimpleStrategy',
                'default_log_level': None,
                'logger_log_level': None,
                'log_to_stdout': True,
                'log_to_syslog': None,
                'authenticator_user': None,
                'authenticator_password': None,
                'workdir': None,
                'ldap_attr_role': None,
                'ldap_bind_dn': None,
                'ldap_bind_passwd': None,
                'ldap_url_template': None,
                'saslauthd_socket_path': None,
                'system_key_directory': None,
                'system_info_encryption': None,
                'kmip_hosts': None,
            }
        )

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
        assert yaml1 == ScyllaYaml(
            cluster_name='cluster1',
            # redis_keyspace_replication_strategy property is not getting changed because of the problem with update()
            # which does not allow to make distinction between default value and value that equals to default
            redis_keyspace_replication_strategy='NetworkTopologyStrategy',
            server_encryption_options=ServerEncryptionOptions(internode_encryption='all',
                                                              certificate='/tmp/123.crt', keyfile='/tmp/123.key',
                                                              truststore='/tmp/trust.pem'),
            client_encryption_options=ClientEncryptionOptions()
        )

    @staticmethod
    def test_update_with_dict_object():
        yaml1 = ScyllaYaml(cluster_name='cluster1', redis_keyspace_replication_strategy='NetworkTopologyStrategy')
        test_config_file = Path(__file__).parent / 'test_data' / 'scylla_yaml_update.yaml'
        with open(test_config_file, encoding="utf-8") as test_file:
            test_config_file_yaml = yaml.load(test_file)
            append_scylla_args_dict = yaml.load(test_config_file_yaml["append_scylla_yaml"])
        yaml1.update(append_scylla_args_dict)
        assert yaml1.enable_sstables_mc_format == append_scylla_args_dict["enable_sstables_mc_format"]
        assert yaml1.enable_sstables_md_format == append_scylla_args_dict["enable_sstables_md_format"]

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
        assert copy_instance.dict(exclude_unset=True, exclude_defaults=True) == original.dict(
            exclude_unset=True, exclude_defaults=True)
        copy_instance.client_encryption_options.enabled = False
        assert copy_instance.client_encryption_options.enabled is False
        assert original.client_encryption_options.enabled is True
        assert copy_instance != original
        assert copy_instance.dict(exclude_unset=True, exclude_defaults=True) != original.dict(
            exclude_unset=True, exclude_defaults=True)

        copy_instance = original.copy()
        copy_instance.client_encryption_options = None
        assert copy_instance != original
        assert copy_instance.dict(exclude_unset=True, exclude_defaults=True) != original.dict(
            exclude_unset=True, exclude_defaults=True)
