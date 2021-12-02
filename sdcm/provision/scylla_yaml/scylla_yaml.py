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
from difflib import unified_diff
from typing import List, Literal, Union

import logging
import yaml
from pydantic import Field, validator, BaseModel  # pylint: disable=no-name-in-module

from sdcm.provision.scylla_yaml.auxiliaries import RequestSchedulerOptions, EndPointSnitchType, SeedProvider, \
    ServerEncryptionOptions, ClientEncryptionOptions


logger = logging.getLogger(__name__)


class ScyllaYaml(BaseModel):  # pylint: disable=too-few-public-methods
    broadcast_address: str = ""
    api_port: int = 10000
    api_address: str = ""
    ssl_storage_port: int = 7001
    background_writer_scheduling_quota: float = 1.0
    auto_adjust_flush_quota: bool = False
    memtable_flush_static_shares: int = 0
    compaction_static_shares: int = 0
    compaction_enforce_min_threshold: bool = False
    cluster_name: str = ""
    listen_address: str = "localhost"
    listen_interface: str = "eth0"
    listen_interface_prefer_ipv6: bool = False
    commitlog_directory: str = ""
    data_file_directories: List[str] = None
    hints_directory: str = ""
    view_hints_directory: str = ""
    saved_caches_directory: str = ""
    commit_failure_policy: Literal["die", "stop", "stop_commit", "ignore"] = "stop"
    disk_failure_policy: Literal["die", "stop_paranoid", "stop", "best_effort", "ignore"] = "stop"
    endpoint_snitch: EndPointSnitchType = Field('org.apache.cassandra.locator.SimpleSnitch')

    # pylint: disable=no-self-argument,no-self-use
    @validator("endpoint_snitch", pre=True, always=True)
    def set_endpoint_snitch(cls, endpoint_snitch: str):
        if endpoint_snitch.startswith('org.apache.cassandra.locator.'):
            return endpoint_snitch
        return 'org.apache.cassandra.locator.' + endpoint_snitch

    rpc_address: str = 'localhost'
    rpc_interface: str = "eth1"
    rpc_interface_prefer_ipv6: bool = False
    seed_provider: List[SeedProvider] = [SeedProvider(class_name='org.apache.cassandra.locator.SimpleSeedProvider')]
    compaction_throughput_mb_per_sec: int = 16
    compaction_large_partition_warning_threshold_mb: int = 1000
    compaction_large_row_warning_threshold_mb: int = 10
    compaction_large_cell_warning_threshold_mb: int = 1
    compaction_rows_count_warning_threshold: int = 100000
    memtable_total_space_in_mb: int = 0
    concurrent_reads: int = 32
    concurrent_writes: int = 32
    concurrent_counter_writes: int = 32
    incremental_backups: bool = False
    snapshot_before_compaction: bool = False
    phi_convict_threshold: int = 8
    commitlog_sync: Literal['periodic', 'batch'] = 'periodic'
    commitlog_segment_size_in_mb: int = 64
    commitlog_sync_period_in_ms: int = 10000
    commitlog_sync_batch_window_in_ms: int = 10000
    commitlog_total_space_in_mb: int = -1
    commitlog_reuse_segments: bool = True
    commitlog_use_o_dsync: bool = True
    compaction_preheat_key_cache: bool = True
    concurrent_compactors: int = 0
    in_memory_compaction_limit_in_mb: int = 64
    preheat_kernel_page_cache: bool = False
    sstable_preemptive_open_interval_in_mb: int = 50
    defragment_memory_on_idle: bool = False
    memtable_allocation_type: Literal[
        'heap_buffers',
        'offheap_buffers',
        'offheap_objects'
    ] = "heap_buffers"
    memtable_cleanup_threshold: float = .11
    file_cache_size_in_mb: int = 512
    memtable_flush_queue_size: int = 4
    memtable_flush_writers: int = 1
    memtable_heap_space_in_mb: int = 0
    memtable_offheap_space_in_mb: int = 0
    column_index_size_in_kb: int = 64
    index_summary_capacity_in_mb: int = 0
    index_summary_resize_interval_in_minutes: int = 60
    reduce_cache_capacity_to: float = .6
    reduce_cache_sizes_at: float = .85
    stream_throughput_outbound_megabits_per_sec: int = 400
    inter_dc_stream_throughput_outbound_megabits_per_sec: int = 0
    trickle_fsync: bool = False
    trickle_fsync_interval_in_kb: int = 10240
    auto_bootstrap: bool = True
    batch_size_warn_threshold_in_kb: int = 5
    batch_size_fail_threshold_in_kb: int = 50
    listen_on_broadcast_address: bool = False
    initial_token: int = None
    num_tokens: int = 1
    partitioner: Literal['org.apache.cassandra.dht.Murmur3Partitioner'] = 'org.apache.cassandra.dht.Murmur3Partitioner'
    storage_port: int = 7000
    auto_snapshot: bool = True
    key_cache_keys_to_save: int = 0
    key_cache_save_period: int = 14400
    key_cache_size_in_mb: int = 100
    row_cache_keys_to_save: int = 0
    row_cache_size_in_mb: int = 0
    row_cache_save_period: int = 0
    memory_allocator: Literal[
        "NativeAllocator",
        "JEMallocAllocator"
    ] = "NativeAllocator"
    counter_cache_size_in_mb: int = 0
    counter_cache_save_period: int = 7200
    counter_cache_keys_to_save: int = 0
    tombstone_warn_threshold: int = 1000
    tombstone_failure_threshold: int = 100000
    range_request_timeout_in_ms: int = 10000
    read_request_timeout_in_ms: int = 5000
    counter_write_request_timeout_in_ms: int = 5000
    cas_contention_timeout_in_ms: int = 1000
    truncate_request_timeout_in_ms: int = 60000
    write_request_timeout_in_ms: int = 2000
    request_timeout_in_ms: int = 10000
    cross_node_timeout: bool = False
    internode_send_buff_size_in_bytes: int = 0
    internode_recv_buff_size_in_bytes: int = 0
    internode_compression: Literal['none', 'all', 'dc'] = "none"
    inter_dc_tcp_nodelay: bool = False
    streaming_socket_timeout_in_ms: int = 0
    start_native_transport: bool = True
    native_transport_port: int = 9042
    native_transport_port_ssl: int = 9142
    native_transport_max_threads: int = 128
    native_transport_max_frame_size_in_mb: int = 256
    native_shard_aware_transport_port: int = 19042
    native_shard_aware_transport_port_ssl: int = 19142
    broadcast_rpc_address: str = None
    rpc_port: int = 9160
    start_rpc: bool = True
    rpc_keepalive: bool = True
    rpc_max_threads: int = 0
    rpc_min_threads: int = 16
    rpc_recv_buff_size_in_bytes: int = 0
    rpc_send_buff_size_in_bytes: int = 0
    rpc_server_type: Literal['sync', 'hsh'] = "sync"
    cache_hit_rate_read_balancing: bool = True
    dynamic_snitch_badness_threshold: int = 0
    dynamic_snitch_reset_interval_in_ms: int = 60000
    dynamic_snitch_update_interval_in_ms: int = 100
    hinted_handoff_enabled: Literal[
        'enabled', 'true', 'True', '1', True,
        'disabled', 'false', 'False', '0', False,
    ] = 'enabled'
    hinted_handoff_throttle_in_kb: int = 1024
    max_hint_window_in_ms: int = 10800000
    max_hints_delivery_threads: int = 2
    batchlog_replay_throttle_in_kb: int = 1024
    request_scheduler: Literal[
        'org.apache.cassandra.scheduler.NoScheduler',
        'org.apache.cassandra.scheduler.RoundRobinScheduler'
    ] = "org.apache.cassandra.scheduler.NoScheduler"

    # pylint: disable=no-self-argument,no-self-use
    @validator("request_scheduler", pre=True, always=True)
    def set_request_scheduler(cls, request_scheduler: str):
        if request_scheduler.startswith('org.apache.cassandra.scheduler.'):
            return request_scheduler
        return 'org.apache.cassandra.scheduler.' + request_scheduler

    request_scheduler_id: str = None
    request_scheduler_options: RequestSchedulerOptions = None
    thrift_framed_transport_size_in_mb: int = 15
    thrift_max_message_length_in_mb: int = 16
    authenticator: Literal[
        "org.apache.cassandra.auth.PasswordAuthenticator",
        "org.apache.cassandra.auth.AllowAllAuthenticator",
        "com.scylladb.auth.TransitionalAuthenticator",
        "com.scylladb.auth.SaslauthdAuthenticator"
    ] = "org.apache.cassandra.auth.AllowAllAuthenticator"

    # pylint: disable=no-self-argument,no-self-use
    @validator("authenticator", pre=True, always=True)
    def set_authenticator(cls, authenticator: str):
        if authenticator.startswith('org.apache.cassandra.auth.') or authenticator.startswith('com.scylladb.auth.'):
            return authenticator
        if authenticator in ['PasswordAuthenticator', 'AllowAllAuthenticator']:
            return 'org.apache.cassandra.auth.' + authenticator
        if authenticator in ['TransitionalAuthenticator', 'SaslauthdAuthenticator']:
            return 'com.scylladb.auth.' + authenticator
        return authenticator

    internode_authenticator: Literal["enabled", "disabled"] = "disabled"
    authorizer: Literal[
        "org.apache.cassandra.auth.AllowAllAuthorizer",
        "org.apache.cassandra.auth.CassandraAuthorizer",
        "com.scylladb.auth.TransitionalAuthorizer",
        "com.scylladb.auth.SaslauthdAuthorizer"
    ] = "org.apache.cassandra.auth.AllowAllAuthorizer"

    # pylint: disable=no-self-argument,no-self-use
    @validator("authorizer", pre=True, always=True)
    def set_authorizer(cls, authorizer: str):
        if authorizer.startswith('org.apache.cassandra.auth.') or authorizer.startswith('com.scylladb.auth.'):
            return authorizer
        if authorizer in ['AllowAllAuthorizer', 'CassandraAuthorizer']:
            return 'org.apache.cassandra.auth.' + authorizer
        if authorizer in ['TransitionalAuthorizer', 'SaslauthdAuthorizer']:
            return 'com.scylladb.auth.' + authorizer
        return authorizer

    role_manager: str = "org.apache.cassandra.auth.CassandraRoleManager"
    permissions_validity_in_ms: int = 10000
    permissions_update_interval_in_ms: int = 2000
    permissions_cache_max_entries: int = 1000
    server_encryption_options: ServerEncryptionOptions = None
    client_encryption_options: ClientEncryptionOptions = None
    enable_in_memory_data_store: bool = False
    enable_cache: bool = True
    enable_commitlog: bool = True
    volatile_system_keyspace_for_testing: bool = False
    api_ui_dir: str = "swagger-ui/dist/"
    api_doc_dir: str = "api/api-doc/"
    load_balance: str = "none"
    consistent_rangemovement: bool = True
    join_ring: bool = True
    load_ring_state: bool = True
    replace_node: str = ""
    replace_token: str = ""
    replace_address: str = ""
    replace_address_first_boot: str = ""
    override_decommission: bool = False
    enable_repair_based_node_ops: bool = True
    ring_delay_ms: int = 30 * 1000
    shadow_round_ms: int = 300 * 1000
    fd_max_interval_ms: int = 2 * 1000
    fd_initial_value_ms: int = 2 * 1000
    shutdown_announce_in_ms: int = 2 * 1000
    developer_mode: bool = False
    skip_wait_for_gossip_to_settle: int = -1
    force_gossip_generation: int = -1
    experimental: bool = False
    experimental_features: dict = {}
    lsa_reclamation_step: int = 1
    prometheus_port: int = 9180
    prometheus_address: str = "localhost"
    prometheus_prefix: str = "scylla"
    abort_on_lsa_bad_alloc: bool = False
    murmur3_partitioner_ignore_msb_bits: int = 12
    virtual_dirty_soft_limit: float = 0.6
    sstable_summary_ratio: float = 0.0005
    large_memory_allocation_warning_threshold: int = 2 ** 20
    enable_deprecated_partitioners: bool = False
    enable_keyspace_column_family_metrics: bool = False
    enable_sstable_data_integrity_check: bool = False
    enable_sstable_key_validation: bool = None
    cpu_scheduler: bool = True
    view_building: bool = True
    enable_sstables_mc_format: bool = True
    enable_sstables_md_format: bool = False
    enable_dangerous_direct_import_of_cassandra_counters: bool = False
    enable_shard_aware_drivers: bool = True
    enable_ipv6_dns_lookup: bool = False
    abort_on_internal_error: bool = False
    max_partition_key_restrictions_per_query: int = 100
    max_clustering_key_restrictions_per_query: int = 100
    max_memory_for_unlimited_query: int = 1048576
    initial_sstable_loading_concurrency: str = '4u'
    enable_3_1_0_compatibility_mode: bool = False
    enable_user_defined_functions: bool = False
    user_defined_function_time_limit_ms: int = 10
    user_defined_function_allocation_limit_bytes: int = 1024 * 1024
    user_defined_function_contiguous_allocation_limit_bytes: int = 1024 * 1024
    alternator_port: int = 0
    alternator_https_port: int = 0
    alternator_address: str = "0.0.0.0"
    alternator_enforce_authorization: bool = False
    alternator_write_isolation: Literal["unsafe_rmw", "only_rmw_uses_lwt", "forbid_rmw", "always_use_lwt"] = None
    alternator_streams_time_window_s: int = 10
    abort_on_ebadf: bool = True
    redis_port: int = 0
    redis_ssl_port: int = 0
    redis_read_consistency_level: str = "LOCAL_QUORUM"
    redis_write_consistency_level: str = "LOCAL_QUORUM"
    redis_database_count: int = 16
    redis_keyspace_replication_strategy: Literal['SimpleStrategy', 'NetworkTopologyStrategy'] = 'SimpleStrategy'
    default_log_level: str = None
    logger_log_level: str = None
    log_to_stdout: bool = None
    log_to_syslog: bool = None
    authenticator_user: str = None
    authenticator_password: str = None
    workdir: str = None

    ldap_attr_role: str = None
    ldap_bind_dn: str = None
    ldap_bind_passwd: str = None
    ldap_url_template: str = None
    saslauthd_socket_path: str = None

    def dict(  # pylint: disable=arguments-differ
        self,
        *,
        include: Union['MappingIntStrAny', 'AbstractSetIntStr'] = None,
        exclude: Union['MappingIntStrAny', 'AbstractSetIntStr'] = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_unset: bool = False,
        explicit: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None,
    ) -> 'DictStrAny':
        to_dict = super().dict(
            include=include, exclude=exclude, by_alias=by_alias, skip_defaults=skip_defaults,
            exclude_unset=exclude_unset, exclude_defaults=exclude_defaults, exclude_none=exclude_none)
        if explicit:
            for required_attrs in explicit:
                to_dict[required_attrs] = getattr(self, required_attrs)
        return to_dict

    def _update_dict(self, obj: dict, fields_data: dict):
        for attr_name, attr_value in obj.items():
            attr_info = fields_data.get(attr_name, None)
            if attr_info is None:
                raise ValueError("Provided unknown attribute `%s`" % (attr_name,))
            if hasattr(attr_info.type_, "__attrs_attrs__"):
                if attr_value is not None:
                    if not isinstance(attr_value, dict):
                        raise ValueError("Unexpected data `%s` in attribute `%s`" % (
                            type(attr_value), attr_name))
                    attr_value = attr_info.type(**attr_value)
            setattr(self, attr_name, attr_value)

    def update(self, *objects: Union['ScyllaYaml', dict]):
        """
        Do the same as dict.update, with one exception.
        It ignores whatever key if it's value equal to default
        This comes from module `attr` and probably could be tackled there
        """
        fields_data = self.__fields__
        for obj in objects:
            if isinstance(obj, ScyllaYaml):
                for attr_name, attr_value in obj.__dict__.items():
                    attr_type = fields_data.get(attr_name)
                    if attr_value != attr_type.default and attr_value != getattr(self, attr_name, None):
                        setattr(self, attr_name, attr_value)
            elif isinstance(obj, dict):
                self._update_dict(obj, fields_data=fields_data)
            else:
                raise ValueError("Only dict or ScyllaYaml is accepted")
        return self

    def diff(self, other: 'ScyllaYaml') -> str:
        self_str = yaml.safe_dump(self.dict(
            exclude_defaults=True, exclude_unset=True, exclude_none=True)).splitlines(keepends=True)
        other_str = yaml.safe_dump(other.dict(
            exclude_defaults=True, exclude_unset=True, exclude_none=True)).splitlines(keepends=True)
        return "".join(unified_diff(self_str, other_str))

    def __copy__(self):
        return self.__class__(**self.dict(exclude_defaults=True, exclude_unset=True))

    copy = __copy__
