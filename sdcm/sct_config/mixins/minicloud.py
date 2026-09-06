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
# Copyright (c) 2020 ScyllaDB

"""Minicloud backend configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class MinicloudConfigMixin(BaseModel):
    """Minicloud backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Minicloud backend"

    minicloud_endpoint_url: String = SctField(
        description="EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud "
        "limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000",
        appendable=False,
    )
    minicloud_docker_image: String = SctField(
        description="Explicit minicloud image override. Empty means the renovate-managed "
        "default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)",
    )
    minicloud_lightweight: Boolean = SctField(
        description="Enable lightweight mode for minicloud deployments",
    )
    minicloud_lightweight_memory: String = SctField(
        description="Memory allocation for lightweight minicloud deployments",
    )
    minicloud_lightweight_vcpus: int = SctField(
        description="vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this "
        "multiplies with minicloud_lightweight_memory across every guest in the test — raise it "
        "only on a host with cores to spare",
    )
    minicloud_container_memory: String = SctField(
        description="Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker "
        "limit, so the container can consume the whole host. Setting it also makes this, rather "
        "than the host's free memory, the budget the preflight guest-memory gate measures against",
    )
    minicloud_container_cpus: String = SctField(
        description="Cap the minicloud container's CPU allowance, in docker --cpus form "
        "(e.g. '8' or '7.5'). Empty means no limit",
    )
    minicloud_state_dir: String = SctField(
        description="Where minicloud keeps its image cache, per-instance disks and minicloud.log — "
        "tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace",
    )
    minicloud_container_name: String = SctField(
        description="Name of the minicloud docker container. Change it to run two emulators on one "
        "host — a second run under the same name force-removes the first one's container",
    )
    minicloud_keep_alive: Boolean = SctField(
        description="Leave the minicloud container running after the test instead of tearing it down "
        "(CI sets this so separate provision/test/collect/clean stages reach the same container)",
    )
    minicloud_skip_memory_check: Boolean = SctField(
        description="Skip the conservative host-memory preflight gate — for development machines "
        "whose owner knows the workload's real footprint; an oversized test then dies mid-run as "
        "a container OOM kill (exit 137)",
    )
    minicloud_s3_passthrough_buckets: StringOrList = SctField(
        description="S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). "
        "Backend-independent: GCE runs reach S3 for the same content",
    )
    minicloud_regions: StringOrList = SctField(
        description="Narrow the AWS regions minicloud prepares (default: every SCT-supported region; "
        "each costs ~2s at start-up)",
    )
    minicloud_gcs_bucket: String = SctField(
        description="GCS bucket for minicloud GCE image staging. Empty means derive "
        "<project>-minicloud-staging and create it on demand",
    )
    logs_transport: Literal["ssh", "docker", "syslog-ng", "vector"] = SctField(
        description="How to transport logs: syslog-ng, ssh or docker",
    )
    collect_logs: Boolean = SctField(
        description="Collect logs from instances and sct runner",
    )
    collect_nvme_diagnostics: Boolean = SctField(
        description="Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. "
        "Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.",
    )
    nvme_self_test_type: int = SctField(
        description="NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). "
        "Only used when collect_nvme_diagnostics is enabled.",
    )
    use_scylla_doctor_on_failure: Boolean = SctField(
        description="Run scylla-doctor on test failure to collect additional diagnostics",
    )
    execute_post_behavior: Boolean = SctField(
        description="Run post behavior actions in sct teardown step",
    )
    post_behavior_db_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_loader_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_monitor_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.
         """,
    )
    post_behavior_k8s_cluster: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.

        'destroy' - Destroy k8s cluster and credentials (default)
        'keep' - Keep k8s cluster running and leave credentials alone
        'keep-on-failure' - Keep k8s cluster if testrun failed
        """,
    )
    post_behavior_vector_store_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.

        'destroy' - Destroy instances and credentials (default)
        'keep' - Keep instances running and leave credentials alone
        'keep-on-failure' - Keep instances if testrun failed
        """,
    )
    post_behavior_emr_cluster: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.

        'destroy' - Destroy EMR cluster (default)
        'keep' - Keep EMR cluster running
        'keep-on-failure' - Keep EMR cluster if testrun failed
        """,
    )
    internode_compression: String = SctField(description="Scylla option: internode_compression.")
    internode_encryption: String = SctField(
        description="Scylla sub option of server_encryption_options: internode_encryption.",
    )
    jmx_heap_memory: int = SctField(
        description="The total size of the memory allocated to JMX. Values in MB, so for 1GB enter 1024(MB).",
    )
    loader_swap_size: int = SctField(
        description="The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB",
    )
    monitor_swap_size: int = SctField(
        description="The size of the swap file for the monitors. Its size in bytes calculated by x * 1MB",
    )
    append_scylla_setup_args: String = SctField(
        description="More arguments to append to scylla_setup command line",
    )
    use_preinstalled_scylla: Boolean = SctField(
        description="Don't install/update ScyllaDB on DB nodes",
    )
    stress_cdclog_reader_cmd: String = SctField(
        description="""cdc-stressor command to read cdc_log table.
                       You can specify everything but the -node, -keyspace, -table parameter, which is going to
                       be provided by the test suite infrastructure.
                       Multiple commands can be passed as a list.""",
    )
    store_cdclog_reader_stats_in_es: Boolean = SctField(
        description="Add cdclog reader stats to ES for future performance result calculating",
    )
    stop_test_on_stress_failure: Boolean = SctField(
        description="""If set to True the test will be stopped immediately when stress command failed.
                       When set to False the test will continue to run even when there are errors in the
                       stress process""",
    )
    stress_cdc_log_reader_batching_enable: Boolean = SctField(
        description="""retrieving data from multiple streams in one poll""",
    )
    use_legacy_cluster_init: Boolean = SctField(
        description="""Use legacy cluster initialization with autobootsrap disabled and parallel node setup""",
    )
    availability_zone: String = SctField(
        description="""Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).
              "Same for multi-region scenario.""",
    )
    aws_fallback_to_next_availability_zone: Boolean = SctField(
        description="Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.",
    )
    fallback_to_next_availability_zone: Boolean = SctField(
        description="On capacity errors, automatically retry provisioning in the next available AZ in the same region. "
        "Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.",
    )
    pre_filter_unavailable_availability_zones: Boolean = SctField(
        description="Filter availability zones upfront to only those that support all required instance types. "
        "Replaces invalid AZs with valid alternatives in the same region before any provisioning attempt. "
        "Supported backends: AWS, GCE.",
    )
    pre_flight_capacity_probe: Boolean = SctField(
        description="Before provisioning, probe capacity by launching and terminating one on-demand instance per dynamic type "
        "(`instance_type_db_target`, `nemesis_grow_shrink_instance_type`) in the chosen AZ. On capacity errors, raise to "
        "trigger AZ/region fallback. Costs ~1 min per type. AWS-only.",
    )
    fallback_to_next_region: Boolean = SctField(
        description="On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next "
        "eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted "
        "datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target "
        "region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC "
        "and global images make any supported region eligible. Only applies during initial setup. "
        "Supported backends: AWS, GCE.",
    )
    num_nodes_to_rollback: int = SctField(
        description="Number of nodes to upgrade and rollback in test_generic_cluster_upgrade",
    )
    upgrade_sstables: Boolean = SctField(
        description="Whether to upgrade sstables as part of upgrade_node or not",
    )
    enable_truncate_checks_on_node_upgrade: Boolean = SctField(
        description="Enables or disables truncate checks on each node upgrade and rollback",
    )
    stress_before_upgrade: StringOrList = SctField(
        description="Stress command to be run before upgrade starts (preload/validation stage). "
        "This workload runs before any nodes are upgraded and can use CL=ALL for data validation.",
    )
    large_partition_stress_during_upgrade: StringOrList = SctField(
        description="Stress command to be run during rolling upgrade while nodes are being upgraded. "
        "This workload cannot use CL=ALL as not all nodes may be available during the upgrade.",
    )
    stress_during_entire_upgrade: StringOrList = SctField(
        description="Stress command to be run during the upgrade - user should take care for suitable duration",
    )
    stress_after_cluster_upgrade: StringOrList = SctField(
        description="Stress command to be run after full upgrade - usually used to read the dataset for verification",
    )
