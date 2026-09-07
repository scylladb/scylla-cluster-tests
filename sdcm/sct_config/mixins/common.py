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

"""General and provisioning configuration options."""

from typing import ClassVar, Literal
from typing_extensions import Annotated

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from sdcm.sct_config.types import (
    AdaptiveTimeoutMultipliers,
    Boolean,
    DictOrStr,
    IntOrList,
    SctField,
    String,
    StringOrList,
    dict_or_str_or_pydantic,
)
from sdcm.test_metadata import TestMetadata


class CommonConfigMixin(BaseModel):
    """General and provisioning.

    Cluster topology, region/AZ placement, instance provisioning, credentials and test-level
    plumbing. Options here apply to every backend and every test type.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "General and provisioning"

    adaptive_timeout_multipliers: Annotated[AdaptiveTimeoutMultipliers, BeforeValidator(dict_or_str_or_pydantic)] = (
        SctField(
            description="Optional dict of adaptive-timeout multipliers keyed by operation name "
            "(from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). "
            "If the current operation key is absent, multiplier 1.0 is used.<br>"
            "YAML example:<br>"
            "adaptive_timeout_multipliers:<br>"
            "  decommission: 4<br>"
            "  new_node: 2<br>"
            "Environment variable examples:<br>"
            "SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS=\"{'decommission': 4, 'new_node': 2}\"<br>"
            "Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>"
            "Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4",
        )
    )
    adaptive_timeout_store_metrics: Boolean = SctField(
        description="Store adaptive timeout metrics in Argus. Disabled for performance tests only.",
    )
    add_node_cnt: int = SctField(
        description="The number of nodes to add during the test.",
    )
    agent: DictOrStr = SctField(
        description="""
            Configuration for SCT agent - a lightweight service for remote command execution.                 When enabled, replaces SSH-based command execution with RESTful API calls for DB nodes.
            Configuration options:
            - enabled: bool - enable agent (required)
            - port: int - agent HTTP API port (default: 16000)
            - binary_url: str - URL to download agent binary
            - max_concurrent_jobs: int - max concurrent jobs per agent (default: 10)
            - log_level: str - logging level (default: info)
            - tls: bool - enable TLS for agent communication (default: false)""",
    )
    availability_zone: String = SctField(
        description="""Availability zone to use. Specify multiple (comma separated) to deploy resources to multi az (works on AWS).
              "Same for multi-region scenario.""",
    )
    billing_project: String = SctField(
        description="""Billing project for the test run. Used for cost tracking and reporting""",
    )
    bisect_end_date: String = SctField(
        description="End date for bisecting test runs to find regressions",
    )
    bisect_start_date: String = SctField(
        description="Start date for bisecting test runs to find regressions",
    )
    cluster_backend: String = SctField(
        description="backend that will be used, aws/gce/azure/oci/docker/xcloud",
        appendable=False,
    )
    cluster_health_check: Boolean = SctField(
        description="Enable or disable starting cluster health checker for all nodes",
    )
    cluster_health_check_parallel_workers: int = SctField(
        description="Number of parallel workers for health checks. "
        "Values above 10 are not recommended (diminishing returns, risk of API rate limiting). "
        "Default: 5.",
    )
    config_files: StringOrList = SctField(
        description="a list of config files that would be used",
        appendable=False,
    )
    data_volume_disk_iops: int = SctField(
        description="Number of iops for ebs type io2|io3|gp3",
    )
    data_volume_disk_num: int = SctField(
        description="""Number of additional data volumes attached to instances
         if data_volume_disk_num > 0, then data volumes (ebs on aws) will be
         used for scylla data directory""",
    )
    data_volume_disk_size: int = SctField(
        description="Size of additional volume in GB",
    )
    data_volume_disk_throughput: int = SctField(
        description="Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.",
    )
    data_volume_disk_type: Literal[
        # AWS
        "gp2",
        "gp3",
        "io2",
        "io3",
        "",
        # OCI
        "lower_cost",
        "balanced",
        "higher_performance",
        "ultra",
    ] = SctField(
        description=(
            "Type of additional volumes. AWS: gp2|gp3|io2|io3. OCI: lower_cost|balanced|higher_performance|ultra"
        ),
    )
    db_nodes_shards_selection: Literal["default", "random"] = SctField(
        description="""How to select number of shards of Scylla. Expected values: default/random.
         Default value: 'default'.
         In case of random option - Scylla will start with different (random) shards on every node of the cluster
         """,
    )
    fallback_to_next_availability_zone: Boolean = SctField(
        description="On capacity errors, automatically retry provisioning in the next available AZ in the same region. "
        "Backend-agnostic parameter; supersedes `aws_fallback_to_next_availability_zone`.",
    )
    fallback_to_next_region: Boolean = SctField(
        description="On capacity errors, after all AZs/zones in the configured region are exhausted, relocate to the next "
        "eligible region: a single-region cluster moves as a whole, while in a multi-region test only the exhausted "
        "datacenter is relocated (to a region no other datacenter occupies) and the cluster is retried. On AWS the target "
        "region should be VPC-peered with the runner region with infra-prepared and AMI available; on GCE the global VPC "
        "and global images make any supported region eligible. Only applies during initial setup. "
        "Supported backends: AWS, GCE.",
    )
    force_run_iotune: Boolean = SctField(
        description="Force running iotune on the DB nodes, regardless if image has predefined values",
    )
    instance_provision: Literal["spot", "on_demand", "spot_fleet", "spot_low_price"] = SctField(
        description="instance_provision: spot|on_demand|spot_fleet",
    )
    instance_provision_fallback_on_demand: Boolean = SctField(
        description="instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected "
        "'instance_provision' type creation failed. "
        "Expected values: true|false (default - false",
    )
    instance_type_db: String = SctField(
        description="AWS image type of the db node",
    )
    instance_type_db_oracle: String = SctField(
        description="AWS image type of the oracle node",
    )
    instance_type_db_target: String = SctField(
        description="Target AWS instance type for platform migration (e.g., i8g.2xlarge for ARM)",
    )
    instance_type_loader: String = SctField(
        description="AWS image type of the loader node",
    )
    instance_type_monitor: String = SctField(
        description="AWS image type of the monitor node",
    )
    instance_type_runner: String = SctField(
        description="instance type of the sct-runner node",
    )
    instance_type_vector_store: String = SctField(
        description="AWS/GCP cloud provider instance type for Vector Store nodes",
    )
    intra_node_comm_public: Boolean = SctField(
        description="If True, all communication between nodes are via public addresses",
    )
    ip_ssh_connections: Literal["public", "private", "ipv6"] = SctField(
        description="""
            Type of IP used to connect to machine instances.
            This depends on whether you are running your tests from a machine inside
            your cloud provider, where it makes sense to use 'private', or outside (use 'public')

            Default: Use public IPs to connect to instances (public)
            Use private IPs to connect to instances (private)
            Use IPv6 IPs to connect to instances (ipv6)
         """,
    )
    keystore_backend: Literal["s3", "secretsmanager"] = SctField(
        description="Credential storage backend for KeyStore: 'secretsmanager' (default) or 's3' (legacy)",
    )
    keystore_sm_prefix: String = SctField(
        description="AWS Secrets Manager secret name prefix when keystore_backend=secretsmanager (default: 'sct/')",
    )
    keystore_sm_region: String = SctField(
        description="AWS region holding the KeyStore secrets when keystore_backend=secretsmanager (default: 'us-east-1')",
    )
    latency_decorator_error_thresholds: DictOrStr = SctField(
        description="Error thresholds for latency decorator. "
        "Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}",
    )
    n_db_nodes: IntOrList = SctField(
        description="Number list of database nodes in multiple data centers.",
    )
    n_db_zero_token_nodes: IntOrList = SctField(
        description="Number of zero token nodes in cluster. Value should be set as '0 1 1' "
        "for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions",
    )
    n_loaders: IntOrList = SctField(
        description="Number list of loader nodes in multiple data centers",
    )
    n_monitor_nodes: IntOrList = SctField(
        description="Number list of monitor nodes in multiple data centers",
    )
    parallel_node_operations: Boolean = SctField(
        description="When defined true, will run node operations in parallel. Supported operations: startup",
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
    raid_level: int = SctField(
        description="Number of of raid level: 0 - RAID0, 5 - RAID5",
    )
    region_name: StringOrList = SctField(
        description="Cloud region(s) to run in. A space-separated list or YAML list provisions a multi-region cluster, one entry per datacenter. Despite the AWS-sounding default, this is the generic region option; GCE uses 'gce_datacenter' and Azure uses 'azure_region_name'.",
        appendable=False,
    )
    reuse_cluster: String = SctField(
        description="""
        If reuse_cluster is set it should hold test_id of the cluster that will be reused.
        `reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`
        """,
    )
    sct_aws_account_id: String = SctField(
        description="AWS account id on behalf of which the test is run",
    )
    sct_public_ip: String = SctField(
        description="""
            Override the default hostname address of the sct test runner,
            for the monitoring of the Nemesis.
            can only work out of the box in AWS
        """,
    )
    seeds_num: int = SctField(
        description="""Number of seeds to select""",
    )
    seeds_selector: Literal["random", "first", "all"] = SctField(
        description="""How to select the seeds. Expected values: random/first/all""",
    )
    simulated_racks: int = SctField(
        description="""Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.
         Provide number of racks to simulate. Takes effect only with more than one DB node: a
         single-node cluster stays in one rack and `endpoint_snitch` is left alone. On the docker
         backend the rack is passed to the image entrypoint as `--dc/--rack`, which requires Scylla
         >= 2026.1; an older image fails the configuration, so set 1 to opt out.""",
    )
    simulated_regions: Literal[0, 2, 3, 4, 5] = SctField(
        description="Number of simulated regions for the test",
    )
    sizing_db: dict | None = SctField(
        default=None, description="Cloud-agnostic instance sizing constraints for db nodes"
    )
    sizing_db_oracle: dict | None = SctField(
        default=None, description="Cloud-agnostic instance sizing constraints for db_oracle nodes"
    )
    sizing_loader: dict | None = SctField(
        default=None, description="Cloud-agnostic instance sizing constraints for loader nodes"
    )
    sizing_monitor: dict | None = SctField(
        default=None, description="Cloud-agnostic instance sizing constraints for monitor nodes"
    )
    skip_test_stages: DictOrStr = SctField(
        description="Skip selected stages of a test scenario",
    )
    ssh_transport: Literal["libssh2", "fabric"] = SctField(
        description="""Set type of ssh library to use. Could be 'libssh2' (default) or 'fabric'""",
        default="libssh2",
    )
    test_duration: int = SctField(
        description="""
              Test duration (min). Parameter used to keep instances produced by tests
              and for jenkins pipeline timeout and TimoutThread.
        """,
    )
    test_id: String = SctField(
        description="""Set the test_id of the run manually. Use only from the env before running Hydra""",
    )
    test_metadata: Annotated[TestMetadata | None, BeforeValidator(dict_or_str_or_pydantic)] = SctField(
        description=(
            "Structured metadata for test documentation and labeling. Validated by pydantic model. Flows to Argus."
        ),
    )
    test_method: String = SctField(
        description="class.method used to run the test. Filled automatically with run-test sct command.",
        appendable=False,
    )
    use_dns_names: Boolean = SctField(
        description="""Use dns names instead of ip addresses for nodes in cluster""",
    )
    use_legacy_cluster_init: Boolean = SctField(
        description="""Use legacy cluster initialization with autobootsrap disabled and parallel node setup""",
    )
    use_zero_nodes: Boolean = SctField(
        description="If True, enable support in SCT of zero nodes (configuration, nemesis)",
    )
    user_credentials_path: str = SctField(
        description="Path to the SSH private key SCT uses to reach the nodes it provisions. The QA key is fetched automatically from the KeyStore, so this rarely needs setting by hand.",
    )
    user_prefix: String = SctField(
        description="the prefix of the name of the cloud instances, defaults to username",
    )
    workload_name: String = SctField(
        description="Workload name, can be: write|read|mixed|unset. "
        "Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). "
        "If unset, workload is taken from test name.",
    )
    zero_token_instance_type_db: String = SctField(
        description="Instance type for zero-token DB nodes -- nodes that join the ring for reads/writes but own no token range. Falls back to 'instance_type_db' when unset.",
    )
