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

"""AWS backend configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.kafka.kafka_config import SctKafkaConfiguration
from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String, StringOrList


class AwsConfigMixin(BaseModel):
    """AWS backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "AWS backend"

    ami_id_db_scylla_desc: String = SctField(
        description="version name to report stats to Elasticsearch and tagged on cloud instances",
    )
    instance_provision: Literal["spot", "on_demand", "spot_fleet", "spot_low_price"] = SctField(
        description="instance_provision: spot|on_demand|spot_fleet",
    )
    instance_provision_fallback_on_demand: Boolean = SctField(
        description="instance_provision_fallback_on_demand: create instance on_demand provision type if instance with selected "
        "'instance_provision' type creation failed. "
        "Expected values: true|false (default - false",
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
    instance_type_loader: String = SctField(
        description="AWS image type of the loader node",
    )
    instance_type_monitor: String = SctField(
        description="AWS image type of the monitor node",
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
    instance_type_runner: String = SctField(
        description="instance type of the sct-runner node",
    )
    region_name: StringOrList = SctField(
        description="AWS regions to use",
        appendable=False,
    )
    use_placement_group: Boolean = SctField(
        description="if true, create 'cluster' placement group for test case "
        "for low-latency network performance achievement",
    )
    ami_id_db_scylla: String = SctField(
        description="AMS AMI id to use for scylla db node",
    )
    ami_id_loader: String = SctField(
        description="AMS AMI id to use for loader node",
    )
    ami_id_monitor: String = SctField(
        description="AMS AMI id to use for monitor node",
    )
    ami_id_db_cassandra: String = SctField(
        description="AMS AMI id to use for cassandra node",
    )
    ami_id_db_oracle: String = SctField(
        description="AMS AMI id to use for oracle node",
    )
    ami_id_vector_store: String = SctField(
        description="AMS AMI id to use for vector store node",
    )
    instance_type_vector_store: String = SctField(
        description="AWS/GCP cloud provider instance type for Vector Store nodes",
    )
    root_disk_size_db: int = SctField(
        description="",
    )
    root_disk_size_monitor: int = SctField(
        description="",
    )
    root_disk_size_loader: int = SctField(
        description="",
    )
    root_disk_size_runner: int = SctField(
        description="root disk size in Gb for sct-runner",
    )
    ami_db_scylla_user: String = SctField(
        description="",
    )
    ami_monitor_user: String = SctField(
        description="",
    )
    ami_loader_user: String = SctField(
        description="",
    )
    ami_db_cassandra_user: String = SctField(
        description="",
    )
    ami_vector_store_user: String = SctField(
        description="",
    )
    spot_max_price: float = SctField(
        description="The max percentage of the on demand price we set for spot/fleet instances",
    )
    extra_network_interface: Boolean = SctField(
        description="if true, create extra network interface on each node",
    )
    aws_instance_profile_name_db: String = SctField(
        description="This is the name of the instance profile to set on all db instances",
    )
    aws_instance_profile_name_loader: String = SctField(
        description="This is the name of the instance profile to set on all loader instances",
    )
    backup_bucket_backend: String = SctField(
        description="the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')",
    )
    backup_bucket_location: StringOrList = SctField(
        description="the bucket name to be used for backup (e.g., 'manager-backup-tests')",
    )
    backup_bucket_region: String = SctField(
        description="the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')",
    )
    use_prepared_loaders: Boolean = SctField(
        description="If True, we use prepared VMs for loader (instead of using docker images)",
    )
    scylla_d_overrides_files: StringOrList = SctField(
        description="list of files that should upload to /etc/scylla.d/ directory to override scylla config files",
        appendable=True,
    )
    gce_project: String = SctField(
        description="gcp project name to use",
    )
    gce_datacenter: StringOrList = SctField(
        description="Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region "
        "(e.g., us-east1) means the zone will be selected automatically, or you can mention the zone "
        "explicitly (e.g., us-east1-b)",
        appendable=False,
    )
    gce_network: String = SctField(
        description="gce network to use",
    )
    gce_image_db: String = SctField(
        description="gce image to use for db nodes",
    )
    gce_image_db_oracle: String = SctField(
        description="GCE image to use for oracle (2nd ref cluster) DB node(s). "
        "If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.",
    )
    gce_image_monitor: String = SctField(
        description="gce image to use for monitor nodes",
    )
    scylla_network_config: list = SctField(
        description="""Configure Scylla networking with single or multiple NIC/IP combinations.
              It must be defined for listen_address and rpc_address. For each address mandatory parameters are:
              - address: listen_address/rpc_address/broadcast_rpc_address/broadcast_address/test_communication
              - ip_type: ipv4 or ipv6
              - public: false or true
              - nic: number of NIC. 0, 1
              Supported for AWS and GCE meanwhile""",
    )
    gce_image_loader: String = SctField(
        description="Google Compute Engine image to use for loader nodes",
    )
    gce_image_username: String = SctField(
        description="Username for the Google Compute Engine image",
    )
    gce_instance_type_loader: String = SctField(
        description="Instance type for loader nodes in Google Compute Engine",
    )
    gce_root_disk_type_loader: String = SctField(
        description="Root disk type for loader nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_loader: int = SctField(
        description="Number of local SSD disks for loader nodes in Google Compute Engine",
    )
    gce_instance_type_monitor: String = SctField(
        description="Instance type for monitor nodes in Google Compute Engine",
    )
    gce_root_disk_type_monitor: String = SctField(
        description="Root disk type for monitor nodes in Google Compute Engine",
    )
    validate_large_collections: Boolean = SctField(
        description="Flag to validate large collections in the database",
    )
    run_commit_log_check_thread: Boolean = SctField(
        description="Flag to run a thread that checks commit logs",
    )
    teardown_validators: DictOrStr = SctField(
        description="Validators to use during teardown phase",
    )
    use_capacity_reservation: Boolean = SctField(
        description="Flag to use capacity reservation for instances",
    )
    use_dedicated_host: Boolean = SctField(
        description="Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)",
    )
    aws_dedicated_host_ids: StringOrList = SctField(
        description="List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)",
    )
    post_behavior_dedicated_host: Literal["keep", "destroy"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.

        'destroy' - Destroy hosts (default)
        'keep' - Keep hosts allocated
        """,
    )
    bisect_start_date: String = SctField(
        description="Start date for bisecting test runs to find regressions",
    )
    bisect_end_date: String = SctField(
        description="End date for bisecting test runs to find regressions",
    )
    kafka_backend: Literal["localstack", "vm", "msk"] | None = SctField(
        description="Type of Kafka backend to use",
    )
    kafka_connectors: list[SctKafkaConfiguration] = SctField(
        description="Kafka connectors to use",
    )
