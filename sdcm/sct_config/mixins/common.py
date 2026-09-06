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

from sdcm.sct_config.types import Boolean, IntOrList, SctField, String, StringOrList, dict_or_str_or_pydantic
from sdcm.test_metadata import TestMetadata


class CommonConfigMixin(BaseModel):
    """General and provisioning configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "General and provisioning"

    data_volume_disk_throughput: int = SctField(
        description="Throughput in MiB/sec for ebs type gp3. Min is 125. Max is 1000.",
    )
    config_files: StringOrList = SctField(
        description="a list of config files that would be used",
        appendable=False,
    )
    cluster_backend: String = SctField(
        description="backend that will be used, aws/gce/azure/oci/docker/xcloud",
        appendable=False,
    )
    test_method: String = SctField(
        description="class.method used to run the test. Filled automatically with run-test sct command.",
        appendable=False,
    )
    test_duration: int = SctField(
        description="""
              Test duration (min). Parameter used to keep instances produced by tests
              and for jenkins pipeline timeout and TimoutThread.
        """,
    )
    n_db_nodes: IntOrList = SctField(
        description="Number list of database nodes in multiple data centers.",
    )
    n_test_oracle_db_nodes: IntOrList = SctField(
        description="Number list of oracle test nodes in multiple data centers.",
    )
    n_loaders: IntOrList = SctField(
        description="Number list of loader nodes in multiple data centers",
    )
    n_monitor_nodes: IntOrList = SctField(
        description="Number list of monitor nodes in multiple data centers",
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
    parallel_node_operations: Boolean = SctField(
        description="When defined true, will run node operations in parallel. Supported operations: startup",
    )
    user_prefix: String = SctField(
        description="the prefix of the name of the cloud instances, defaults to username",
    )
    sct_public_ip: String = SctField(
        description="""
            Override the default hostname address of the sct test runner,
            for the monitoring of the Nemesis.
            can only work out of the box in AWS
        """,
    )
    reuse_cluster: String = SctField(
        description="""
        If reuse_cluster is set it should hold test_id of the cluster that will be reused.
        `reuse_cluster: 7dc6db84-eb01-4b61-a946-b5c72e0f6d71`
        """,
    )
    test_id: String = SctField(
        description="""Set the test_id of the run manually. Use only from the env before running Hydra""",
    )
    billing_project: String = SctField(
        description="""Billing project for the test run. Used for cost tracking and reporting""",
    )
    db_nodes_shards_selection: Literal["default", "random"] = SctField(
        description="""How to select number of shards of Scylla. Expected values: default/random.
         Default value: 'default'.
         In case of random option - Scylla will start with different (random) shards on every node of the cluster
         """,
    )
    seeds_selector: Literal["random", "first", "all"] = SctField(
        description="""How to select the seeds. Expected values: random/first/all""",
    )
    seeds_num: int = SctField(
        description="""Number of seeds to select""",
    )
    ssh_transport: Literal["libssh2", "fabric"] = SctField(
        description="""Set type of ssh library to use. Could be 'libssh2' (default) or 'fabric'""",
        default="libssh2",
    )
    data_volume_disk_num: int = SctField(
        description="""Number of additional data volumes attached to instances
         if data_volume_disk_num > 0, then data volumes (ebs on aws) will be
         used for scylla data directory""",
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
    data_volume_disk_size: int = SctField(
        description="Size of additional volume in GB",
    )
    data_volume_disk_iops: int = SctField(
        description="Number of iops for ebs type io2|io3|gp3",
    )
    raid_level: int = SctField(
        description="Number of of raid level: 0 - RAID0, 5 - RAID5",
    )
    simulated_regions: Literal[0, 2, 3, 4, 5] = SctField(
        description="Number of simulated regions for the test",
    )
    simulated_racks: int = SctField(
        description="""Forces GossipingPropertyFileSnitch (regardless `endpoint_snitch`) to simulate racks.
         Provide number of racks to simulate. Takes effect only with more than one DB node: a
         single-node cluster stays in one rack and `endpoint_snitch` is left alone. On the docker
         backend the rack is passed to the image entrypoint as `--dc/--rack`, which requires Scylla
         >= 2026.1; an older image fails the configuration, so set 1 to opt out.""",
    )
    rack_aware_loader: Boolean = SctField(
        description="When enabled, loaders will look for nodes on the same rack.",
    )
    use_dns_names: Boolean = SctField(
        description="""Use dns names instead of ip addresses for nodes in cluster""",
    )
    test_metadata: Annotated[TestMetadata | None, BeforeValidator(dict_or_str_or_pydantic)] = SctField(
        description=(
            "Structured metadata for test documentation and labeling. Validated by pydantic model. Flows to Argus."
        ),
    )
