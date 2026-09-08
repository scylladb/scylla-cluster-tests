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

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class AwsConfigMixin(BaseModel):
    """AWS backend.

    AWS-specific provisioning: AMIs, EC2 instance and disk settings, placement groups, capacity
    reservations and dedicated hosts.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "AWS backend"

    ami_db_cassandra_user: String = SctField(
        description="SSH login user baked into the Cassandra AMI, for the auxiliary cluster.",
    )
    ami_db_scylla_user: String = SctField(
        description="SSH login user baked into the DB node AMI (e.g. 'centos', 'ubuntu').",
    )
    ami_id_db_cassandra: String = SctField(
        description="AMS AMI id to use for cassandra node",
    )
    ami_id_db_oracle: String = SctField(
        description="AMS AMI id to use for oracle node",
    )
    ami_id_db_scylla: String = SctField(
        description="AMS AMI id to use for scylla db node",
    )
    ami_id_db_scylla_desc: String = SctField(
        description="version name to report stats to Elasticsearch and tagged on cloud instances",
    )
    ami_id_loader: String = SctField(
        description="AMS AMI id to use for loader node",
    )
    ami_id_monitor: String = SctField(
        description="AMS AMI id to use for monitor node",
    )
    ami_id_vector_store: String = SctField(
        description="AMS AMI id to use for vector store node",
    )
    ami_loader_user: String = SctField(
        description="SSH login user baked into the loader AMI.",
    )
    ami_monitor_user: String = SctField(
        description="SSH login user baked into the monitoring node AMI.",
    )
    ami_vector_store_user: String = SctField(
        description="SSH login user baked into the Vector Store AMI.",
    )
    aws_dedicated_host_ids: StringOrList = SctField(
        description="List of host ids to use, relevant only if `use_dedicated_host: true` (AWS only)",
    )
    aws_fallback_to_next_availability_zone: Boolean = SctField(
        description="Deprecated alias of `fallback_to_next_availability_zone`. Kept for backward compatibility.",
    )
    aws_instance_profile_name_db: String = SctField(
        description="This is the name of the instance profile to set on all db instances",
    )
    aws_instance_profile_name_loader: String = SctField(
        description="This is the name of the instance profile to set on all loader instances",
    )
    extra_network_interface: Boolean = SctField(
        description="if true, create extra network interface on each node",
    )
    sct_aws_account_id: String = SctField(
        description="AWS account id on behalf of which the test is run",
    )
    spot_max_price: float = SctField(
        description="The max percentage of the on demand price we set for spot/fleet instances",
    )
    use_capacity_reservation: Boolean = SctField(
        description="Flag to use capacity reservation for instances",
    )
    use_dedicated_host: Boolean = SctField(
        description="Flag to allocate dedicated hosts for the instances for the entire duration of the test run (AWS only)",
    )
    use_placement_group: Boolean = SctField(
        description="if true, create 'cluster' placement group for test case "
        "for low-latency network performance achievement",
    )
