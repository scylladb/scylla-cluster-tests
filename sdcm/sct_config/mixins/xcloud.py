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

"""Scylla Cloud (xcloud) backend configuration options."""

from typing import ClassVar
from typing_extensions import Annotated

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from sdcm.sct_config.types import SctField, String, dict_or_str


class XcloudConfigMixin(BaseModel):
    """Scylla Cloud (xcloud) backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Scylla Cloud (xcloud) backend"

    cloud_credentials_path: String = SctField(
        description="""Path to your user credentials. qa key are downloaded automatically from S3 bucket""",
    )
    cloud_cluster_id: int = SctField(
        description="""scylla cloud cluster id""",
    )
    cloud_prom_bearer_token: String = SctField(
        description="""scylla cloud promproxy bearer_token to federate monitoring data into our monitoring instance""",
    )
    cloud_prom_path: String = SctField(
        description="""scylla cloud promproxy path to federate monitoring data into our monitoring instance""",
    )
    cloud_prom_host: String = SctField(
        description="""scylla cloud promproxy hostname to federate monitoring data into our monitoring instance""",
    )
    xcloud_credentials_path: String = SctField(
        description="Path to Scylla Cloud credentials file, if stored locally",
    )
    xcloud_env: String = SctField(
        description="Scylla Cloud environment (e.g., lab).",
    )
    xcloud_provider: String = SctField(
        description="Cloud provider for Scylla Cloud deployment (aws, gce)",
    )
    xcloud_replication_factor: int = SctField(
        description="Replication factor for Scylla Cloud cluster",
    )
    xcloud_availability_zones: String = SctField(
        description="""Comma-separated availability zones for Scylla Cloud DB placement.
         AWS values are AZ IDs (e.g., 'use1-az1,use1-az2,use1-az3'); GCE values are zone names
         (e.g., 'us-east1-b,us-east1-c'). When set, SCT sends 'availabilityZoneIdsOverride' and forces placement.
         Provide one zone per DB node, or provide a shorter list to cycle round-robin (node count must divide evenly).
         Repeat the same zone to keep all nodes in one AZ. Leave empty (default) to let Scylla Cloud choose placement
         (multi-AZ spread). Cannot be used with 'xcloud_scaling_config'.""",
    )
    xcloud_vpc_peering: Annotated[dict, BeforeValidator(dict_or_str)] = SctField(
        description="""Dictionary of VPC peering parameters for private connectivity between
         SCT infrastructure and Scylla Cloud. The following parameters are used:
         enabled: bool - indicates whether VPC peering is to be used
         cidr_pool_base: str - base of CIDR pool to use for cluster private networks ('172.31.0.0/16' by default)
         cidr_subnet_size: int - size of subnet to use for cluster private network (24 by default)""",
    )
    xcloud_scaling_config: Annotated[dict | None, BeforeValidator(dict_or_str)] = SctField(
        description="""Scaling policy configuration. The payload should follow the following structure:

        {
            "InstanceFamilies": ["i8g"],
            "Mode": "xcloud",
            "Policies": {
                "Storage": {"Min": 0, "TargetUtilization": 0.8},
                "VCPU": {"Min": 0}
            }
        }

        - InstanceFamilies(list): instance families to use for scaling (e.g., ["i4i", "i8g"])
        - Mode(str): scaling mode, always "xcloud"
        - Policies(dict): scaling policies with the following keys:
            - Storage(dict):
                - Min(int): minimum storage in TB to maintain
                - TargetUtilization(float): target storage utilization from 0.7 to 0.9 with 0.05 step
            - VCPU(dict):
                - Min(int): minimum number of virtual CPUs to maintain

        For more details, see `scaling` parameter description in Cloud REST API documentation:
        https://cloud.docs.scylladb.com/stable/api.html#tag/Cluster/operation/createCluster""",
    )
