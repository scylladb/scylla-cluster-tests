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

"""Baremetal backend configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String, StringOrList


class BaremetalConfigMixin(BaseModel):
    """Baremetal backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Baremetal backend"

    s3_baremetal_config: String = SctField(
        description="Configuration for S3 in baremetal setups. This includes details such as endpoint URL, access key, secret key, and bucket name.",
    )
    db_nodes_private_ip: StringOrList = SctField(
        description="Private IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    db_nodes_public_ip: StringOrList = SctField(
        description="Public IP addresses of DB nodes. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    loaders_private_ip: StringOrList = SctField(
        description="Private IP addresses of loader nodes. Loaders are used for running stress tests or other workloads against the DB. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    loaders_public_ip: StringOrList = SctField(
        description="Public IP addresses of loader nodes. These IPs are used for accessing the loaders from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    monitor_nodes_private_ip: StringOrList = SctField(
        description="Private IP addresses of monitor nodes. Monitoring nodes host monitoring tools like Prometheus and Grafana for DB performance monitoring. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
    monitor_nodes_public_ip: StringOrList = SctField(
        description="Public IP addresses of monitor nodes. These IPs are used for accessing the monitoring tools from outside the private network. Can be a single IP, a list of IPs, or an expression that evaluates to a list.",
    )
