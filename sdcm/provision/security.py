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
# Copyright (c) 2022 ScyllaDB

from enum import Enum


class ScyllaOpenPorts(Enum):
    SSH = 22
    GRAFANA = 3000
    CQL = 9042
    CQL_SSL = 9142
    CQL_SHARD_AWARE = 19042
    NODE_EXPORTER = 9100
    ALTERNATOR = 8080
    PROMETHEUS = 9090
    RPC = 7000
    RPC_SSL = 7001
    JMX_MGMT = 7199
    REST_API = 10001  # why? scylla docs say 10000
    PROMETHEUS_API = 9180
    MANAGER_AGENT = 56090
    MANAGER_HTTP = 5080  # Scylla Manager HTTP API
    MANAGER_HTTPS = 5443  # Scylla Manager HTTPS API
    MANAGER_AGENT_PROMETHEUS = 5090  # Scylla Manager Agent Prometheus API
    MANAGER_DEBUG = 5112  # Scylla Manager pprof Debug
    VECTOR = 15000  # Vector log transport
