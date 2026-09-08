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

"""Domain mixins holding the SCTConfiguration field definitions.

Each mixin owns one group of configuration options. ``SCTConfiguration`` inherits from all of
them, so every field stays flat and every existing YAML key and attribute access is unchanged --
only the source is split.

``CONFIG_GROUPS`` is the browse order used for the generated documentation and for the field
order on the assembled model: cross-cutting concerns first, then per-backend, then per-test-type.

A field's group is decided by what it configures, not by which file it happened to sit in.
``unit_tests/unit/config/test_option_groups.py`` guards the conventions -- every option belongs to
exactly one mixin, name prefixes agree with their group, and every option carries a description
that says more than its own name.
"""

from sdcm.sct_config.mixins.common import CommonConfigMixin
from sdcm.sct_config.mixins.scylla import ScyllaConfigMixin
from sdcm.sct_config.mixins.scylla_doctor import ScyllaDoctorConfigMixin
from sdcm.sct_config.mixins.nemesis import NemesisConfigMixin
from sdcm.sct_config.mixins.stress import StressConfigMixin
from sdcm.sct_config.mixins.monitoring import MonitoringConfigMixin
from sdcm.sct_config.mixins.logs import LogsConfigMixin
from sdcm.sct_config.mixins.manager import ManagerConfigMixin
from sdcm.sct_config.mixins.aux_db import AuxDbConfigMixin
from sdcm.sct_config.mixins.alternator import AlternatorConfigMixin
from sdcm.sct_config.mixins.vector_store import VectorStoreConfigMixin
from sdcm.sct_config.mixins.kafka import KafkaConfigMixin
from sdcm.sct_config.mixins.aws import AwsConfigMixin
from sdcm.sct_config.mixins.gce import GceConfigMixin
from sdcm.sct_config.mixins.azure import AzureConfigMixin
from sdcm.sct_config.mixins.oci import OciConfigMixin
from sdcm.sct_config.mixins.kubernetes import KubernetesConfigMixin
from sdcm.sct_config.mixins.docker import DockerConfigMixin
from sdcm.sct_config.mixins.baremetal import BaremetalConfigMixin
from sdcm.sct_config.mixins.xcloud import XcloudConfigMixin
from sdcm.sct_config.mixins.minicloud import MinicloudConfigMixin
from sdcm.sct_config.mixins.longevity import LongevityConfigMixin
from sdcm.sct_config.mixins.performance import PerformanceConfigMixin
from sdcm.sct_config.mixins.upgrade import UpgradeConfigMixin
from sdcm.sct_config.mixins.grow_cluster import GrowClusterConfigMixin
from sdcm.sct_config.mixins.refresh import RefreshConfigMixin
from sdcm.sct_config.mixins.jepsen import JepsenConfigMixin
from sdcm.sct_config.mixins.emr import EmrConfigMixin
from sdcm.sct_config.mixins.spark_migrator import SparkMigratorConfigMixin

#: Mixins in documentation/browse order. Also the field order on the assembled model.
CONFIG_GROUPS = (
    CommonConfigMixin,
    ScyllaConfigMixin,
    NemesisConfigMixin,
    StressConfigMixin,
    MonitoringConfigMixin,
    LogsConfigMixin,
    ScyllaDoctorConfigMixin,
    ManagerConfigMixin,
    AuxDbConfigMixin,
    AlternatorConfigMixin,
    VectorStoreConfigMixin,
    KafkaConfigMixin,
    AwsConfigMixin,
    GceConfigMixin,
    AzureConfigMixin,
    OciConfigMixin,
    KubernetesConfigMixin,
    DockerConfigMixin,
    BaremetalConfigMixin,
    XcloudConfigMixin,
    MinicloudConfigMixin,
    LongevityConfigMixin,
    PerformanceConfigMixin,
    UpgradeConfigMixin,
    GrowClusterConfigMixin,
    RefreshConfigMixin,
    JepsenConfigMixin,
    EmrConfigMixin,
    SparkMigratorConfigMixin,
)

__all__ = [
    "CommonConfigMixin",
    "ScyllaConfigMixin",
    "NemesisConfigMixin",
    "StressConfigMixin",
    "MonitoringConfigMixin",
    "LogsConfigMixin",
    "ScyllaDoctorConfigMixin",
    "ManagerConfigMixin",
    "AuxDbConfigMixin",
    "AlternatorConfigMixin",
    "VectorStoreConfigMixin",
    "KafkaConfigMixin",
    "AwsConfigMixin",
    "GceConfigMixin",
    "AzureConfigMixin",
    "OciConfigMixin",
    "KubernetesConfigMixin",
    "DockerConfigMixin",
    "BaremetalConfigMixin",
    "XcloudConfigMixin",
    "MinicloudConfigMixin",
    "LongevityConfigMixin",
    "PerformanceConfigMixin",
    "UpgradeConfigMixin",
    "GrowClusterConfigMixin",
    "RefreshConfigMixin",
    "JepsenConfigMixin",
    "EmrConfigMixin",
    "SparkMigratorConfigMixin",
    "CONFIG_GROUPS",
]
