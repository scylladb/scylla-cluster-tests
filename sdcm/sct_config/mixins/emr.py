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

"""Amazon EMR (spark-migrator) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class EmrConfigMixin(BaseModel):
    """Amazon EMR (spark-migrator).

    The EMR cluster that runs the Spark migrator job.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Amazon EMR (spark-migrator)"

    emr_applications: list = SctField(
        description="List of EMR applications to install (default: ['Spark'])",
    )
    emr_install_spark4_via_bootstrap: Boolean = SctField(
        description="Legacy fallback: install Spark 4.x via an EMR bootstrap action and submit the migrator "
        "through script-runner.jar (for emr-7.x releases). Default value is false - i.e. deployment of native Spark "
        "on an `emr-spark-8.x` release label.",
    )
    emr_instance_count_core: int = SctField(
        description="How many EMR core nodes to launch.",
    )
    emr_instance_count_task: int = SctField(
        description="How many EMR task nodes to launch (compute only, no HDFS).",
    )
    emr_instance_type_core: String = SctField(
        description="EC2 instance type for the EMR core nodes (they run both compute and HDFS).",
    )
    emr_instance_type_master: String = SctField(
        description="Instance type for EMR master node (e.g., 'm5.xlarge')",
    )
    emr_instance_type_task: String = SctField(
        description="Instance type for EMR task nodes (optional, uses Spot instances)",
    )
    emr_keep_alive: Boolean = SctField(
        description="Whether EMR cluster stays alive after job completion (default: true for reuse during testing)",
    )
    emr_log_uri: String = SctField(
        description="S3 URI for EMR cluster logs (e.g., 's3://sct-emr-spark-migrator-{region}/logs/')",
    )
    emr_release_label: String = SctField(
        description="EMR release version (e.g., 'emr-7.8.0'). When set, an EMR cluster is provisioned alongside the Scylla cluster.",
    )
    emr_spark_migrator_jar_path: String = SctField(
        description="S3 path or local path to the spark-migrator JAR file",
    )
    emr_spark_migrator_release: String = SctField(
        description="scylla-migrator release tag (e.g., 'v1.1.2'). When set, JAR is auto-downloaded "
        "from GitHub releases and uploaded to S3. Takes precedence over emr_spark_migrator_jar_path.",
    )
    emr_spot_bid_percentage: int = SctField(
        description="Max Spot price as percentage of On-Demand for EMR task nodes (default: 100)",
    )
