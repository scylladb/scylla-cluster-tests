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

"""Scylla Manager configuration options."""

from typing import ClassVar
from typing_extensions import Annotated

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from sdcm.mgmt.common import AgentBackupParameters
from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String, StringOrList, dict_or_str


class ManagerConfigMixin(BaseModel):
    """Scylla Manager.

    Scylla Manager server and agent: versions, repos and backup/restore settings.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Scylla Manager"

    backup_bucket_backend: String = SctField(
        description="the backend to be used for backup (e.g., 's3', 'gcs' or 'azure')",
    )
    backup_bucket_location: StringOrList = SctField(
        description="the bucket name to be used for backup (e.g., 'manager-backup-tests')",
    )
    backup_bucket_region: String = SctField(
        description="the AWS region of a bucket to be used for backup (e.g., 'eu-west-1')",
    )
    manager_backup_restore_method: String = SctField(
        description="The object storage transfer method to use by Scylla Manager in backup or restore. Supported methods: native, rclone, auto.",
    )
    manager_prometheus_port: int = SctField(
        description="Port to be used by the manager to contact Prometheus",
    )
    manager_scylla_backend_version: String = SctField(
        description="Version of ScyllaDB to install as Manager backend",
        appendable=False,
    )
    manager_version: String = SctField(
        description="Version of Scylla Manager server and agent to install",
        appendable=False,
    )
    mgmt_agent_backup_config: Annotated[AgentBackupParameters | None, BeforeValidator(dict_or_str)] = SctField(
        description="Manager agent backup general configuration: checkers, transfers, low_level_retries. For example, {'checkers': 100, 'transfers': 2, 'low_level_retries': 20}",
    )
    mgmt_docker_image: String = SctField(
        description="Scylla manager docker image, i.e. 'scylladb/scylla-manager:2.2.1'",
    )
    mgmt_nodetool_refresh_flags: String = SctField(
        description="Nodetool refresh extra options like --load-and-stream or --primary-replica-only",
    )
    mgmt_prepare_snapshot_size: int = SctField(
        description="Size of backup snapshot in Gb to be prepared for backup",
    )
    mgmt_restore_extra_params: String = SctField(
        description="Manager restore operation extra parameters: batch-size, parallel, etc. "
        "For example, `--batch-size 2 --parallel 1`. Provided string appends the restore cmd",
    )
    mgmt_reuse_backup_snapshot_name: String = SctField(
        description="Name of backup snapshot to use in Manager restore benchmark test, for example, 500gb_2t_ics. "
        "The name provides the info about dataset size (500gb), tables number (2) and compaction (ICS)",
    )
    mgmt_skip_post_restore_stress_read: Boolean = SctField(
        description="Skip post-restore c-s verification read in the Manager restore benchmark tests",
    )
    mgmt_snapshots_preparer_params: DictOrStr = SctField(
        description="Custom parameters of c-s write operation used in snapshots preparer",
    )
    scylla_mgmt_address: String = SctField(
        description="Url to the repo of scylla manager version to install for management tests",
    )
    scylla_mgmt_agent_address: String = SctField(
        description="Url to the repo of scylla manager agent version to install for management tests",
    )
    scylla_mgmt_agent_version: String = SctField(
        description="Version of Scylla Manager agent to install for management tests",
        appendable=False,
    )
    scylla_mgmt_pkg: String = SctField(
        description="Url to the scylla manager packages to install for management tests",
    )
    scylla_mgmt_upgrade_to_repo: String = SctField(
        description="Url to the repo of scylla manager version to upgrade to for management tests",
    )
    scylla_repo_m: String = SctField(
        description="Url to the repo of scylla version to install scylla from for management tests",
    )
    target_manager_version: String = SctField(
        description="Version of Scylla Manager server and agent to upgrade to",
        appendable=False,
    )
    target_scylla_mgmt_agent_address: String = SctField(
        description="Url to the repo of scylla manager version used to upgrade the manager agents",
    )
    target_scylla_mgmt_server_address: String = SctField(
        description="Url to the repo of scylla manager version used to upgrade the manager server",
    )
    use_cloud_manager: Boolean = SctField(
        description="When define true, will install scylla cloud manager",
    )
    use_mgmt: Boolean = SctField(
        description="When define true, will install scylla management",
    )
