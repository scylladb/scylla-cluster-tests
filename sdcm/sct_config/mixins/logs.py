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

"""Logs, diagnostics and teardown configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String


class LogsConfigMixin(BaseModel):
    """Logs, diagnostics and teardown.

    How logs and diagnostics are collected, and what happens to the resources when the test ends.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Logs, diagnostics and teardown"

    collect_logs: Boolean = SctField(
        description="Collect logs from instances and sct runner",
    )
    collect_nvme_diagnostics: Boolean = SctField(
        description="Collect NVMe SMART logs, error logs, and self-test results from DB nodes during test teardown. "
        "Requires nvme-cli to be installed on the nodes. Skipped gracefully on backends without NVMe devices.",
    )
    execute_post_behavior: Boolean = SctField(
        description="Run post behavior actions in sct teardown step",
    )
    logs_transport: Literal["ssh", "docker", "syslog-ng", "vector"] = SctField(
        description="How to transport logs: syslog-ng, ssh or docker",
    )
    nvme_self_test_type: int = SctField(
        description="NVMe device self-test type to run: 1 (short, ~2 min) or 2 (extended, may take hours). "
        "Only used when collect_nvme_diagnostics is enabled.",
    )
    post_behavior_db_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the db cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_dedicated_host: Literal["keep", "destroy"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the dedicated hosts at the end of the test.

        'destroy' - Destroy hosts (default)
        'keep' - Keep hosts allocated
        """,
    )
    post_behavior_emr_cluster: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the EMR cluster at the end of the test.

        'destroy' - Destroy EMR cluster (default)
        'keep' - Keep EMR cluster running
        'keep-on-failure' - Keep EMR cluster if testrun failed
        """,
    )
    post_behavior_k8s_cluster: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the k8s cluster at the end of the test.

        'destroy' - Destroy k8s cluster and credentials (default)
        'keep' - Keep k8s cluster running and leave credentials alone
        'keep-on-failure' - Keep k8s cluster if testrun failed
        """,
    )
    post_behavior_loader_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the loader cloud instances at the end of the test.

            'destroy' - Destroy instances and credentials (default)
            'keep' - Keep instances running and leave credentials alone
            'keep-on-failure' - Keep instances if testrun failed
         """,
    )
    post_behavior_monitor_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
            Failure/post test behavior, i.e. what to do with the monitor cloud instances at the end of the test.
         """,
    )
    post_behavior_vector_store_nodes: Literal["destroy", "keep", "keep-on-failure"] = SctField(
        description="""
        Failure/post test behavior, i.e. what to do with the vector store cloud instances at the end of the test.

        'destroy' - Destroy instances and credentials (default)
        'keep' - Keep instances running and leave credentials alone
        'keep-on-failure' - Keep instances if testrun failed
        """,
    )
    run_scylla_doctor: Boolean = SctField(
        description="Flag to run Scylla Doctor tool",
    )
    run_scylla_doctor_only: Boolean = SctField(
        description="""When true, the artifact test runs only the Scylla Doctor validation
                (install, collect vitals, analyze, verify) and skips all other artifact checks
                such as stop/start, cassandra-stress, etc. Useful for fast SD
                release gating. Implies run_scylla_doctor=true.""",
    )
    scylla_doctor_edition: Literal["basic", "full"] = SctField(
        description="""Scylla Doctor edition to use. Allowed values: 'basic', 'full'.
                'basic' fetches the free/open-source edition via HTTP.
                'full' fetches the full/enterprise edition from a private S3 bucket.""",
    )
    scylla_doctor_full_tarball_url: String = SctField(
        description="""Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the
                standard version-based S3 lookup and downloads SD directly from this URL.
                Use for testing unofficial or pre-release SD versions.
                Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'""",
    )
    scylla_doctor_version: String = SctField(
        description="""Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')
                to hardcode the version, or leave empty to use the latest available version. For stability,
                artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.""",
    )
    teardown_validators: DictOrStr = SctField(
        description="Validators to use during teardown phase",
    )
    use_scylla_doctor_on_failure: Boolean = SctField(
        description="Run scylla-doctor on test failure to collect additional diagnostics",
    )
