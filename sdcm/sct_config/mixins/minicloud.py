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
# Copyright (c) 2026 ScyllaDB

"""Minicloud configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class MinicloudConfigMixin(BaseModel):
    """Minicloud.

    Minicloud is an AWS-API-compatible environment rather than a cloud of its own: it runs with
    `cluster_backend: aws` and an endpoint override, and these options control the local minicloud
    service.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Minicloud"

    minicloud_container_cpus: String = SctField(
        description="Cap the minicloud container's CPU allowance, in docker --cpus form "
        "(e.g. '8' or '7.5'). Empty means no limit",
    )
    minicloud_container_memory: String = SctField(
        description="Cap the minicloud container's memory (e.g. '32GiB'). Empty means no docker "
        "limit, so the container can consume the whole host. Setting it also makes this, rather "
        "than the host's free memory, the budget the preflight guest-memory gate measures against",
    )
    minicloud_container_name: String = SctField(
        description="Name of the minicloud docker container. Change it to run two emulators on one "
        "host — a second run under the same name force-removes the first one's container",
    )
    minicloud_docker_image: String = SctField(
        description="Explicit minicloud image override. Empty means the renovate-managed "
        "default from defaults/docker_images/minicloud/ (exposed as stress_image.minicloud)",
    )
    minicloud_endpoint_url: String = SctField(
        description="EC2 API endpoint URL for minicloud. When set, SCT adapts for minicloud "
        "limitations (no spot, no EIP, graceful TerminateInstances). Example: http://localhost:5000",
        appendable=False,
    )
    minicloud_gcs_bucket: String = SctField(
        description="GCS bucket for minicloud GCE image staging. Empty means derive "
        "<project>-minicloud-staging and create it on demand",
    )
    minicloud_keep_alive: Boolean = SctField(
        description="Leave the minicloud container running after the test instead of tearing it down "
        "(CI sets this so separate provision/test/collect/clean stages reach the same container)",
    )
    minicloud_lightweight: Boolean = SctField(
        description="Enable lightweight mode for minicloud deployments",
    )
    minicloud_lightweight_memory: String = SctField(
        description="Memory allocation for lightweight minicloud deployments",
    )
    minicloud_lightweight_vcpus: int = SctField(
        description="vCPUs per guest in lightweight mode. Scylla runs one shard per vCPU, so this "
        "multiplies with minicloud_lightweight_memory across every guest in the test — raise it "
        "only on a host with cores to spare",
    )
    minicloud_regions: StringOrList = SctField(
        description="Narrow the AWS regions minicloud prepares (default: every SCT-supported region; "
        "each costs ~2s at start-up)",
    )
    minicloud_s3_passthrough_buckets: StringOrList = SctField(
        description="S3 buckets minicloud proxies to real AWS (keystore, job artifacts, downloads). "
        "Backend-independent: GCE runs reach S3 for the same content",
    )
    minicloud_scylla_reserve_memory: String = SctField(
        description="Extra memory reserved by scylla-server for the guest OS on lightweight minicloud guests, "
        "passed as --reserve-memory (for example, '3G'). Without this option, Scylla usually keeps about "
        "~1.5GiB on guests below ~22GiB RAM, which may be too small for sshd and SCT helper tools; then sshd "
        "cannot fork and one-shot commands can be OOM-killed. Empty (default) means do not pass this option. "
        "Because this memory comes from Scylla own budget, tests must opt in. Ignored if append_scylla_args "
        "already contains --memory or --reserve-memory",
    )
    minicloud_skip_memory_check: Boolean = SctField(
        description="Skip the conservative host-memory preflight gate — for development machines "
        "whose owner knows the workload's real footprint; an oversized test then dies mid-run as "
        "a container OOM kill (exit 137)",
    )
    minicloud_state_dir: String = SctField(
        description="Where minicloud keeps its image cache, per-instance disks and minicloud.log — "
        "tens of GiB. Empty means ~/.cache/minicloud; point it at a bigger disk or a CI workspace",
    )
