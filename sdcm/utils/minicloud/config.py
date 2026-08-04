"""Minicloud configuration: module constants, MinicloudError and MinicloudConfig."""

import dataclasses
import functools
import os
from typing import List
from urllib.parse import urlparse

import yaml

from sdcm import sct_abs_path
from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.utils.minicloud.endpoint import MINICLOUD_PORT, get_minicloud_endpoint


@functools.lru_cache(maxsize=1)
def default_minicloud_image() -> str:
    """Default minicloud docker image.

    The tag lives in defaults/docker_images/minicloud/values_minicloud.yaml — the same
    helm-values shape as the stress-tool images, so renovate bumps it automatically and
    SCTConfiguration exposes it as ``stress_image.minicloud``. Read directly here as well
    because the container-management CLI paths (start-minicloud, clean-resources) run
    without an SCTConfiguration.
    """
    values_file = sct_abs_path("defaults/docker_images/minicloud/values_minicloud.yaml")
    with open(values_file, encoding="utf-8") as fh:
        return yaml.safe_load(fh)["minicloud"]["image"]


# In lightweight mode minicloud gives every VM 1 vCPU and this much RAM, ignoring the
# instance type the test asked for. Scylla refuses to start below 1 GiB per shard, and it
# reserves ~1.7 GiB of the guest for the OS before splitting the rest across shards - so a
# 2.5 GiB VM leaves only 826 MiB and dies with "memory per shard too low" on first boot,
# before SCT gets a chance to apply append_scylla_args or developer_mode. 4 GiB leaves
# ~2.2 GiB for the single shard. Raising this multiplies across every VM in the test, so
# the sct-runner has to be sized for n_db_nodes + n_loaders + n_monitor_nodes times this.
MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT = "4GiB"
# One shard per vCPU. minicloud's own default is also 1, but pass it explicitly so the value a
# run used is visible in its config rather than inherited from whichever image version ran.
MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT = 1
# Image cache, per-instance qcow2 disks and minicloud.log all live here - tens of GiB.
MINICLOUD_STATE_DIR_DEFAULT = "~/.cache/minicloud"
# Backend-independent: GCE runs also reach S3 (keystore, job artifacts, downloads), so the
# same list is passed to the container no matter which backend the test uses.
# Keep in sync with the default in scripts/start-minicloud.sh.
MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT = (
    "scylla-qa-keystore",
    "cloudius-jenkins-test",
    "downloads.scylladb.com",
)

MINICLOUD_CONTAINER_NAME = "minicloud"
MINICLOUD_HEALTH_TIMEOUT = 30
MINICLOUD_HEALTH_INTERVAL = 1
# EC2 action used as the liveness probe. Must be an action minicloud actually implements
# and serves locally, otherwise every probe is rejected and logged as an unknown action.
MINICLOUD_HEALTH_ACTION = "DescribeVpcs"
# minicloud validates a not-yet-cached AMI by calling real AWS DescribeImages with the
# server's own --aws-region (src/api/instance.rs), not the region the RunInstances request
# targets. Scylla dev AMIs are published in eu-west-1, so any other default makes every
# cache-miss launch fail with InvalidAMIID.NotFound. Keep this on eu-west-1 until
# minicloud's create-resource path honours the per-request region.
MINICLOUD_DEFAULT_REGION = "eu-west-1"
# Only used when neither the gce_project param nor SCT_GCE_PROJECT says otherwise. minicloud
# serves the Compute API itself, but the GCS staging bucket and Cloud Build image export are
# real services in this project, so a wrong value here creates real resources in the wrong place.
MINICLOUD_GCP_PROJECT_DEFAULT = "sct-project-1"

# The emulated guest networks deliberately live in a DIFFERENT range than any real cloud
# network. The host running the guests may itself sit inside a real cloud VPC - an
# sct-runner always does - and any overlap is fatal twice over: an emulated CIDR equal to a
# real one routes guest traffic out eth0 instead of the minicloud TUN, and a blanket host
# route (the old 10.0.0.0/8) black-holes the QA infra (Argus, argus-proxy) from the host.
#
# Every emulated guest lands inside 10.160.0.0/11 - the single range (plus the emulator's
# built-in 172.31.0.0/16 default VPC) that MINICLOUD_HOST_VPC_ROUTES tells
# minicloud-setup.sh to route into the TUN device (scylladb/minicloud#187):
#
#   * AWS: AwsRegion shifts its region index (0-15 from all_aws_regions) by 160 whenever
#     is_minicloud_active(), so the emulated VPCs occupy 10.160.0.0/16 .. 10.175.0.0/16.
#   * GCE: prepare_gce_network() pre-creates the qa-vpc network with one explicit subnet
#     per supported region in 10.176.0.0/16 .. 10.179.0.0/16. Without it, minicloud
#     emulates GCE auto-mode and allocates /20s from 10.128.0.0/9 - unroutable from the
#     host and inside the real GCE VPC space a runner lives in.
#
# The TUN address stays outside every routed range.
MINICLOUD_REGION_INDEX_OFFSET = 160
MINICLOUD_TUN_ADDR = "10.127.0.1/24"
MINICLOUD_HOST_VPC_ROUTES = ("10.160.0.0/11", "172.31.0.0/16")
MINICLOUD_GCE_NETWORK = "qa-vpc"
MINICLOUD_GCE_REGION_INDEX_OFFSET = 176
# Matches getJenkinsLabels' supported GCE regions; the order defines each region's subnet
# index, so only append.
MINICLOUD_GCE_REGIONS = ("us-east1", "us-east4", "us-west1", "us-central1")
MINICLOUD_GCE_SUBNET_CIDR_TMPL = "10.{}.0.0/16"


class MinicloudError(Exception):
    """Raised when minicloud lifecycle operations fail with actionable messages."""


def resolve_minicloud_regions(params=None) -> List[str]:
    """Return every AWS region minicloud should prepare.

    Defaults to all SCT-supported regions. minicloud scopes EC2 resources per region -
    a VPC/subnet/security group created in one region is invisible from another, as on
    real AWS - so a region that was never prepared fails provisioning with
    "No SCT security group configured for <region>". Preparing them all up front means
    any test, single- or multi-region, finds what it needs without minicloud having to
    know which regions the test picked.

    The ``minicloud_regions`` param (SCT_MINICLOUD_REGIONS) narrows this to a subset,
    which is worth it only when start-up time matters: each region costs ~two seconds.
    """
    if regions := _param_as_list(params, "minicloud_regions"):
        return regions
    return list(AWS_SUPPORTED_REGIONS)


def _param_as_list(params, name: str) -> List[str]:
    """Flatten a StringOrList param: plain 'a,b' string, list, or list of joined strings."""
    value = params.get(name) if params else None
    if not value:
        return []
    if isinstance(value, str):
        value = [value]
    return [item.strip() for entry in value for item in str(entry).split(",") if item.strip()]


def resolve_minicloud_default_region(params=None) -> str:
    """Region for the container's ``--aws-region``/``AWS_REGION``.

    Mostly a fallback for requests that carry no region of their own - boto3 signs each
    call with its target region and minicloud reads that off the SigV4 credential scope.
    Prefer the test's own region so those region-less requests land where the test works.

    Not purely cosmetic, though: minicloud's RunInstances validates an uncached AMI
    against this region instead of the request's, so a mismatch fails the launch with
    InvalidAMIID.NotFound. See MINICLOUD_DEFAULT_REGION.
    """
    configured = params.get("region_name") if params else None
    if not configured:
        configured = os.environ.get("SCT_REGION_NAME", "")
    if isinstance(configured, str):
        configured = configured.replace(",", " ").split()
    for region in configured or []:
        if region:
            return region
    return MINICLOUD_DEFAULT_REGION


@dataclasses.dataclass
class MinicloudConfig:
    """Configuration for MinicloudManager."""

    docker_image: str = dataclasses.field(default_factory=default_minicloud_image)
    port: int = MINICLOUD_PORT
    lightweight: bool = True
    lightweight_memory: str = MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    lightweight_vcpus: int = MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT
    # Docker limits on the container itself. Empty means "pass no flag" — the container is then
    # bounded only by the host, which is what every run did before these became configurable.
    container_memory: str = ""
    container_cpus: str = ""
    container_name: str = MINICLOUD_CONTAINER_NAME
    s3_passthrough_buckets: List[str] = dataclasses.field(
        default_factory=lambda: list(MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT)
    )
    # Every region the test will use. minicloud stores EC2 resources per region, so each
    # one has to be prepared separately - a resource created in one region is invisible
    # from another, exactly as on real AWS.
    regions: List[str] = dataclasses.field(default_factory=lambda: list(AWS_SUPPORTED_REGIONS))
    default_region: str = MINICLOUD_DEFAULT_REGION
    gcp_project: str = MINICLOUD_GCP_PROJECT_DEFAULT
    gcs_bucket: str = ""
    state_dir: str = dataclasses.field(default_factory=lambda: os.path.expanduser(MINICLOUD_STATE_DIR_DEFAULT))
    log_file: str = ""
    backend: str = ""
    # Skips the host-memory preflight gate (minicloud_skip_memory_check param) — the
    # arithmetic is deliberately conservative and a developer who knows the workload's
    # real footprint should not be blocked by it.
    skip_memory_check: bool = False

    @property
    def region(self) -> str:
        """Region passed to the container as --aws-region/AWS_REGION.

        Mostly a fallback for requests that carry no region of their own: boto3 signs
        every call with its target region and minicloud reads that off the SigV4
        credential scope, so per-region clients work regardless of this value - except
        for uncached-AMI validation on RunInstances, which uses this region rather than
        the request's. See MINICLOUD_DEFAULT_REGION.
        """
        if self.default_region in self.regions:
            return self.default_region
        return self.regions[0] if self.regions else MINICLOUD_DEFAULT_REGION

    @classmethod
    def from_env(cls, params=None) -> "MinicloudConfig":
        """Build MinicloudConfig from SCT params.

        Every knob is a documented `minicloud_*` SCT config option (each with its
        automatic SCT_* env form), so a test-case yaml, a Jenkins job and a shell all use
        the same names. No bare MINICLOUD_* env vars are read — the only non-param inputs
        are cloud-standard ones (SCT_GCE_PROJECT, GOOGLE_APPLICATION_CREDENTIALS, AWS_*)
        and the SCT_MINICLOUD_ENDPOINT_URL activation/endpoint override.
        """
        state_dir = MINICLOUD_STATE_DIR_DEFAULT
        backend = ""
        docker_image = default_minicloud_image()
        lightweight = True
        lightweight_memory = MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
        lightweight_vcpus = MINICLOUD_LIGHTWEIGHT_VCPUS_DEFAULT
        container_memory = ""
        container_cpus = ""
        container_name = MINICLOUD_CONTAINER_NAME
        skip_memory_check = False
        gcs_bucket = ""
        gcp_project = ""
        if params:
            backend = params.get("cluster_backend") or ""
            # minicloud_docker_image is the explicit override; the renovate-managed
            # default arrives via stress_image.minicloud (defaults/docker_images/).
            docker_image = params.get("minicloud_docker_image") or params.get("stress_image.minicloud") or docker_image
            # only an explicit value overrides - a params mapping that simply lacks the key
            # (a bare dict from a caller or a test) must not turn lightweight mode off
            if (explicit_lightweight := params.get("minicloud_lightweight")) is not None:
                lightweight = bool(explicit_lightweight)
            lightweight_memory = params.get("minicloud_lightweight_memory") or lightweight_memory
            lightweight_vcpus = int(params.get("minicloud_lightweight_vcpus") or lightweight_vcpus)
            # These three are genuinely optional: empty has to mean "no docker limit" / "keep the
            # default name", so they only ever widen, never blank a value the caller set.
            container_memory = params.get("minicloud_container_memory") or ""
            container_cpus = params.get("minicloud_container_cpus") or ""
            container_name = params.get("minicloud_container_name") or container_name
            state_dir = params.get("minicloud_state_dir") or state_dir
            skip_memory_check = bool(params.get("minicloud_skip_memory_check"))
            gcs_bucket = params.get("minicloud_gcs_bucket") or ""
            # the SCT param wins over the environment: gce_project can be set from a
            # test-case yaml or defaults without ever reaching os.environ, and this project
            # is where ensure_gcs_bucket() creates the real staging bucket - it has to be
            # the same project the GCE clients talk to.
            gcp_project = params.get("gce_project") or ""
        if not backend:
            backend = os.environ.get("SCT_CLUSTER_BACKEND", "")
        if not gcp_project:
            gcp_project = os.environ.get("SCT_GCE_PROJECT") or MINICLOUD_GCP_PROJECT_DEFAULT
        s3_passthrough_buckets = _param_as_list(params, "minicloud_s3_passthrough_buckets") or list(
            MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT
        )
        # ~ only expands here, after the param has had its say, so a configured path with a
        # leading ~ works the same as the default.
        state_dir = os.path.expanduser(state_dir)
        return cls(
            docker_image=docker_image,
            port=cls._resolve_port(params),
            lightweight=lightweight,
            lightweight_memory=lightweight_memory,
            lightweight_vcpus=lightweight_vcpus,
            container_memory=container_memory,
            container_cpus=container_cpus,
            container_name=container_name,
            s3_passthrough_buckets=s3_passthrough_buckets,
            regions=resolve_minicloud_regions(params),
            default_region=resolve_minicloud_default_region(params),
            gcp_project=gcp_project,
            gcs_bucket=gcs_bucket,
            state_dir=state_dir,
            log_file=os.path.join(state_dir, "minicloud.log"),
            backend=backend,
            skip_memory_check=skip_memory_check,
        )

    @staticmethod
    def _resolve_port(params=None) -> int:
        """Port the manager probes and starts the container on.

        Derived from the resolved endpoint so that a yaml/env endpoint on a custom
        port (e.g. ``minicloud_endpoint_url: http://localhost:6000``) is the port the
        manager actually uses, instead of silently rebuilding localhost:5000.
        """
        return urlparse(get_minicloud_endpoint(params)).port or MINICLOUD_PORT
