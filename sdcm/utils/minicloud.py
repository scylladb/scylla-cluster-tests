import atexit
import dataclasses
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import traceback
from pathlib import Path
from typing import List

import json

import requests

from sdcm.keystore import KeyStore
from sdcm.sct_config import AWS_SUPPORTED_REGIONS
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.aws_region import AwsRegion

LOGGER = logging.getLogger(__name__)

MINICLOUD_DOCKER_IMAGE_DEFAULT = "ghcr.io/scylladb/minicloud:master-4bd3fb6"

# In lightweight mode minicloud gives every VM 1 vCPU and this much RAM, ignoring the
# instance type the test asked for. Scylla refuses to start below 1 GiB per shard, and it
# reserves ~1.7 GiB of the guest for the OS before splitting the rest across shards - so a
# 2.5 GiB VM leaves only 826 MiB and dies with "memory per shard too low" on first boot,
# before SCT gets a chance to apply append_scylla_args or developer_mode. 4 GiB leaves
# ~2.2 GiB for the single shard. Raising this multiplies across every VM in the test, so
# the sct-runner has to be sized for n_db_nodes + n_loaders + n_monitor_nodes times this.
MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT = "4GiB"
MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT = (
    "scylla-qa-keystore",
    "cloudius-jenkins-test",
    "downloads.scylladb.com",
)

MINICLOUD_PORT = 5000
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


class MinicloudError(Exception):
    """Raised when minicloud lifecycle operations fail with actionable messages."""


def _decode_exit(code: int | None) -> str:
    """Human-readable cause for a container exit code."""
    match code:
        case 0:
            return "clean exit"
        case 137:
            return "SIGKILL - `docker rm -f`/`docker kill` from outside, or cgroup OOM"
        case 143:
            return "SIGTERM - `docker stop` from outside"
        case _:
            return "minicloud process exited on its own"


def resolve_minicloud_regions() -> List[str]:
    """Return every AWS region minicloud should prepare.

    Defaults to all SCT-supported regions. minicloud scopes EC2 resources per region -
    a VPC/subnet/security group created in one region is invisible from another, as on
    real AWS - so a region that was never prepared fails provisioning with
    "No SCT security group configured for <region>". Preparing them all up front means
    any test, single- or multi-region, finds what it needs without minicloud having to
    know which regions the test picked.

    ``MINICLOUD_AWS_REGION`` (comma-separated) narrows this to a subset, which is worth
    it only when start-up time matters: each region costs about two seconds.
    """
    if env_regions := os.environ.get("MINICLOUD_AWS_REGION", ""):
        return [region.strip() for region in env_regions.split(",") if region.strip()]
    return list(AWS_SUPPORTED_REGIONS)


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

    docker_image: str = MINICLOUD_DOCKER_IMAGE_DEFAULT
    port: int = 5000
    lightweight: bool = True
    lightweight_memory: str = MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
    s3_passthrough_buckets: List[str] = dataclasses.field(
        default_factory=lambda: list(MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT)
    )
    # Every region the test will use. minicloud stores EC2 resources per region, so each
    # one has to be prepared separately - a resource created in one region is invisible
    # from another, exactly as on real AWS.
    regions: List[str] = dataclasses.field(default_factory=lambda: list(AWS_SUPPORTED_REGIONS))
    default_region: str = MINICLOUD_DEFAULT_REGION
    gcp_project: str = "sct-project-1"
    gcs_bucket: str = ""
    state_dir: str = dataclasses.field(default_factory=lambda: os.path.expanduser("~/.cache/minicloud"))
    log_file: str = ""
    backend: str = ""

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
        """Build MinicloudConfig from SCT params (preferred) or environment variables.

        The `minicloud_*` SCT params are the source of truth - they are the only knob
        available to a test-case yaml or a Jenkins job. The bare MINICLOUD_* env vars stay
        supported so the scripts/ wrappers (and a hand-started container) keep working, and
        they win when set, because they are the more specific, per-invocation override.
        """
        state_dir = os.path.expanduser("~/.cache/minicloud")
        backend = ""
        docker_image = MINICLOUD_DOCKER_IMAGE_DEFAULT
        lightweight = True
        lightweight_memory = MINICLOUD_LIGHTWEIGHT_MEMORY_DEFAULT
        s3_passthrough_buckets = list(MINICLOUD_S3_PASSTHROUGH_BUCKETS_DEFAULT)
        if params:
            backend = params.get("cluster_backend") or ""
            docker_image = params.get("minicloud_docker_image") or docker_image
            # only an explicit value overrides - a params mapping that simply lacks the key
            # (a bare dict from a caller or a test) must not turn lightweight mode off
            if (explicit_lightweight := params.get("minicloud_lightweight")) is not None:
                lightweight = bool(explicit_lightweight)
            lightweight_memory = params.get("minicloud_lightweight_memory") or lightweight_memory
            # StringOrList hands back a plain string ('a,b,c') for the yaml default and a
            # list for an explicit yaml list - and does not split on commas either way, so
            # flatten both shapes rather than trusting the type.
            if buckets := params.get("minicloud_s3_passthrough_buckets"):
                if isinstance(buckets, str):
                    buckets = [buckets]
                s3_passthrough_buckets = [
                    bucket.strip() for entry in buckets for bucket in str(entry).split(",") if bucket.strip()
                ]
        if not backend:
            backend = os.environ.get("SCT_CLUSTER_BACKEND", "")
        # SCT_MINICLOUD_DOCKER_IMAGE is the env form of the minicloud_docker_image param, and
        # has to be read directly, not only via `params`: the sct.py CLI commands that manage
        # the container (start-minicloud, provision-resources, clean-resources) build no
        # SCTConfiguration, so a Jenkins job selecting an image got the built-in default and
        # silently ran a different minicloud than the one its config reported.
        #
        # `or`, not a get() default: an exported-but-empty value (a job or wrapper passing
        # through an unset image param) must not blank out the resolved image.
        docker_image = os.environ.get("SCT_MINICLOUD_DOCKER_IMAGE") or docker_image
        # The bare MINICLOUD_DOCKER stays the most specific override, for the scripts/ wrappers.
        docker_image = os.environ.get("MINICLOUD_DOCKER") or docker_image
        return cls(
            docker_image=docker_image,
            port=int(os.environ.get("MINICLOUD_PORT", "5000")),
            lightweight=lightweight,
            lightweight_memory=os.environ.get("MINICLOUD_LIGHTWEIGHT_MEMORY", lightweight_memory),
            s3_passthrough_buckets=(
                os.environ["S3_PASSTHROUGH_BUCKETS"].split(",")
                if os.environ.get("S3_PASSTHROUGH_BUCKETS")
                else s3_passthrough_buckets
            ),
            regions=resolve_minicloud_regions(),
            default_region=resolve_minicloud_default_region(params),
            gcp_project=os.environ.get("SCT_GCE_PROJECT", "sct-project-1"),
            gcs_bucket=os.environ.get("MINICLOUD_GCS_BUCKET", ""),
            state_dir=state_dir,
            log_file=os.path.join(state_dir, "minicloud.log"),
            backend=backend,
        )


def is_minicloud_active(params=None) -> bool:
    """Check if minicloud mode is active based on env vars or SCT config.

    ``params`` is optional because most call sites (AZ resolver, log collector, cluster
    teardown) have no SCTConfiguration to hand. Pass it wherever one is available: the
    ``minicloud_endpoint_url`` param is the only way a test-case yaml can turn minicloud on,
    and while this looked at the environment alone, a yaml-only setup silently provisioned
    against the real cloud instead.
    """
    if os.environ.get("MINICLOUD_DOCKER"):
        return True

    aws_endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    if aws_endpoint and "localhost" in aws_endpoint:
        return True

    gce_endpoint = os.environ.get("GCE_ENDPOINT_URL", "")
    if gce_endpoint and "localhost" in gce_endpoint:
        return True

    if os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", ""):
        return True

    return bool(params is not None and params.get("minicloud_endpoint_url"))


def get_minicloud_endpoint(params=None) -> str:
    """Get the minicloud endpoint URL.

    Mirrors is_minicloud_active()'s precedence, so a yaml that switched minicloud on via
    ``minicloud_endpoint_url`` is also the one that decides where it listens.
    """
    endpoint = os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", "")
    if endpoint:
        return endpoint
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    if endpoint:
        return endpoint
    if params is not None and (endpoint := params.get("minicloud_endpoint_url")):
        return endpoint
    return f"http://localhost:{MINICLOUD_PORT}"


def check_minicloud_reachability(endpoint: str | None = None, timeout: int = 5) -> bool:
    """Check if minicloud is reachable at the given endpoint.

    Uses DescribeVpcs as the probe: it is implemented by minicloud, served locally
    (no passthrough to real AWS), and cheap. Do not use DescribeRegions — minicloud
    rejects it as an unimplemented action and logs a warning on every probe.

    Returns True if minicloud responds, raises RuntimeError with actionable message if not.
    """
    endpoint = endpoint or get_minicloud_endpoint()
    try:
        response = requests.post(
            endpoint,
            data={"Action": MINICLOUD_HEALTH_ACTION, "Version": "2016-11-15"},
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"minicloud at {endpoint} answered the {MINICLOUD_HEALTH_ACTION} health probe with "
                f"HTTP {response.status_code} instead of 200: {response.text[:500]}"
            )
        return True
    except requests.ConnectionError as exc:
        raise RuntimeError(
            f"minicloud is not reachable at {endpoint}. "
            f"Ensure minicloud is running: minicloud --port {MINICLOUD_PORT}\n"
            f"Connection error: {exc}"
        ) from exc
    except requests.Timeout as exc:
        raise RuntimeError(
            f"minicloud at {endpoint} timed out after {timeout}s. Is it overloaded or starting up?"
        ) from exc


def ensure_minicloud_ready(backend: str = "aws") -> None:
    """Ensure minicloud is running and AWS_ENDPOINT_URL is set.

    If minicloud is already running, validates reachability and sets env vars.
    If not running after retries, auto-starts it with keep_alive=True.

    Retries reachability with exponential backoff to handle transient
    unavailability (e.g., minicloud briefly overloaded or restarting).
    """
    endpoint = get_minicloud_endpoint()
    max_retries = 4
    for attempt in range(max_retries):
        try:
            check_minicloud_reachability(endpoint, timeout=5)
            os.environ.setdefault("AWS_ENDPOINT_URL", endpoint)
            return
        except RuntimeError:
            if attempt < max_retries - 1:
                wait = 2**attempt
                LOGGER.warning(
                    "Minicloud not reachable at %s (attempt %d/%d), retrying in %ds...",
                    endpoint,
                    attempt + 1,
                    max_retries,
                    wait,
                )
                time.sleep(wait)

    LOGGER.info("Minicloud not reachable after %d attempts — auto-starting", max_retries)
    cfg = MinicloudConfig.from_env()
    manager = MinicloudManager(cfg)
    manager.keep_alive = True
    # Skip AWS credentials validation on restart — credentials are already
    # mounted in the container from the initial start-minicloud stage.
    manager.preflight_check(skip_aws_creds=True)
    manager.start()
    if backend in ("aws", "aws-siren"):
        manager.prepare_regions()


def collect_minicloud_logs(logdir: str) -> None:
    """Dump minicloud container logs and inspect state into the test logdir.

    Produces: minicloud.log, minicloud-stderr.log, minicloud-inspect.json.
    Never raises — each collector runs independently.

    Runs after the manager has already streamed the log and (on death or teardown)
    recorded minicloud-inspect.json, so anything already written here wins: those files
    were captured while the container still existed, this one runs after `docker rm -f`.
    """
    container_name = MinicloudManager.MINICLOUD_CONTAINER_NAME
    os.makedirs(logdir, exist_ok=True)

    # 1. Collect container logs (works on stopped/exited containers, fails only if removed)
    log_path = os.path.join(logdir, "minicloud.log")
    if os.path.exists(log_path) and os.path.getsize(log_path):
        LOGGER.info("minicloud logs already streamed to %s, keeping the streamed copy", log_path)
    else:
        result = subprocess.run(["docker", "logs", container_name], capture_output=True, check=False)
        if result.returncode == 0:
            with open(log_path, "wb") as fh:
                fh.write(result.stdout)
                fh.write(result.stderr)
            LOGGER.info("Collected minicloud logs to %s (%d bytes)", log_path, len(result.stdout) + len(result.stderr))

            # Write stderr separately for crash diagnostics
            if result.stderr:
                stderr_path = os.path.join(logdir, "minicloud-stderr.log")
                with open(stderr_path, "wb") as fh:
                    fh.write(result.stderr)
                LOGGER.info("Collected minicloud stderr to %s (%d bytes)", stderr_path, len(result.stderr))
        else:
            LOGGER.warning(
                "Failed to collect minicloud container logs (container removed?): %s",
                result.stderr.decode(errors="replace").strip(),
            )

    # 2. Collect container inspect (state, exit code, health, config)
    inspect_path = os.path.join(logdir, "minicloud-inspect.json")
    if os.path.exists(inspect_path):
        LOGGER.info("minicloud state already recorded to %s while the container existed", inspect_path)
        return
    inspect_result = subprocess.run(
        ["docker", "inspect", container_name],
        capture_output=True,
        check=False,
    )
    if inspect_result.returncode == 0:
        with open(inspect_path, "wb") as fh:
            fh.write(inspect_result.stdout)
        LOGGER.info("Collected minicloud inspect to %s", inspect_path)

        # Log key state info for quick debugging
        try:
            inspect_data = json.loads(inspect_result.stdout)
            if inspect_data:
                state = inspect_data[0].get("State", {})
                LOGGER.info(
                    "Minicloud container state: Status=%s, ExitCode=%s (%s), OOMKilled=%s",
                    state.get("Status"),
                    state.get("ExitCode"),
                    _decode_exit(state.get("ExitCode")),
                    state.get("OOMKilled"),
                )
        except (json.JSONDecodeError, IndexError, KeyError):
            pass
    else:
        LOGGER.warning(
            "Failed to collect minicloud inspect (container removed?): %s",
            inspect_result.stderr.decode(errors="replace").strip(),
        )


class MinicloudManager:
    """Manages the minicloud Docker container lifecycle for SCT tests.

    Usage:
        with MinicloudManager() as mc:
            # minicloud container is running, AWS_ENDPOINT_URL is set
            # run your test...
            pass
        # minicloud container is stopped
    """

    MINICLOUD_CONTAINER_NAME = "minicloud"

    def __init__(self, config: MinicloudConfig | None = None):
        self.config = config or MinicloudConfig.from_env()
        self.port = self.config.port
        self._container_log_process: subprocess.Popen | None = None
        self._stopped = False
        self._owner_pid = os.getpid()
        self.keep_alive = False
        # Container ID of the container we started/adopted. Everything that inspects or
        # removes the container goes through this rather than the name: a concurrent
        # `docker run --name minicloud` recycles the name, and acting on the name would
        # then hit somebody else's container.
        self._container_id: str | None = None
        self._stopping = False
        self._state_snapshotted = False
        self._death_watcher: threading.Thread | None = None

    def preflight_check(self, skip_aws_creds: bool = False, params=None) -> None:
        """Verify prerequisites before starting minicloud container.

        Pass ``params`` (an SCTConfiguration) wherever one exists: it is the only thing that
        knows the test's node counts after the yaml+env merge, and without them the memory
        check below cannot run - the failure then arrives mid-test as a cgroup OOM kill
        (container exit 137) and a wall of SSH timeouts, far from the cause.
        """
        if not Path("/dev/kvm").exists():
            raise MinicloudError(
                "/dev/kvm is not available. Ensure KVM is enabled on this host and the current "
                "user is in the 'kvm' group: sudo usermod -aG kvm $USER"
            )
        if not shutil.which("docker"):
            raise MinicloudError("docker is not available on PATH. Install Docker to run minicloud in container mode.")
        if params is not None:
            self._check_host_memory(params)
        if not skip_aws_creds:
            self._check_aws_credentials()

    @staticmethod
    def _sum_node_counts(value) -> int:
        """Sum an IntOrList param value: an int, a list, or a '3 3' multi-DC string."""
        if not value:
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return sum(int(part) for part in value.split())
        return sum(int(part) for part in value)

    @staticmethod
    def _parse_memory_gib(value: str) -> float:
        """Parse a '4GiB'/'2.5GiB'/'4096MiB' memory string into GiB."""
        match = re.fullmatch(r"\s*([\d.]+)\s*([KMGT])i?B?\s*", str(value), flags=re.IGNORECASE)
        if not match:
            raise MinicloudError(f"cannot parse minicloud_lightweight_memory value: {value!r}")
        factor = {"K": 1 / 1024 / 1024, "M": 1 / 1024, "G": 1, "T": 1024}[match.group(2).upper()]
        return float(match.group(1)) * factor

    def _check_host_memory(self, params) -> None:
        """Fail before start when the test's guests cannot fit into this host's free memory.

        Lightweight mode gives every guest a fixed ``lightweight_memory``, so the requirement
        is exactly guests x per-guest plus host headroom - and only params knows the guest
        count: ``n_db_nodes`` is IntOrList ('3 3' for multi-DC), summed the way
        sct_config.py:sum(n_db_nodes) does. Without this check the container is
        cgroup-OOM-killed mid-test (exit 137) and every VM dies with it.
        """
        if not self.config.lightweight:
            return  # non-lightweight sizing follows the requested instance types; out of scope here
        guests = sum(self._sum_node_counts(params.get(name)) for name in ("n_db_nodes", "n_loaders", "n_monitor_nodes"))
        if not guests:
            return
        meminfo = Path("/proc/meminfo")
        if not meminfo.exists():  # non-Linux dev box; the container will not run here anyway
            return
        available_gib = 0.0
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                available_gib = int(line.split()[1]) / 1024 / 1024
                break
        per_guest_gib = self._parse_memory_gib(self.config.lightweight_memory)
        host_headroom_gib = 2.0  # dockerd, hydra, SCT itself and the page cache need to live too
        needed_gib = guests * per_guest_gib + host_headroom_gib
        if available_gib and available_gib < needed_gib:
            raise MinicloudError(
                f"not enough memory for this test on this host: {guests} guest(s) x "
                f"{per_guest_gib:.1f}GiB ({self.config.lightweight_memory}) + {host_headroom_gib:.0f}GiB "
                f"host headroom = {needed_gib:.1f}GiB needed, but only {available_gib:.1f}GiB is "
                f"available. Reduce n_db_nodes/n_loaders/n_monitor_nodes, lower "
                f"minicloud_lightweight_memory, or use a bigger host - otherwise the container is "
                f"OOM-killed mid-test (exit 137) taking every VM with it."
            )

    def _check_aws_credentials(self) -> None:
        """Verify AWS credentials are configured and valid."""
        try:
            result = subprocess.run(
                ["aws", "sts", "get-caller-identity"],
                capture_output=True,
                timeout=15,
                check=False,
            )
            if result.returncode != 0:
                raise MinicloudError(
                    "AWS credentials are not configured or are expired. Run 'aws sts get-caller-identity' to diagnose."
                )
        except FileNotFoundError as exc:
            raise MinicloudError("AWS CLI not found. Install it or ensure it is on PATH.") from exc

    def _is_endpoint_healthy(self) -> bool:
        """Quick check if minicloud is already responding on the configured endpoint."""
        endpoint = f"http://localhost:{self.config.port}"
        try:
            check_minicloud_reachability(endpoint, timeout=2)
            return True
        except RuntimeError:
            return False

    @property
    def backend(self) -> str:
        """Effective backend — from SCT params when available, else the env var."""
        return self.config.backend or os.environ.get("SCT_CLUSTER_BACKEND", "")

    def _get_running_image(self) -> str:
        """Return the image name of the running minicloud container, or empty string."""
        result = subprocess.run(
            ["docker", "inspect", self.MINICLOUD_CONTAINER_NAME, "--format", "{{.Config.Image}}"],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.decode().strip()
        return ""

    def _inspect_container(self, go_template: str) -> list | None:
        """Return a JSON-decoded `docker inspect` field, or None if unavailable."""
        result = subprocess.run(
            ["docker", "inspect", self.MINICLOUD_CONTAINER_NAME, "--format", go_template],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        try:
            return json.loads(result.stdout.decode())
        except json.JSONDecodeError:
            return None

    def _resolve_container_id(self) -> str | None:
        """Look up the ID of the container currently holding the minicloud name."""
        container_id = self._inspect_container("{{json .Id}}")
        return container_id if isinstance(container_id, str) else None

    def _snapshot_container_state(self, reason: str) -> dict | None:
        """Record the container's exit state while it can still be inspected.

        Must run before any `docker rm -f`: removal destroys the exit code, which is the
        only thing that distinguishes "minicloud crashed on its own" from "something
        outside killed it". Writes minicloud-inspect.json next to the streamed log and
        appends a banner to the log itself, so the evidence lands in the file that gets
        collected either way. Idempotent and never raises.
        """
        if self._state_snapshotted or not self._container_id:
            return None
        self._state_snapshotted = True

        result = subprocess.run(["docker", "inspect", self._container_id], capture_output=True, check=False)
        if result.returncode != 0:
            LOGGER.warning(
                "minicloud container %s was removed before it could be inspected (%s): %s",
                self._container_id[:12],
                reason,
                result.stderr.decode(errors="replace").strip(),
            )
            return None

        log_path = Path(self.config.log_file)
        try:
            (log_path.parent / "minicloud-inspect.json").write_bytes(result.stdout)
            state = json.loads(result.stdout)[0]["State"]
        except (OSError, json.JSONDecodeError, IndexError, KeyError) as exc:
            LOGGER.warning("Could not record minicloud container state (%s): %s", reason, exc)
            return None

        exited = state.get("Status") != "running"
        if exited:
            banner = (
                f"=== minicloud container {self._container_id[:12]} EXITED at {state.get('FinishedAt')} "
                f"[{reason}]: ExitCode={state.get('ExitCode')} ({_decode_exit(state.get('ExitCode'))}) "
                f"OOMKilled={state.get('OOMKilled')} Error={state.get('Error')!r} - every VM it hosted "
                f"is gone, so all further SSH to node IPs will time out ===\n"
            )
        else:
            banner = (
                f"=== minicloud container {self._container_id[:12]} still running at {reason}; "
                f"state recorded to minicloud-inspect.json ===\n"
            )
        try:
            with open(log_path, "a") as fh:
                fh.write(banner)
        except OSError as exc:
            LOGGER.warning("Could not append minicloud state banner to %s: %s", log_path, exc)
        (LOGGER.error if exited else LOGGER.info)(banner.strip())
        return state

    def _watch_container_death(self, log_process: subprocess.Popen) -> None:
        """Snapshot the container's state the moment it dies.

        `docker logs -f` returns exactly when the container exits, so waiting on the
        streamer we already spawn is a precise death signal that costs nothing - and the
        snapshot then happens while the exit code is still there to read.
        """
        log_process.wait()
        if self._stopping or self._stopped or self.keep_alive:
            return  # we tore it down ourselves, nothing to report
        state = self._snapshot_container_state(reason="died while the test was still running")
        TestFrameworkEvent(
            source="MinicloudManager",
            message=(
                f"minicloud container died mid-test (ExitCode={(state or {}).get('ExitCode')}); "
                f"all VMs it hosted are gone - any SSH failure after this is a consequence, not the cause"
            ),
            severity=Severity.ERROR,
        ).publish_or_dump()

    def _container_gce_gaps(self) -> list[str]:
        """Return the reasons a running container cannot serve the gce backend.

        Both of these are start-time only — neither can be added to a live container:

        * GOOGLE_APPLICATION_CREDENTIALS — without it minicloud's gcp_auth falls back to
          the GCE metadata service, absent in the container, and every GCP call fails with
          'no available authentication method found'.
        * --gcs-bucket — without it image downloads fail with
          '500 Internal error: --gcs-bucket is required for GCP image downloads'.

        A container missing either still answers EC2 health probes, so liveness alone is
        not enough to decide the container is reusable.
        """
        gaps = []
        env = self._inspect_container("{{json .Config.Env}}")
        if not any(str(entry).startswith("GOOGLE_APPLICATION_CREDENTIALS=") for entry in env or []):
            gaps.append("no GOOGLE_APPLICATION_CREDENTIALS")
        cmd = self._inspect_container("{{json .Config.Cmd}}")
        if "--gcs-bucket" not in [str(arg) for arg in cmd or []]:
            gaps.append("no --gcs-bucket")
        return gaps

    def _force_stop_container(self) -> None:
        # Target the ID we started when we know it, so we can never remove a different
        # container that has meanwhile taken over the 'minicloud' name.
        target = self._container_id or self.MINICLOUD_CONTAINER_NAME
        subprocess.run(["docker", "rm", "-f", target], capture_output=True, check=False)
        subprocess.run(["docker", "network", "disconnect", "-f", "host", target], capture_output=True, check=False)

    def start(self) -> None:
        """Start minicloud Docker container and wait for it to become healthy.

        If minicloud is already running with the expected image and the credentials the
        current backend needs, reuse it. Otherwise stop it and start fresh — a container
        that merely answers health probes may still be unusable for this backend.
        """
        self._setup_gcp_credentials()
        self._setup_host_networking()

        if self._is_endpoint_healthy():
            running_image = self._get_running_image()
            expected_image = self.config.docker_image
            restart_reason = ""
            if running_image and running_image != expected_image:
                restart_reason = f"running image '{running_image}' != expected '{expected_image}'"
            elif self.backend in ("gce", "gce-siren") and (gaps := self._container_gce_gaps()):
                restart_reason = f"running container is not usable for the '{self.backend}' backend: {', '.join(gaps)}"
            if restart_reason:
                LOGGER.info("minicloud restarting — %s", restart_reason)
                self._force_stop_container()
            else:
                endpoint = f"http://localhost:{self.config.port}"
                LOGGER.info("minicloud already running at %s (image: %s), reusing", endpoint, running_image)
                self._container_id = self._resolve_container_id()
                os.environ["AWS_ENDPOINT_URL"] = endpoint
                self.set_env_overrides()
                self._start_log_streaming()
                return

        container_name = self.MINICLOUD_CONTAINER_NAME
        image = self.config.docker_image

        self._force_stop_container()

        docker_cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            container_name,
            "--device",
            "/dev/kvm",
            "--device",
            "/dev/net/tun",
            "--network",
            "host",
            "--cap-add",
            "NET_ADMIN",
            "-v",
            f"{self.config.state_dir}:/root/.cache/minicloud",
        ]

        aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if aws_key and aws_secret:
            docker_cmd += ["-e", f"AWS_ACCESS_KEY_ID={aws_key}", "-e", f"AWS_SECRET_ACCESS_KEY={aws_secret}"]
            aws_token = os.environ.get("AWS_SESSION_TOKEN")
            if aws_token:
                docker_cmd += ["-e", f"AWS_SESSION_TOKEN={aws_token}"]
        else:
            aws_dir = Path.home() / ".aws"
            if aws_dir.is_dir():
                docker_cmd += ["-v", f"{aws_dir}:/root/.aws:ro"]

        docker_cmd += ["-e", f"AWS_REGION={self.config.region}"]

        gcs_key = os.environ.get("GCS_KEY_FILE") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if gcs_key and Path(gcs_key).is_file():
            docker_cmd += [
                "-v",
                f"{gcs_key}:/etc/minicloud/gcs-key.json:ro",
                "-e",
                "GOOGLE_APPLICATION_CREDENTIALS=/etc/minicloud/gcs-key.json",
                "-e",
                f"GOOGLE_CLOUD_PROJECT={self.config.gcp_project}",
            ]

        docker_cmd.append(image)

        minicloud_args = [
            "--port",
            str(self.config.port),
            "--aws-region",
            self.config.region,
            "--s3-passthrough-buckets",
            ",".join(self.config.s3_passthrough_buckets),
        ]
        if self.config.gcs_bucket:
            minicloud_args += ["--gcs-bucket", self.config.gcs_bucket]
        minicloud_args += ["--gcp-project", self.config.gcp_project]
        if self.config.lightweight:
            minicloud_args += ["--lightweight", "--lightweight-memory", self.config.lightweight_memory]

        full_cmd = docker_cmd + minicloud_args
        LOGGER.info("Starting minicloud container: %s", " ".join(full_cmd))
        # `docker run -d` prints the container ID - keep it, it is what we inspect and
        # remove later, and it stays valid even if the name gets taken over.
        run_result = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
        self._container_id = run_result.stdout.strip() or self._resolve_container_id()
        LOGGER.info("minicloud container id: %s", self._container_id)

        endpoint = f"http://localhost:{self.config.port}"
        os.environ["AWS_ENDPOINT_URL"] = endpoint
        LOGGER.info("Set AWS_ENDPOINT_URL=%s", endpoint)

        self._wait_for_health(endpoint)
        self.set_env_overrides()
        self._start_log_streaming()

        atexit.register(self._atexit_stop)
        LOGGER.info("minicloud container is ready")

    def stop(self) -> None:
        """Stop and remove the minicloud Docker container."""
        if self._stopped:
            return
        if self.keep_alive:
            LOGGER.info("minicloud keep_alive is set, skipping stop")
            return
        container_name = self.MINICLOUD_CONTAINER_NAME
        # Set before terminating the streamer: that terminate wakes the death watcher, and
        # this flag is how it tells our own teardown apart from an external kill.
        self._stopping = True
        if self._container_log_process:
            self._container_log_process.terminate()
            self._container_log_process = None
        LOGGER.warning(
            "DEBUG: docker rm -f %s called from stop(), pid=%s, owner_pid=%s\n%s",
            container_name,
            os.getpid(),
            self._owner_pid,
            "".join(traceback.format_stack()),
        )
        LOGGER.info("Stopping minicloud container '%s'...", container_name)
        # Snapshot before removal - `docker rm -f` destroys the exit code for good.
        self._snapshot_container_state(reason="teardown")
        self._force_stop_container()
        os.environ.pop("AWS_ENDPOINT_URL", None)
        os.environ.pop("GCE_ENDPOINT_URL", None)
        self._stopped = True
        LOGGER.info("minicloud stopped")

    def _atexit_stop(self) -> None:
        """Atexit handler — only stops if we own the process and haven't stopped yet."""
        LOGGER.warning(
            "DEBUG: _atexit_stop called, pid=%s, owner_pid=%s, stopped=%s", os.getpid(), self._owner_pid, self._stopped
        )
        if os.getpid() != self._owner_pid:
            return
        if not self._stopped:
            self.stop()

    def _start_log_streaming(self) -> None:
        """Stream minicloud container logs to the configured log file."""
        log_path = Path(self.config.log_file)
        os.makedirs(log_path.parent, exist_ok=True)
        log_fh = open(log_path, "a")  # noqa: SIM115
        self._container_log_process = subprocess.Popen(
            ["docker", "logs", "-f", self._container_id or self.MINICLOUD_CONTAINER_NAME],
            stdout=log_fh,
            stderr=log_fh,
        )
        LOGGER.info("minicloud logs streaming to %s", log_path)
        # `docker logs -f` returns exactly when the container exits, so waiting on the
        # streamer we already spawn is a free, precise death signal.
        self._death_watcher = threading.Thread(
            target=self._watch_container_death,
            args=(self._container_log_process,),
            name="minicloud-death-watcher",
            daemon=True,
        )
        self._death_watcher.start()

    def prepare_regions(self) -> None:
        """Configure every region in the config via AwsRegion.configure().

        Must be called only after AWS_ENDPOINT_URL is set (i.e., after start()).
        SSM configuration failures are logged as warnings and do not abort.

        All regions are prepared, not just the one the test names: minicloud keeps EC2
        resources per region, so a region that was skipped has no VPC, subnets or
        security group and provisioning into it fails outright.
        """
        LOGGER.info("Preparing %d AWS region(s) for minicloud: %s", len(self.config.regions), self.config.regions)
        for region_name in self.config.regions:
            LOGGER.info("Preparing AWS region '%s' for minicloud...", region_name)
            region = AwsRegion(region_name=region_name)
            try:
                region.configure()
            except Exception as exc:  # noqa: BLE001
                exc_str = str(exc).lower()
                if "ssm" in exc_str or "systems manager" in exc_str:
                    LOGGER.warning("SSM configuration failed (expected on minicloud, ignoring): %s", exc)
                else:
                    raise
            LOGGER.info("Region '%s' prepared.", region_name)

    def set_env_overrides(self) -> None:
        """Set SCT environment overrides required for minicloud mode."""
        endpoint = f"http://localhost:{self.config.port}"
        overrides = {
            "AWS_ENDPOINT_URL": endpoint,
            "SCT_MINICLOUD_ENDPOINT_URL": endpoint,
            "SCT_IP_SSH_CONNECTIONS": "private",
            "SCT_INSTANCE_PROVISION": "on_demand",
            "SCT_ENTERPRISE_DISABLE_KMS": "true",
            "SCT_FORCE_RUN_IOTUNE": "false",
        }
        if self.backend in ("gce", "gce-siren"):
            overrides["GCE_ENDPOINT_URL"] = endpoint
        for key, value in overrides.items():
            os.environ[key] = value
            LOGGER.debug("Set %s=%s", key, value)
        LOGGER.info("minicloud env overrides applied")

    def _setup_host_networking(self) -> None:
        """Extract and run minicloud-setup.sh on the host to configure TUN device and routes.

        minicloud requires a persistent TUN device (minicloud0) with IP 10.127.0.1/24 on the host
        for VM networking (IMDS, DNS, and host↔VM connectivity). The setup script is bundled inside
        the container image and must be run with sudo on the host before the container starts.
        """
        tun_name = "minicloud0"
        result = subprocess.run(
            ["ip", "addr", "show", tun_name],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0 and b"10.127.0.1" in result.stdout:
            LOGGER.info("Host networking already configured (%s has 10.127.0.1)", tun_name)
            return

        LOGGER.info("Configuring host networking for minicloud...")
        image = self.config.docker_image
        setup_script_path = os.path.join(self.config.state_dir, "minicloud-setup.sh")
        os.makedirs(self.config.state_dir, exist_ok=True)

        extract = subprocess.run(
            ["docker", "run", "--rm", "--entrypoint", "cat", image, "/usr/local/bin/minicloud-setup.sh"],
            capture_output=True,
            check=False,
        )
        if extract.returncode != 0:
            LOGGER.warning("Could not extract minicloud-setup.sh from image %s: %s", image, extract.stderr.decode())
            return

        with open(setup_script_path, "wb") as fh:
            fh.write(extract.stdout)
        os.chmod(setup_script_path, 0o755)

        run_result = subprocess.run(
            ["sudo", setup_script_path],
            capture_output=True,
            check=False,
        )
        if run_result.returncode != 0:
            LOGGER.warning(
                "minicloud-setup.sh failed (exit %d): %s",
                run_result.returncode,
                run_result.stderr.decode().strip(),
            )
        else:
            LOGGER.info("Host networking configured successfully")

    def _setup_gcp_credentials(self) -> None:
        """Download GCP service account JSON from KeyStore and set GOOGLE_APPLICATION_CREDENTIALS.

        minicloud's Rust gcp_auth crate uses Application Default Credentials (ADC).
        Setting GOOGLE_APPLICATION_CREDENTIALS to a service account JSON file is the
        simplest way to provide credentials for GCE API passthrough.
        """
        if self.backend not in ("gce", "gce-siren"):
            return

        creds = None
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

        if creds_path and Path(creds_path).is_file():
            LOGGER.info("GOOGLE_APPLICATION_CREDENTIALS already set to %s", creds_path)
            with open(creds_path) as fh:
                creds = json.load(fh)
        else:
            try:
                creds = KeyStore().get_gcp_credentials()
                creds_path = os.path.join(self.config.state_dir, "gcp-credentials.json")
                os.makedirs(self.config.state_dir, exist_ok=True)
                with open(creds_path, "w") as fh:
                    json.dump(creds, fh)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
                LOGGER.info("Set GOOGLE_APPLICATION_CREDENTIALS=%s", creds_path)
            except Exception:  # noqa: BLE001
                LOGGER.warning(
                    "Failed to download GCP credentials from KeyStore; minicloud GCE passthrough may not work"
                )

        if not self.config.gcs_bucket and creds:
            self.config.gcs_bucket = self._ensure_gcs_bucket(creds)

    @staticmethod
    def _ensure_gcs_bucket(creds: dict) -> str:
        """Create the minicloud staging GCS bucket if it doesn't exist, return its name."""
        from google.cloud import storage  # noqa: PLC0415 - optional GCE dependency
        from google.oauth2 import service_account  # noqa: PLC0415

        project_id = os.environ.get("SCT_GCE_PROJECT", "sct-project-1")
        bucket_name = f"{project_id}-minicloud-staging"
        credentials = service_account.Credentials.from_service_account_info(creds)
        client = storage.Client(credentials=credentials, project=project_id)

        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            LOGGER.info("Creating GCS bucket %s for minicloud image staging", bucket_name)
            bucket.storage_class = "STANDARD"
            client.create_bucket(bucket, location="us")
            bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 7}}]
            bucket.patch()
            LOGGER.info("Created GCS bucket %s with 7-day lifecycle", bucket_name)
        else:
            LOGGER.info("GCS bucket %s already exists", bucket_name)

        MinicloudManager._ensure_cloudbuild_access(creds, project_id, bucket_name)
        return bucket_name

    @staticmethod
    def _ensure_cloudbuild_access(creds: dict, project_id: str, bucket_name: str) -> None:
        """Ensure Cloud Build API is enabled and service account has bucket access.

        minicloud exports GCP images via Cloud Build. This requires:
        1. cloudbuild.googleapis.com enabled on the project
        2. The Cloud Build service account has objectAdmin on the staging bucket
        3. The compute service account has cloudbuild.builds.builder role

        These are idempotent operations — safe to call on every run.
        """
        from google.cloud import storage  # noqa: PLC0415 - optional GCE dependency
        from google.oauth2 import service_account  # noqa: PLC0415

        credentials = service_account.Credentials.from_service_account_info(creds)

        try:
            scoped_creds = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
            import google.auth.transport.requests  # noqa: PLC0415

            request = google.auth.transport.requests.Request()
            scoped_creds.refresh(request)

            import requests as http_requests  # noqa: PLC0415

            url = f"https://serviceusage.googleapis.com/v1/projects/{project_id}/services/cloudbuild.googleapis.com:enable"
            resp = http_requests.post(
                url,
                headers={"Authorization": f"Bearer {scoped_creds.token}"},
                timeout=30,
            )
            if resp.status_code in (200, 409):
                LOGGER.info("Cloud Build API enabled (or already enabled) for %s", project_id)
            else:
                LOGGER.warning(
                    "Could not enable Cloud Build API (status %d): %s. "
                    "Run manually: gcloud services enable cloudbuild.googleapis.com --project=%s",
                    resp.status_code,
                    resp.text[:200],
                    project_id,
                )
        except Exception:  # noqa: BLE001
            LOGGER.warning(
                "Could not enable Cloud Build API programmatically. "
                "Run manually: gcloud services enable cloudbuild.googleapis.com --project=%s",
                project_id,
            )

        try:
            client = storage.Client(credentials=credentials, project=project_id)
            bucket = client.bucket(bucket_name)
            policy = bucket.get_iam_policy(requested_policy_version=3)

            cloudbuild_sa = None
            for binding in policy.bindings:
                for member in binding.get("members", []):
                    if "@cloudbuild.gserviceaccount.com" in member:
                        cloudbuild_sa = member
                        break

            if cloudbuild_sa:
                LOGGER.info("Cloud Build SA %s already has bucket access", cloudbuild_sa)
            else:
                LOGGER.warning(
                    "Cloud Build service account not found in bucket IAM. "
                    "If image export fails, run:\n"
                    "  gcloud services enable cloudbuild.googleapis.com --project=%s\n"
                    "  # Wait 60s, then:\n"
                    "  gsutil iam ch serviceAccount:$(gcloud projects describe %s "
                    "--format='value(projectNumber)')@cloudbuild.gserviceaccount.com:objectAdmin "
                    "gs://%s",
                    project_id,
                    project_id,
                    bucket_name,
                )
        except Exception:  # noqa: BLE001
            LOGGER.warning("Could not verify Cloud Build bucket access")

    def _wait_for_health(self, endpoint: str) -> None:
        """Wait for minicloud to respond to health checks."""
        deadline = time.time() + MINICLOUD_HEALTH_TIMEOUT
        last_error = None

        while time.time() < deadline:
            try:
                check_minicloud_reachability(endpoint, timeout=2)
                LOGGER.info("minicloud is healthy at %s", endpoint)
                return
            except RuntimeError as exc:
                last_error = exc
                time.sleep(MINICLOUD_HEALTH_INTERVAL)

        raise RuntimeError(
            f"minicloud did not become healthy within {MINICLOUD_HEALTH_TIMEOUT}s.\nLast error: {last_error}"
        )

    @property
    def is_running(self) -> bool:
        """Check if minicloud container is running."""
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self.MINICLOUD_CONTAINER_NAME],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0 and "true" in result.stdout

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
