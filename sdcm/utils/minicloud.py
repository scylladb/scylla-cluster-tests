import atexit
import dataclasses
import logging
import os
import shutil
import subprocess
import time
import traceback
from pathlib import Path
from typing import List

import json

import requests

from sdcm.keystore import KeyStore
from sdcm.utils.aws_region import AwsRegion

LOGGER = logging.getLogger(__name__)

MINICLOUD_DOCKER_IMAGE_DEFAULT = "ghcr.io/scylladb/minicloud:0.1.0"

MINICLOUD_PORT = 5000
MINICLOUD_HEALTH_TIMEOUT = 30
MINICLOUD_HEALTH_INTERVAL = 1


class MinicloudError(Exception):
    """Raised when minicloud lifecycle operations fail with actionable messages."""


@dataclasses.dataclass
class MinicloudConfig:
    """Configuration for MinicloudManager."""

    docker_image: str = MINICLOUD_DOCKER_IMAGE_DEFAULT
    port: int = 5000
    lightweight: bool = True
    lightweight_memory: str = "2.5GiB"
    s3_passthrough_buckets: List[str] = dataclasses.field(
        default_factory=lambda: ["scylla-qa-keystore", "cloudius-jenkins-test", "downloads.scylladb.com"]
    )
    region: str = "us-east-1"
    gcp_project: str = "sct-project-1"
    gcs_bucket: str = ""
    state_dir: str = dataclasses.field(default_factory=lambda: os.path.expanduser("~/.cache/minicloud"))
    log_file: str = ""

    @classmethod
    def from_env(cls) -> "MinicloudConfig":
        """Build MinicloudConfig from environment variables."""
        state_dir = os.path.expanduser("~/.cache/minicloud")
        return cls(
            docker_image=os.environ.get("MINICLOUD_DOCKER", MINICLOUD_DOCKER_IMAGE_DEFAULT),
            port=int(os.environ.get("MINICLOUD_PORT", "5000")),
            lightweight=True,
            lightweight_memory=os.environ.get("MINICLOUD_LIGHTWEIGHT_MEMORY", "2.5GiB"),
            s3_passthrough_buckets=os.environ.get(
                "S3_PASSTHROUGH_BUCKETS", "scylla-qa-keystore,cloudius-jenkins-test,downloads.scylladb.com"
            ).split(","),
            region=os.environ.get("MINICLOUD_AWS_REGION", "us-east-1"),
            gcp_project=os.environ.get("SCT_GCE_PROJECT", "sct-project-1"),
            gcs_bucket=os.environ.get("MINICLOUD_GCS_BUCKET", ""),
            state_dir=state_dir,
            log_file=os.path.join(state_dir, "minicloud.log"),
        )


def is_minicloud_active() -> bool:
    """Check if minicloud mode is active based on env vars or SCT config."""
    if os.environ.get("MINICLOUD_DOCKER"):
        return True

    aws_endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    if aws_endpoint and "localhost" in aws_endpoint:
        return True

    gce_endpoint = os.environ.get("GCE_ENDPOINT_URL", "")
    if gce_endpoint and "localhost" in gce_endpoint:
        return True

    minicloud_url = os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", "")
    return bool(minicloud_url)


def get_minicloud_endpoint() -> str:
    """Get the minicloud endpoint URL."""
    endpoint = os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", "")
    if endpoint:
        return endpoint
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    if endpoint:
        return endpoint
    return f"http://localhost:{MINICLOUD_PORT}"


def check_minicloud_reachability(endpoint: str | None = None, timeout: int = 5) -> bool:
    """Check if minicloud is reachable at the given endpoint.

    Returns True if minicloud responds, raises RuntimeError with actionable message if not.
    """
    endpoint = endpoint or get_minicloud_endpoint()
    try:
        response = requests.post(
            endpoint,
            data={"Action": "DescribeRegions", "Version": "2016-11-15"},
            timeout=timeout,
        )
        return response.status_code in (200, 400)
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
        manager.prepare_region()


def collect_minicloud_logs(logdir: str) -> None:
    """Dump minicloud container logs and inspect state into the test logdir.

    Produces: minicloud.log, minicloud-stderr.log, minicloud-inspect.json.
    Never raises — each collector runs independently.
    """
    container_name = MinicloudManager.MINICLOUD_CONTAINER_NAME
    os.makedirs(logdir, exist_ok=True)

    # 1. Collect container logs (works on stopped/exited containers, fails only if removed)
    result = subprocess.run(
        ["docker", "logs", container_name],
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        log_path = os.path.join(logdir, "minicloud.log")
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
    inspect_result = subprocess.run(
        ["docker", "inspect", container_name],
        capture_output=True,
        check=False,
    )
    if inspect_result.returncode == 0:
        inspect_path = os.path.join(logdir, "minicloud-inspect.json")
        with open(inspect_path, "wb") as fh:
            fh.write(inspect_result.stdout)
        LOGGER.info("Collected minicloud inspect to %s", inspect_path)

        # Log key state info for quick debugging
        try:
            inspect_data = json.loads(inspect_result.stdout)
            if inspect_data:
                state = inspect_data[0].get("State", {})
                LOGGER.info(
                    "Minicloud container state: Status=%s, ExitCode=%s, OOMKilled=%s",
                    state.get("Status"),
                    state.get("ExitCode"),
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

    def preflight_check(self, skip_aws_creds: bool = False) -> None:
        """Verify prerequisites before starting minicloud container."""
        if not Path("/dev/kvm").exists():
            raise MinicloudError(
                "/dev/kvm is not available. Ensure KVM is enabled on this host and the current "
                "user is in the 'kvm' group: sudo usermod -aG kvm $USER"
            )
        if not shutil.which("docker"):
            raise MinicloudError("docker is not available on PATH. Install Docker to run minicloud in container mode.")
        if not skip_aws_creds:
            self._check_aws_credentials()

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

    def start(self) -> None:
        """Start minicloud Docker container and wait for it to become healthy.

        If minicloud is already running (e.g., started externally), detect it via
        health check and reuse it instead of attempting to start a new container.
        """
        self._setup_gcp_credentials()
        self._setup_host_networking()

        if self._is_endpoint_healthy():
            endpoint = f"http://localhost:{self.config.port}"
            LOGGER.info("minicloud already running at %s, reusing", endpoint)
            os.environ["AWS_ENDPOINT_URL"] = endpoint
            self.set_env_overrides()
            self._start_log_streaming()
            return

        container_name = self.MINICLOUD_CONTAINER_NAME
        image = self.config.docker_image

        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)
        subprocess.run(
            ["docker", "network", "disconnect", "-f", "host", container_name], capture_output=True, check=False
        )

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
        subprocess.run(full_cmd, check=True)

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
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, check=False)
        subprocess.run(
            ["docker", "network", "disconnect", "-f", "host", container_name], capture_output=True, check=False
        )
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
            ["docker", "logs", "-f", self.MINICLOUD_CONTAINER_NAME],
            stdout=log_fh,
            stderr=log_fh,
        )
        LOGGER.info("minicloud logs streaming to %s", log_path)

    def prepare_region(self) -> None:
        """Configure the AWS region via AwsRegion.configure().

        Must be called only after AWS_ENDPOINT_URL is set (i.e., after start()).
        SSM configuration failures are logged as warnings and do not abort.
        """
        LOGGER.info("Preparing AWS region '%s' for minicloud...", self.config.region)
        region = AwsRegion(region_name=self.config.region)
        try:
            region.configure()
        except Exception as exc:  # noqa: BLE001
            exc_str = str(exc).lower()
            if "ssm" in exc_str or "systems manager" in exc_str:
                LOGGER.warning("SSM configuration failed (expected on minicloud, ignoring): %s", exc)
            else:
                raise
        LOGGER.info("Region '%s' prepared.", self.config.region)

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
        if os.environ.get("SCT_CLUSTER_BACKEND") == "gce":
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
        if os.environ.get("SCT_CLUSTER_BACKEND") != "gce":
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
