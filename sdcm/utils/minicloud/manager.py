"""MinicloudManager: docker container lifecycle, forensics and region preparation."""

import atexit
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.minicloud.activation import (
    check_minicloud_reachability,
    set_minicloud_endpoint_env,
    validate_minicloud_params,
)
from sdcm.utils.minicloud.config import (
    MINICLOUD_HEALTH_INTERVAL,
    MINICLOUD_HEALTH_TIMEOUT,
    MinicloudConfig,
    MinicloudError,
)
from sdcm.utils.minicloud.gcp import setup_gcp_credentials
from sdcm.utils.minicloud.log_collection import _decode_exit, redact_docker_inspect
from sdcm.utils.minicloud.networking import setup_host_networking
from sdcm.utils.minicloud.preflight import (
    check_aws_credentials,
    check_host_memory,
    parse_memory_gib,
    sum_node_counts,
)

LOGGER = logging.getLogger(__name__)


class MinicloudManager:
    """Manages the minicloud Docker container lifecycle for SCT tests.

    Usage:
        with MinicloudManager() as mc:
            # minicloud container is running, AWS_ENDPOINT_URL is set
            # run your test...
            pass
        # minicloud container is stopped
    """

    def __init__(self, config: MinicloudConfig | None = None):
        self.config = config or MinicloudConfig.from_env()
        self.port = self.config.port
        self._container_log_process: subprocess.Popen | None = None
        self._container_log_file = None
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

    def preflight_check(self, skip_aws_creds: bool = False, params=None, enforce_overlay: bool = True) -> None:
        """Verify prerequisites before starting minicloud container.

        Pass ``params`` (an SCTConfiguration) wherever one exists: it is the only thing that
        knows the test's node counts after the yaml+env merge, and without them the memory
        check below cannot run - the failure then arrives mid-test as a cgroup OOM kill
        (container exit 137) and a wall of SSH timeouts, far from the cause.

        ``enforce_overlay=False`` downgrades the configurations/minicloud.yaml overlay
        check to a warning — for the container-management CLI paths where the caller gave
        no --config at all (a bare `hydra start-minicloud -b aws` builds default params
        that legitimately fail the overlay check but runs no test with them).
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
            if enforce_overlay:
                validate_minicloud_params(params)
            else:
                try:
                    validate_minicloud_params(params)
                except MinicloudError as exc:
                    LOGGER.warning("%s", exc)
        if not skip_aws_creds:
            self._check_aws_credentials()

    # thin delegates to sdcm.utils.minicloud.preflight — kept as methods so existing
    # call sites and test patch targets on the class stay valid
    _sum_node_counts = staticmethod(sum_node_counts)
    _parse_memory_gib = staticmethod(parse_memory_gib)

    def _check_host_memory(self, params) -> None:
        check_host_memory(self.config, params)

    @staticmethod
    def _check_aws_credentials() -> None:
        check_aws_credentials()

    def is_endpoint_healthy(self) -> bool:
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
            ["docker", "inspect", self.config.container_name, "--format", "{{.Config.Image}}"],
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.decode().strip()
        return ""

    def _inspect_container(self, go_template: str):
        """Return a JSON-decoded `docker inspect` field, or None if unavailable."""
        result = subprocess.run(
            ["docker", "inspect", self.config.container_name, "--format", go_template],
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
            (log_path.parent / "minicloud-inspect.json").write_bytes(redact_docker_inspect(result.stdout))
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
        # keep_alive deliberately NOT in this condition: keep_alive controls teardown,
        # and CI (which always sets it) is exactly where an unexpected mid-test death
        # must still produce the snapshot and the root-cause event.
        if self._stopping or self._stopped:
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

    def _container_sizing_gaps(self) -> list[str]:
        """Return the ways a running container's sizing differs from what this run asked for.

        Every sizing knob is start-time only — the guest sizing is a minicloud CLI argument and
        the docker caps are cgroup limits fixed at ``docker run`` — so reuse silently keeps the
        previous run's sizing: a rerun with a new ``SCT_MINICLOUD_LIGHTWEIGHT_MEMORY`` would get
        the old guests, and a changed ``minicloud_container_memory`` would leave the host-memory
        gate measuring against a cap nothing enforces.

        A container that cannot be inspected yields no gaps: absence of evidence is not a reason
        to throw away a working emulator (and every VM it hosts) mid-workflow.
        """
        cmd = self._inspect_container("{{json .Config.Cmd}}")
        if cmd is None:
            return []
        cmd = [str(arg) for arg in cmd]
        gaps = []

        def running_arg(flag: str) -> str:
            index = cmd.index(flag) + 1 if flag in cmd else 0
            return cmd[index] if 0 < index < len(cmd) else ""

        if self.config.lightweight != ("--lightweight" in cmd):
            gaps.append(f"lightweight mode is {'--lightweight' in cmd}, this run wants {self.config.lightweight}")
        elif self.config.lightweight:
            for flag, wanted in (
                ("--lightweight-memory", self.config.lightweight_memory),
                ("--lightweight-vcpus", str(self.config.lightweight_vcpus)),
            ):
                if (running := running_arg(flag)) != wanted:
                    gaps.append(f"{flag} is {running or 'unset'}, this run wants {wanted}")

        # docker's own caps, read back from HostConfig: unset means no flag, which docker reports
        # as 0 - so the same "0" stands for both sides of "no limit" and the comparison is exact.
        for field, wanted_value, flag, divisor in (
            ("Memory", self.config.container_memory, "--memory", 1024**3),
            ("NanoCpus", self.config.container_cpus, "--cpus", 10**9),
        ):
            raw = self._inspect_container(f"{{{{json .HostConfig.{field}}}}}")
            if not isinstance(raw, (int, float)):
                continue
            wanted = parse_memory_gib(wanted_value) if flag == "--memory" and wanted_value else float(wanted_value or 0)
            # format both through :g so the GiB->flag rounding docker was given is the same
            # rounding this comparison sees, and an unchanged config never looks like a change
            if f"{raw / divisor:g}" != f"{wanted:g}":
                gaps.append(f"{flag} is {raw / divisor:g}, this run wants {wanted:g}")
        return gaps

    def _force_stop_container(self) -> None:
        # Target the ID we started when we know it, so we can never remove a different
        # container that has meanwhile taken over the 'minicloud' name.
        target = self._container_id or self.config.container_name
        subprocess.run(["docker", "rm", "-f", target], capture_output=True, check=False)
        subprocess.run(["docker", "network", "disconnect", "-f", "host", target], capture_output=True, check=False)

    def _setup_gcp_credentials(self) -> None:
        setup_gcp_credentials(self.config, self.backend)

    def _setup_host_networking(self) -> None:
        setup_host_networking(self.config)

    def start(self) -> None:
        """Start minicloud Docker container and wait for it to become healthy.

        If minicloud is already running with the expected image and the credentials the
        current backend needs, reuse it. Otherwise stop it and start fresh — a container
        that merely answers health probes may still be unusable for this backend.
        """
        self._setup_gcp_credentials()
        self._setup_host_networking()

        if self.is_endpoint_healthy():
            running_image = self._get_running_image()
            expected_image = self.config.docker_image
            restart_reason = ""
            if running_image and running_image != expected_image:
                restart_reason = f"running image '{running_image}' != expected '{expected_image}'"
            elif self.backend in ("gce", "gce-siren") and (gaps := self._container_gce_gaps()):
                restart_reason = f"running container is not usable for the '{self.backend}' backend: {', '.join(gaps)}"
            elif sizing_gaps := self._container_sizing_gaps():
                restart_reason = f"running container was started with different sizing: {', '.join(sizing_gaps)}"
            if restart_reason:
                LOGGER.info("minicloud restarting — %s", restart_reason)
                self._force_stop_container()
            else:
                endpoint = f"http://localhost:{self.config.port}"
                LOGGER.info("minicloud already running at %s (image: %s), reusing", endpoint, running_image)
                self._container_id = self._resolve_container_id()
                self.set_env_overrides()
                self._start_log_streaming()
                return

        container_name = self.config.container_name
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

        # Optional docker limits. Unset means no flag at all, so the container stays bounded only
        # by the host - what every run did before these were configurable. A cap is what stops a
        # runaway emulator from taking the whole dev box (or CI agent) down with it.
        if self.config.container_memory:
            # docker --memory speaks b/k/m/g, not the GiB form the rest of the minicloud config
            # uses, so convert rather than make the user remember two unit styles.
            docker_cmd += ["--memory", f"{parse_memory_gib(self.config.container_memory):g}g"]
        if self.config.container_cpus:
            docker_cmd += ["--cpus", str(self.config.container_cpus)]

        # Name-only --env: docker reads the values from this process's environment, so no
        # credential ever appears in argv (visible via ps/procfs) or in the logged command.
        if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
            docker_cmd += ["-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY"]
            if os.environ.get("AWS_SESSION_TOKEN"):
                docker_cmd += ["-e", "AWS_SESSION_TOKEN"]
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
            minicloud_args += [
                "--lightweight",
                "--lightweight-memory",
                self.config.lightweight_memory,
                "--lightweight-vcpus",
                str(self.config.lightweight_vcpus),
            ]

        full_cmd = docker_cmd + minicloud_args
        # Safe to log verbatim: credentials are passed as name-only --env flags above.
        LOGGER.info("Starting minicloud container: %s", " ".join(full_cmd))
        # `docker run -d` prints the container ID - keep it, it is what we inspect and
        # remove later, and it stays valid even if the name gets taken over.
        run_result = subprocess.run(full_cmd, capture_output=True, text=True, check=False)
        if run_result.returncode != 0:
            raise MinicloudError(
                f"docker run failed (exit {run_result.returncode}) for image {image}: {run_result.stderr.strip()}"
            )
        self._container_id = run_result.stdout.strip() or self._resolve_container_id()
        LOGGER.info("minicloud container id: %s", self._container_id)

        endpoint = f"http://localhost:{self.config.port}"
        try:
            self._wait_for_health(endpoint)
        except Exception:
            # health never arrived and the atexit handler is not registered yet — clean up
            # the container we just started instead of leaking it (snapshot state first)
            self._snapshot_container_state(reason="failed to become healthy during start")
            self._force_stop_container()
            raise
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
        container_name = self.config.container_name
        # Set before terminating the streamer: that terminate wakes the death watcher, and
        # this flag is how it tells our own teardown apart from an external kill.
        self._stopping = True
        if self._container_log_process:
            self._container_log_process.terminate()
            try:
                self._container_log_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._container_log_process.kill()
            self._container_log_process = None
        self._close_log_file()
        LOGGER.info("Stopping minicloud container '%s'...", container_name)
        # Snapshot before removal - `docker rm -f` destroys the exit code for good.
        self._snapshot_container_state(reason="teardown")
        self._force_stop_container()
        os.environ.pop("AWS_ENDPOINT_URL", None)
        os.environ.pop("GCE_ENDPOINT_URL", None)
        os.environ.pop("SCT_MINICLOUD_ENDPOINT_URL", None)
        self._stopped = True
        LOGGER.info("minicloud stopped")

    def _atexit_stop(self) -> None:
        """Atexit handler — only stops if we own the process and haven't stopped yet."""
        if os.getpid() != self._owner_pid:
            return
        if not self._stopped:
            self.stop()

    def _close_log_file(self) -> None:
        if self._container_log_file:
            try:
                self._container_log_file.close()
            except OSError:
                LOGGER.debug("Closing minicloud log file failed", exc_info=True)
            self._container_log_file = None

    def _start_log_streaming(self) -> None:
        """Stream minicloud container logs to the configured log file.

        The file handle is owned by the manager (not a `with` block: the streamer child
        outlives this method) and is closed in stop() — and here first, so re-entry via a
        second start() never leaks the previous handle.
        """
        log_path = Path(self.config.log_file)
        os.makedirs(log_path.parent, exist_ok=True)
        self._close_log_file()
        self._container_log_file = open(log_path, "a")  # noqa: SIM115
        self._container_log_process = subprocess.Popen(
            ["docker", "logs", "-f", self._container_id or self.config.container_name],
            stdout=self._container_log_file,
            stderr=self._container_log_file,
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
        """Point the cloud SDKs at this manager's endpoint.

        Endpoint variables only. Everything else a minicloud run needs
        (instance_provision, ip_ssh_connections, KMS off, iotune off, developer_mode)
        is delivered by the configurations/minicloud.yaml overlay: SCT_* param exports
        made here run after SCTConfiguration is already built, so they never reach
        params — validate_minicloud_params() enforces that the overlay was applied.
        """
        set_minicloud_endpoint_env(f"http://localhost:{self.config.port}", self.backend)
        LOGGER.info("minicloud endpoint env vars applied")

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
            ["docker", "inspect", "-f", "{{.State.Running}}", self.config.container_name],
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
