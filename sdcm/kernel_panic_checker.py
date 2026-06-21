import logging
import os
import socket
import threading
from abc import abstractmethod
from contextlib import contextmanager
from typing import List

import boto3
import oci
import requests

from sdcm.sct_events.system import KernelPanicEvent

LOGGER = logging.getLogger(__name__)


class BaseKernelPanicChecker(threading.Thread):
    """Base class for cloud provider kernel panic checkers.

    Provides common functionality for monitoring cloud instances for kernel panics
    via provider-specific mechanisms. Each cloud provider should subclass this and
    implement the `_get_console_output()` method.
    """

    CHECK_INTERVAL_SECONDS = 30
    SSH_CHECK_PORT = 22
    SSH_CONNECT_TIMEOUT = 5
    SSH_FAILURE_THRESHOLD = 3

    provider_name: str = "unknown"

    def __init__(self, node_name: str, host: str | None = None, logdir: str | None = None):
        super().__init__()
        self.node_name = node_name
        self.host = host
        self.logdir = logdir
        self._stop_event = threading.Event()
        self._panic_detected = threading.Event()
        self._suspended = threading.Event()
        self._ssh_was_reachable = False
        self._ssh_consecutive_failures = 0
        self.daemon = True

    @abstractmethod
    def _get_console_output(self) -> str: ...

    def _extract_panic_lines(self, output: str) -> List[str]:
        panic_lines = []
        for line in output.splitlines():
            line_lower = line.lower()
            if "kernel panic - not syncing" in line_lower or "kernel panic" in line_lower:
                panic_lines.append(line.strip())
        return panic_lines

    def _check_for_panic(self, output: str) -> bool:
        output_lower = output.lower()
        return "kernel panic" in output_lower

    def _check_ssh_connectivity(self) -> bool:
        """Check if SSH port is reachable via TCP connect.

        Returns True if port 22 is open, False otherwise.
        """
        if not self.host:
            return True
        try:
            with socket.create_connection((self.host, self.SSH_CHECK_PORT), timeout=self.SSH_CONNECT_TIMEOUT):
                return True
        except (OSError, TimeoutError):
            return False

    def _publish_panic_event(self, output: str, instance_identifier: str):
        panic_lines = self._extract_panic_lines(output)
        panic_text = " | ".join(panic_lines) if panic_lines else "Kernel panic detected"
        message = f"Kernel panic detected in console log for {instance_identifier}: {panic_text}"

        LOGGER.error("[%s] %s", self.provider_name, message)
        LOGGER.error("[%s] Full console output for %s:\n%s", self.provider_name, instance_identifier, output)

        KernelPanicEvent(node=self.node_name, message=message).publish()

    def _save_console_output(self, output: str):
        """Save console output to a log file in the node's logdir."""
        if not self.logdir or not output:
            return
        try:
            os.makedirs(self.logdir, exist_ok=True)
            log_path = os.path.join(self.logdir, "console_output.log")
            with open(log_path, "w", encoding="utf-8") as fobj:
                fobj.write(output)
            LOGGER.debug("[%s] Saved console output to %s", self.provider_name, log_path)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("[%s] Failed to save console output: %s", self.provider_name, exc)

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Skip panic detection while suspended — cloud providers
                # (especially OCI) report the instance as STOPPED during a hard reboot,
                # which would otherwise trigger a false KernelPanicEvent.
                if self._suspended.is_set():
                    self._stop_event.wait(self.CHECK_INTERVAL_SECONDS)
                    continue

                output = self._get_console_output()

                if output:
                    self._save_console_output(output)

                if output and self._check_for_panic(output) and not self._panic_detected.is_set():
                    self._panic_detected.set()
                    self._publish_panic_event(output, self._get_instance_identifier())
                    self._stop_event.set()
                    break

                # SSH connectivity check — detects node death when console API is unreliable
                ssh_ok = self._check_ssh_connectivity()
                if ssh_ok:
                    self._ssh_was_reachable = True
                    self._ssh_consecutive_failures = 0
                elif self._ssh_was_reachable:
                    self._ssh_consecutive_failures += 1
                    if self._ssh_consecutive_failures >= self.SSH_FAILURE_THRESHOLD:
                        LOGGER.warning(
                            "[%s] SSH unreachable for %d consecutive checks on %s",
                            self.provider_name,
                            self._ssh_consecutive_failures,
                            self.host,
                        )

            except Exception as exc:  # noqa: BLE001
                if not self._stop_event.is_set():
                    LOGGER.error("[%s] Error checking for kernel panic: %s", self.provider_name, exc)

            self._stop_event.wait(self.CHECK_INTERVAL_SECONDS)

    @property
    def ssh_lost(self) -> bool:
        return self._ssh_was_reachable and self._ssh_consecutive_failures >= self.SSH_FAILURE_THRESHOLD

    @abstractmethod
    def _get_instance_identifier(self) -> str: ...

    def stop(self):
        """Stop the checker and save final console output."""
        self._stop_event.set()
        try:
            output = self._get_console_output()
            self._save_console_output(output)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("[%s] Failed to get final console output: %s", self.provider_name, exc)

    def suspend(self):
        LOGGER.debug("[%s] Suspending kernel panic detection", self.provider_name)
        self._suspended.set()

    def resume(self):
        LOGGER.debug("[%s] Resuming kernel panic detection", self.provider_name)
        self._suspended.clear()

    @contextmanager
    def suspended(self):
        self.suspend()
        try:
            yield
        finally:
            self.resume()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join(timeout=10)
        if self.is_alive():
            LOGGER.warning("[%s] Kernel panic checker thread did not stop within timeout", self.provider_name)


class AWSKernelPanicChecker(BaseKernelPanicChecker):
    """Monitor AWS EC2 instance for kernel panics via console output."""

    provider_name = "AWS"

    def __init__(
        self,
        node_name: str,
        instance_id: str,
        region: str = "us-east-1",
        host: str | None = None,
        logdir: str | None = None,
    ):
        super().__init__(node_name=node_name, host=host, logdir=logdir)
        self.instance_id = instance_id
        self.region = region
        self.ec2 = boto3.client("ec2", region_name=region)

    def _get_console_output(self) -> str:
        """Get console output from EC2 instance."""
        console = self.ec2.get_console_output(InstanceId=self.instance_id, Latest=True)
        return console.get("Output", "")

    def _get_instance_identifier(self) -> str:
        """Return the EC2 instance ID for logging."""
        return f"instance {self.instance_id}"


class GCPKernelPanicChecker(BaseKernelPanicChecker):
    """Monitor GCE instance for kernel panics via serial port output."""

    provider_name = "GCP"

    def __init__(
        self,
        node_name: str,
        instance_name: str,
        project: str,
        zone: str,
        host: str | None = None,
        logdir: str | None = None,
    ):
        super().__init__(node_name=node_name, host=host, logdir=logdir)
        self.instance_name = instance_name
        self.project = project
        self.zone = zone
        # cyclic import: gce_utils -> provision -> gce_region -> gce_utils
        from sdcm.utils.gce_utils import get_gce_compute_instances_client  # noqa: PLC0415

        self.compute_client, _ = get_gce_compute_instances_client()

    def _get_console_output(self) -> str:
        """Get serial port output from GCE instance."""
        from google.cloud.compute_v1 import GetSerialPortOutputInstanceRequest  # noqa: PLC0415

        request = GetSerialPortOutputInstanceRequest(
            project=self.project, zone=self.zone, instance=self.instance_name, port=1
        )
        response = self.compute_client.get_serial_port_output(request=request)
        return response.contents if hasattr(response, "contents") else ""

    def _get_instance_identifier(self) -> str:
        """Return the GCE instance name for logging."""
        return f"instance {self.instance_name}"


class AzureKernelPanicChecker(BaseKernelPanicChecker):
    """Monitor Azure VM for kernel panics via boot diagnostics."""

    provider_name = "Azure"

    def __init__(
        self,
        node_name: str,
        vm_name: str,
        region: str,
        resource_group: str,
        host: str | None = None,
        logdir: str | None = None,
    ):
        super().__init__(node_name=node_name, host=host, logdir=logdir)
        self.vm_name = vm_name
        self.region = region
        self.resource_group = resource_group
        # cyclic import: azure_utils -> provision -> azure_region -> azure_utils
        from sdcm.utils.azure_utils import AzureService  # noqa: PLC0415

        self.compute_client = AzureService().compute

    def _get_console_output(self) -> str:
        """Get boot diagnostics serial console log from Azure VM."""
        try:
            boot_diagnostics = self.compute_client.virtual_machines.retrieve_boot_diagnostics_data(
                resource_group_name=self.resource_group, vm_name=self.vm_name
            )
        except Exception as exc:
            if "ResourceNotFoundError" in type(exc).__name__ or "OperationNotAllowed" in str(exc):
                LOGGER.debug("[Azure] Cannot retrieve boot diagnostics for %s: %s", self.vm_name, exc)
                return ""
            raise

        serial_log_uri = getattr(boot_diagnostics, "serial_console_log_blob_uri", None)
        if not serial_log_uri:
            return ""

        try:
            response = requests.get(serial_log_uri, timeout=30)
            response.raise_for_status()
            return response.text
        except (requests.RequestException, OSError) as exc:
            LOGGER.debug("[Azure] Error fetching serial console log for %s: %s", self.vm_name, exc)
            return ""

    def _get_instance_identifier(self) -> str:
        """Return the Azure VM name for logging."""
        return f"VM {self.vm_name}"


class OCIKernelPanicChecker(BaseKernelPanicChecker):
    """Monitor OCI instance for kernel panics via console history."""

    provider_name = "OCI"

    def __init__(
        self,
        node_name: str,
        instance_id: str,
        compartment_id: str,
        region: str,
        host: str | None = None,
        logdir: str | None = None,
    ):
        super().__init__(node_name=node_name, host=host, logdir=logdir)
        self.instance_id = instance_id
        self.compartment_id = compartment_id
        self.region = region
        # cyclic import: oci_utils -> provision -> oci_region -> oci_utils
        from sdcm.utils.oci_utils import OciService  # noqa: PLC0415

        self.compute_client = OciService().get_compute_client(region=region)

    def _get_console_output(self) -> str:
        """Get console history content from OCI instance.

        Uses OCI's capture_console_history API to capture and retrieve
        up to 1MB of the most recent serial console data. Also checks
        instance lifecycle state on every iteration — a STOPPED/STOPPING
        state indicates kernel panic (with kernel.panic=0, instance halts
        rather than rebooting).
        """
        # Always check instance state first — this is the most reliable
        # signal for OCI since capture_console_history may succeed with
        # stale pre-crash content or fail silently after a kernel panic.
        state_result = self._check_instance_state()
        if state_result:
            return state_result

        try:
            capture_details = oci.core.models.CaptureConsoleHistoryDetails(instance_id=self.instance_id)
            capture_response = self.compute_client.capture_console_history(capture_details)
            console_history_id = capture_response.data.id

            try:
                oci.wait_until(
                    self.compute_client,
                    self.compute_client.get_console_history(console_history_id),
                    "lifecycle_state",
                    "SUCCEEDED",
                    max_wait_seconds=30,
                )
            except oci.exceptions.MaximumWaitTimeExceeded:
                LOGGER.debug("[OCI] Console history capture timed out for %s", self.instance_id)
                self._cleanup_console_history(console_history_id)
                return ""

            content_response = self.compute_client.get_console_history_content(
                console_history_id, offset=0, length=1048576
            )
            content = content_response.data if content_response.data else ""

            self._cleanup_console_history(console_history_id)

            result = content if isinstance(content, str) else content.decode("utf-8", errors="replace")
            return result

        except oci.exceptions.ServiceError as exc:
            if exc.status == 404:
                LOGGER.debug("[OCI] Instance %s not found, stopping monitoring", self.instance_id)
                self._stop_event.set()
                return ""
            LOGGER.debug("[OCI] Error capturing console history for %s: %s", self.instance_id, exc)
            return ""

        except Exception:  # noqa: BLE001
            return ""

    def _check_instance_state(self) -> str:
        """Check instance lifecycle state to detect kernel panic.

        On OCI with kernel.panic=0, the instance halts (transitions to
        STOPPED) rather than rebooting. This is the most reliable signal
        since console history APIs may return stale data or fail after crash.
        """
        try:
            instance = self.compute_client.get_instance(self.instance_id).data
            LOGGER.debug("[OCI] Instance %s lifecycle_state: %s", self.instance_id, instance.lifecycle_state)
            if instance.lifecycle_state in ("STOPPED", "STOPPING"):
                LOGGER.warning(
                    "[OCI] Instance %s is %s — likely kernel panic", self.instance_id, instance.lifecycle_state
                )
                return "Kernel panic - instance stopped unexpectedly"
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("[OCI] Error checking instance state for %s: %s", self.instance_id, exc)
        return ""

    def _cleanup_console_history(self, console_history_id: str):
        """Delete a console history capture to avoid accumulation."""
        try:
            self.compute_client.delete_console_history(console_history_id)
        except Exception:  # noqa: BLE001
            pass

    def _get_instance_identifier(self) -> str:
        """Return the OCI instance ID for logging."""
        return f"instance {self.instance_id}"
