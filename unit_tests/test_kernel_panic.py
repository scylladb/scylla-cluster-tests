"""
Integration test for kernel panic detection across cloud providers.

This test validates that the kernel panic checker correctly detects and
reports kernel panics on AWS, GCE, and Azure cloud instances.

To run manually:
    pytest unit_tests/test_kernel_panic.py::test_check_kernel_panic -v -s
"""
import logging
import uuid
import time
import threading
import json

import pytest

from sdcm.remote import shell_script_cmd, RemoteCmdRunnerBase
from sdcm.sct_runner import get_sct_runner, list_sct_runners
from sdcm.sct_config import SCTConfiguration
from sdcm.cluster_aws import AWSKernelPanicChecker
from sdcm.cluster_gce import GCPKernelPanicChecker
from sdcm.cluster_azure import AzureKernelPanicChecker

LOGGER = logging.getLogger(__name__)


def wait_for_kernel_panic_event(events, timeout=300):
    """
    Wait for a KernelPanicEvent to be published by reading the raw events log.

    Args:
        events: The EventsUtilsMixin fixture from pytest
        timeout: Maximum time to wait in seconds (default 300)

    Returns:
        True if event was detected, False if timeout occurred
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            # Get raw events log path using the events fixture
            raw_events_log_path = events.get_raw_events_log()

            # Read all events from the raw events log
            with raw_events_log_path.open() as events_file:
                for line in events_file:
                    try:
                        event = json.loads(line)
                        # Check if this is a KernelPanicEvent
                        if event.get("base") == "KernelPanicEvent":
                            LOGGER.info("Kernel panic event detected: %s", event)
                            return True
                    except json.JSONDecodeError:
                        LOGGER.warning("Failed to parse event line: %s", line)
                        continue

        except (FileNotFoundError, IOError) as e:
            # Events log might not exist yet
            LOGGER.debug("Events log not available yet: %s", e)

        time.sleep(5)  # Check every 5 seconds

    LOGGER.warning("Timeout waiting for kernel panic event after %s seconds", timeout)
    return False


def trigger_kernel_panic_delayed(remoter, delay_seconds=60):
    """Trigger kernel panic after a delay to simulate a crash."""
    LOGGER.info("Kernel panic will be triggered in %s seconds...", delay_seconds)
    time.sleep(delay_seconds)

    LOGGER.info("Triggering kernel panic now...")
    cmd = shell_script_cmd("""
    set -e

    # Allow immediate panic
    sysctl -w kernel.panic=0

    # Enable sysrq
    sysctl -w kernel.sysrq=1

    # Trigger kernel panic
    sleep 2
    echo c > /proc/sysrq-trigger
    """)

    try:
        remoter.sudo(cmd, timeout=5, retry=0)
        LOGGER.info("Kernel panic command executed successfully.")
    except Exception as e:  # noqa: BLE001
        LOGGER.info("Expected failure when triggering kernel panic: %s", e)


@pytest.fixture
def create_sct_runner(request, monkeypatch):
    """
    Factory function to create SCT runner based on backend.
    Supports aws, gce, and azure backends.

    Args:
        request: pytest request fixture to get parametrized values
        monkeypatch: pytest monkeypatch fixture

    Yields:
        Tuple containing backend details and runner instance
    """
    # Unpack the parametrized values
    backend, instance_id_attr, location_label = request.param

    test_id = str(uuid.uuid4())
    duration = 60  # 60 minutes

    # Set up environment and SSH transport
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    RemoteCmdRunnerBase.set_default_ssh_transport("libssh2")

    # Create SCT configuration
    sct_config = SCTConfiguration()

    # Backend-specific configuration
    if backend == "aws":
        region = "us-east-1"
        availability_zone = "a"
    elif backend == "gce":
        region = "us-east1"
        availability_zone = "c"
    elif backend == "azure":
        region = "eastus"
        availability_zone = "1"
    else:
        raise ValueError(f"Unsupported backend: {backend}")

    LOGGER.info("Initializing SCT runner for %s in region %s...", backend, region)
    sct_runner = get_sct_runner(
        cloud_provider=backend, region_name=region, availability_zone=availability_zone, params=sct_config
    )

    LOGGER.info("Creating SCT runner instance with test_id=%s...", test_id)
    instance = sct_runner.create_instance(
        instance_type=sct_config.get("instance_type_runner"),
        root_disk_size_gb=sct_config.get("root_disk_size_runner"),
        test_id=test_id,
        test_name=f"kernel-panic-test-{test_id[:8]}",
        test_duration=duration,
    )

    # Get instance details (backend-agnostic)
    instance_identifier = getattr(instance, instance_id_attr)
    runner_ip = sct_runner.get_instance_public_ip(instance=instance)

    # Verify SSH connectivity
    LOGGER.info("Verifying SSH connectivity...")
    remoter = sct_runner.get_remoter(host=runner_ip, connect_timeout=240)
    if remoter.run("true", timeout=200, verbose=False, ignore_status=True).ok:
        LOGGER.info("Successfully connected to the SCT Runner at %s", runner_ip)
    else:
        LOGGER.error("Unable to SSH to %s!", runner_ip)
        pytest.skip(f"Unable to SSH to {runner_ip}")

    yield backend, sct_runner, remoter, instance, instance_identifier, test_id, region, availability_zone

    # Clean up the SCT runner instance
    LOGGER.info("Cleaning up SCT runner with test_id=%s...", test_id)
    sct_runners_list = list_sct_runners(backend=backend, test_id=test_id)

    if sct_runners_list:
        for sct_runner_info in sct_runners_list:
            LOGGER.info("Terminating runner: %s", sct_runner_info)
            sct_runner_info.terminate()
        LOGGER.info("SCT runner cleaned up successfully for backend: %s", backend)
    else:
        LOGGER.warning("No SCT runners found with test_id=%s for backend: %s", test_id, backend)


@pytest.mark.skipif(
    True, reason="Skipping integration test for kernel panic detection, should be run manually as needed."
)
@pytest.mark.integration
@pytest.mark.parametrize(
    "create_sct_runner",
    [
        pytest.param(("aws", "instance_id", "region"), id="aws"),
        pytest.param(("gce", "name", "zone"), id="gce"),
        pytest.param(("azure", "name", "region"), id="azure"),
    ],
    indirect=True,
)
def test_check_kernel_panic(create_sct_runner, events):
    """
    Integration test for kernel panic detection.

    Creates an SCT runner instance, triggers a kernel panic, and verifies
    that the KernelPanicEvent is properly detected and published.

    This test is marked to skip by default and should be run manually:
        pytest unit_tests/test_kernel_panic.py::test_check_kernel_panic[aws] -v -s
        pytest unit_tests/test_kernel_panic.py::test_check_kernel_panic[gce] -v -s
        pytest unit_tests/test_kernel_panic.py::test_check_kernel_panic[azure] -v -s

    Args:
        create_sct_runner: Fixture that creates and manages SCT runner instance
        events: pytest events fixture for event tracking
    """
    # Unpack SCT runner components
    backend, sct_runner, remoter, instance, instance_identifier, test_id, region, availability_zone = (
        create_sct_runner
    )

    if not instance:
        pytest.fail(f"Failed to create SCT runner instance for backend: {backend}")

    # Determine location based on backend
    if backend == "gce":
        location_value = f"{region}-{availability_zone}"
    else:
        location_value = region

    LOGGER.info("SCT runner created successfully for backend: %s", backend)
    LOGGER.info("Instance identifier: %s", instance_identifier)

    # Start background thread to trigger kernel panic after delay
    panic_thread = threading.Thread(target=trigger_kernel_panic_delayed, args=(remoter, 60), daemon=True)
    panic_thread.start()

    # Start kernel panic checker based on backend
    if backend == "aws":
        checker = AWSKernelPanicChecker(node=instance, instance_id=instance_identifier, region=region)
    elif backend == "gce":
        # Note: GCE requires project parameter
        # This might need adjustment based on how sct_runner provides project info
        project = sct_runner.project if hasattr(sct_runner, 'project') else None
        if not project:
            pytest.skip("GCE project information not available")
        checker = GCPKernelPanicChecker(
            node=instance, instance_name=instance_identifier, project=project, zone=location_value
        )
    elif backend == "azure":
        # Get resource group from the provisioner
        resource_group = sct_runner._instance._provisioner.resource_group_name
        checker = AzureKernelPanicChecker(
            node=instance, vm_name=instance_identifier, region=region, resource_group=resource_group
        )
    else:
        pytest.fail(f"Unsupported backend: {backend}")

    # Use context manager to automatically start/stop checker
    with checker:
        LOGGER.info("Started kernel panic checker for instance %s in %s.", instance_identifier, location_value)

        # Wait for kernel panic event or timeout after 300 seconds
        if wait_for_kernel_panic_event(events, timeout=300):
            LOGGER.info("✓ Kernel panic event detected successfully for backend: %s!", backend)
        else:
            pytest.fail(f"No kernel panic event detected within timeout for backend: {backend}")
