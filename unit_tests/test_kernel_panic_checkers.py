"""
Unit tests for kernel panic checker classes.

Tests AWSKernelPanicChecker, GCPKernelPanicChecker, and AzureKernelPanicChecker
to ensure they properly detect kernel panics and publish critical events.
"""

import time
import unittest
from unittest.mock import Mock, patch

import pytest

from sdcm.cluster_aws import AWSKernelPanicChecker
from sdcm.cluster_gce import GCPKernelPanicChecker
from sdcm.cluster_azure import AzureKernelPanicChecker


class MockNode:
    """Mock node for testing."""

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name


class TestAWSKernelPanicChecker(unittest.TestCase):
    """Test cases for AWSKernelPanicChecker."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_node = MockNode("test-aws-node")
        self.instance_id = "i-1234567890abcdef0"
        self.region = "us-east-1"

    @patch("sdcm.cluster_aws.boto3")
    def test_initialization(self, mock_boto3):
        """Test that the checker initializes correctly."""
        mock_ec2_client = Mock()
        mock_boto3.client.return_value = mock_ec2_client

        checker = AWSKernelPanicChecker(node=self.mock_node, instance_id=self.instance_id, region=self.region)

        self.assertEqual(checker.node, self.mock_node)
        self.assertEqual(checker.instance_id, self.instance_id)
        self.assertEqual(checker.region, self.region)
        self.assertFalse(checker._panic_detected)
        self.assertTrue(checker.daemon)
        mock_boto3.client.assert_called_once_with("ec2", region_name=self.region)

    @patch("sdcm.cluster_aws.boto3")
    @patch("sdcm.cluster_aws.KernelPanicEvent")
    def test_detects_kernel_panic_in_console(self, mock_event_class, mock_boto3):
        """Test that kernel panic is detected in console output."""
        mock_ec2_client = Mock()
        mock_boto3.client.return_value = mock_ec2_client

        # Mock instance status - OK
        mock_ec2_client.describe_instance_status.return_value = {
            "InstanceStatuses": [{"InstanceStatus": {"Status": "ok"}}]
        }

        # Mock console output with kernel panic
        mock_ec2_client.get_console_output.return_value = {
            "Output": "System boot\nKernel panic - not syncing: Fatal exception\nEnd of log"
        }

        mock_event = Mock()
        mock_event_class.return_value = mock_event

        checker = AWSKernelPanicChecker(node=self.mock_node, instance_id=self.instance_id, region=self.region)

        # Start checker in a thread
        checker.start()

        # Wait for the checker to run at least once
        time.sleep(0.5)

        # Stop the checker
        checker.stop()
        checker.join(timeout=2)

        # Verify panic was detected
        self.assertTrue(checker._panic_detected)
        mock_event_class.assert_called_once()
        call_kwargs = mock_event_class.call_args[1]
        self.assertEqual(call_kwargs["node"], self.mock_node)
        self.assertIn("Kernel panic detected", call_kwargs["message"])
        mock_event.publish.assert_called_once()

    @patch("sdcm.cluster_aws.boto3")
    @patch("sdcm.cluster_aws.KernelPanicEvent")
    def test_no_panic_detected_normal_output(self, mock_event_class, mock_boto3):
        """Test that no event is raised with normal console output."""
        mock_ec2_client = Mock()
        mock_boto3.client.return_value = mock_ec2_client

        # Mock instance status - OK
        mock_ec2_client.describe_instance_status.return_value = {
            "InstanceStatuses": [{"InstanceStatus": {"Status": "ok"}}]
        }

        # Mock console output without kernel panic
        mock_ec2_client.get_console_output.return_value = {"Output": "System boot\nLoading kernel\nSystem ready\n"}

        checker = AWSKernelPanicChecker(node=self.mock_node, instance_id=self.instance_id, region=self.region)

        # Run checker briefly
        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Verify no panic detected
        self.assertFalse(checker._panic_detected)
        mock_event_class.assert_not_called()

    @patch("sdcm.cluster_aws.boto3")
    def test_instance_status_not_ok(self, mock_boto3):
        """Test that instance status changes are logged."""
        mock_ec2_client = Mock()
        mock_boto3.client.return_value = mock_ec2_client

        # Mock instance status - impaired
        mock_ec2_client.describe_instance_status.return_value = {
            "InstanceStatuses": [{"InstanceStatus": {"Status": "impaired"}}]
        }

        mock_ec2_client.get_console_output.return_value = {"Output": ""}

        checker = AWSKernelPanicChecker(node=self.mock_node, instance_id=self.instance_id, region=self.region)

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Should have called describe_instance_status
        mock_ec2_client.describe_instance_status.assert_called()

    @patch("sdcm.cluster_aws.boto3")
    def test_context_manager(self, mock_boto3):
        """Test that the checker works as a context manager."""
        mock_ec2_client = Mock()
        mock_boto3.client.return_value = mock_ec2_client

        mock_ec2_client.describe_instance_status.return_value = {"InstanceStatuses": []}
        mock_ec2_client.get_console_output.return_value = {"Output": ""}

        checker = AWSKernelPanicChecker(node=self.mock_node, instance_id=self.instance_id, region=self.region)

        with checker:
            self.assertTrue(checker.is_alive())

        # Should be stopped after exiting context
        time.sleep(0.5)
        self.assertFalse(checker.is_alive())


class TestGCPKernelPanicChecker(unittest.TestCase):
    """Test cases for GCPKernelPanicChecker."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_node = MockNode("test-gce-node")
        self.instance_name = "test-instance-123"
        self.zone = "us-central1-a"
        self.project_id = "test-project"

    @patch("sdcm.cluster_gce.get_gce_compute_instances_client")
    def test_initialization(self, mock_get_client):
        """Test that the checker initializes correctly."""
        mock_client = Mock()
        mock_get_client.return_value = (mock_client, {"project_id": self.project_id})

        checker = GCPKernelPanicChecker(node=self.mock_node, zone=self.zone, instance_name=self.instance_name)

        self.assertEqual(checker.node, self.mock_node)
        self.assertEqual(checker.instance_name, self.instance_name)
        self.assertEqual(checker.zone, self.zone)
        self.assertEqual(checker.project_id, self.project_id)
        self.assertFalse(checker._panic_detected)
        self.assertTrue(checker.daemon)

    @patch("sdcm.cluster_gce.get_gce_compute_instances_client")
    @patch("sdcm.cluster_gce.KernelPanicEvent")
    def test_detects_kernel_panic_in_serial_output(self, mock_event_class, mock_get_client):
        """Test that kernel panic is detected in serial port output."""
        mock_client = Mock()
        mock_get_client.return_value = (mock_client, {"project_id": self.project_id})

        # Mock instance with RUNNING status
        mock_instance = Mock()
        mock_instance.status = "RUNNING"
        mock_client.get.return_value = mock_instance

        # Mock serial output with kernel panic
        mock_serial_output = Mock()
        mock_serial_output.contents = "Boot log\nKernel panic - not syncing\nStack trace"
        mock_client.get_serial_port_output.return_value = mock_serial_output

        mock_event = Mock()
        mock_event_class.return_value = mock_event

        checker = GCPKernelPanicChecker(node=self.mock_node, zone=self.zone, instance_name=self.instance_name)

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Verify panic was detected
        self.assertTrue(checker._panic_detected)
        mock_event_class.assert_called_once()
        call_kwargs = mock_event_class.call_args[1]
        self.assertEqual(call_kwargs["node"], self.mock_node)
        self.assertIn("Kernel panic detected", call_kwargs["message"])
        mock_event.publish.assert_called_once()

    @patch("sdcm.cluster_gce.get_gce_compute_instances_client")
    @patch("sdcm.cluster_gce.KernelPanicEvent")
    def test_no_panic_detected_normal_serial_output(self, mock_event_class, mock_get_client):
        """Test that no event is raised with normal serial output."""
        mock_client = Mock()
        mock_get_client.return_value = (mock_client, {"project_id": self.project_id})

        mock_instance = Mock()
        mock_instance.status = "RUNNING"
        mock_client.get.return_value = mock_instance

        mock_serial_output = Mock()
        mock_serial_output.contents = "Boot log\nSystem starting\nAll services running"
        mock_client.get_serial_port_output.return_value = mock_serial_output

        checker = GCPKernelPanicChecker(node=self.mock_node, zone=self.zone, instance_name=self.instance_name)

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        self.assertFalse(checker._panic_detected)
        mock_event_class.assert_not_called()

    @patch("sdcm.cluster_gce.get_gce_compute_instances_client")
    def test_instance_status_monitoring(self, mock_get_client):
        """Test that instance status is monitored."""
        mock_client = Mock()
        mock_get_client.return_value = (mock_client, {"project_id": self.project_id})

        # Mock instance with TERMINATED status
        mock_instance = Mock()
        mock_instance.status = "TERMINATED"
        mock_client.get.return_value = mock_instance

        mock_serial_output = Mock()
        mock_serial_output.contents = ""
        mock_client.get_serial_port_output.return_value = mock_serial_output

        checker = GCPKernelPanicChecker(node=self.mock_node, zone=self.zone, instance_name=self.instance_name)

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Should have called get to check status
        mock_client.get.assert_called()

    @patch("sdcm.cluster_gce.get_gce_compute_instances_client")
    def test_context_manager(self, mock_get_client):
        """Test that the checker works as a context manager."""
        mock_client = Mock()
        mock_get_client.return_value = (mock_client, {"project_id": self.project_id})

        mock_instance = Mock()
        mock_instance.status = "RUNNING"
        mock_client.get.return_value = mock_instance

        mock_serial_output = Mock()
        mock_serial_output.contents = ""
        mock_client.get_serial_port_output.return_value = mock_serial_output

        checker = GCPKernelPanicChecker(node=self.mock_node, zone=self.zone, instance_name=self.instance_name)

        with checker:
            self.assertTrue(checker.is_alive())

        time.sleep(0.5)
        self.assertFalse(checker.is_alive())


class TestAzureKernelPanicChecker(unittest.TestCase):
    """Test cases for AzureKernelPanicChecker."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_node = MockNode("test-azure-node")
        self.vm_name = "test-vm-123"
        self.region = "eastus"
        self.resource_group = "test-rg"

    @patch("sdcm.cluster_azure.AzureService")
    def test_initialization(self, mock_azure_service_class):
        """Test that the checker initializes correctly."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_blob = Mock()
        mock_azure_service.blob = mock_blob
        mock_container_client = Mock()
        mock_blob.get_container_client.return_value = mock_container_client

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        self.assertEqual(checker.node, self.mock_node)
        self.assertEqual(checker.vm_name, self.vm_name)
        self.assertEqual(checker.resource_group, self.resource_group)
        self.assertFalse(checker._panic_detected)
        self.assertTrue(checker.daemon)

    @patch("sdcm.cluster_azure.urllib.request.urlopen")
    @patch("sdcm.cluster_azure.AzureService")
    @patch("sdcm.cluster_azure.KernelPanicEvent")
    def test_detects_kernel_panic_in_boot_diagnostics(self, mock_event_class, mock_azure_service_class, mock_urlopen):
        """Test that kernel panic is detected in boot diagnostics."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        # Mock compute client
        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        # Mock instance view
        mock_instance_view = Mock()
        mock_status = Mock()
        mock_status.code = "PowerState/running"
        mock_status.display_status = "VM running"
        mock_instance_view.statuses = [mock_status]
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = "https://storage.blob.core.windows.net/screenshot.png"
        mock_boot_diagnostics.serial_console_log_blob_uri = "https://storage.blob.core.windows.net/serial.log"
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        # Mock urllib.request.urlopen to return kernel panic log
        mock_response = Mock()
        mock_response.read.return_value = b"Boot log\nKernel panic - not syncing: VFS\nEnd"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        mock_event = Mock()
        mock_event_class.return_value = mock_event

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Verify panic was detected
        self.assertTrue(checker._panic_detected)
        mock_event_class.assert_called_once()
        call_kwargs = mock_event_class.call_args[1]
        self.assertEqual(call_kwargs["node"], self.mock_node)
        self.assertIn("Kernel panic detected", call_kwargs["message"])
        mock_event.publish.assert_called_once()

    @patch("sdcm.cluster_azure.urllib.request.urlopen")
    @patch("sdcm.cluster_azure.AzureService")
    @patch("sdcm.cluster_azure.KernelPanicEvent")
    def test_no_panic_detected_normal_logs(self, mock_event_class, mock_azure_service_class, mock_urlopen):
        """Test that no event is raised with normal boot diagnostics."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_instance_view = Mock()
        mock_status = Mock()
        mock_status.code = "PowerState/running"
        mock_status.display_status = "VM running"
        mock_instance_view.statuses = [mock_status]
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = "https://storage.blob.core.windows.net/screenshot.png"
        mock_boot_diagnostics.serial_console_log_blob_uri = "https://storage.blob.core.windows.net/serial.log"
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        # Mock urllib.request.urlopen to return normal log
        mock_response = Mock()
        mock_response.read.return_value = b"Boot log\nSystem starting\nAll OK"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        self.assertFalse(checker._panic_detected)
        mock_event_class.assert_not_called()

    @patch("sdcm.cluster_azure.urllib.request.urlopen")
    @patch("sdcm.cluster_azure.AzureService")
    def test_detects_not_syncing_error(self, mock_azure_service_class, mock_urlopen):
        """Test that 'not syncing' errors are also detected."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_instance_view = Mock()
        mock_status = Mock()
        mock_status.code = "PowerState/running"
        mock_status.display_status = "VM running"
        mock_instance_view.statuses = [mock_status]
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = "https://storage.blob.core.windows.net/screenshot.png"
        mock_boot_diagnostics.serial_console_log_blob_uri = "https://storage.blob.core.windows.net/serial.log"
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        # Test with "not syncing" instead of "kernel panic"
        mock_response = Mock()
        mock_response.read.return_value = b"Error: not syncing: Fatal error"
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)
        mock_urlopen.return_value = mock_response

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        self.assertTrue(checker._panic_detected)

    @patch("sdcm.cluster_azure.AzureService")
    def test_vm_power_state_monitoring(self, mock_azure_service_class):
        """Test that VM power state is monitored."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_instance_view = Mock()
        mock_status = Mock()
        mock_status.code = "PowerState/deallocated"
        mock_status.display_status = "VM deallocated"
        mock_instance_view.statuses = [mock_status]
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data to return None (no boot diagnostics available)
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = None
        mock_boot_diagnostics.serial_console_log_blob_uri = None
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Should have called instance_view
        mock_compute.virtual_machines.instance_view.assert_called()

    @patch("sdcm.cluster_azure.AzureService")
    def test_context_manager(self, mock_azure_service_class):
        """Test that the checker works as a context manager."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_instance_view = Mock()
        mock_instance_view.statuses = []
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data to return None
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = None
        mock_boot_diagnostics.serial_console_log_blob_uri = None
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        with checker:
            self.assertTrue(checker.is_alive())

        time.sleep(0.5)
        self.assertFalse(checker.is_alive())

    @patch("sdcm.cluster_azure.urllib.request.urlopen")
    @patch("sdcm.cluster_azure.AzureService")
    def test_skips_already_processed_blobs(self, mock_azure_service_class, mock_urlopen):
        """Test that already processed blobs are skipped."""
        mock_azure_service = Mock()
        mock_azure_service_class.return_value = mock_azure_service
        mock_azure_service.subscription_id = "test-sub-id"

        mock_compute = Mock()
        mock_azure_service.compute = mock_compute

        mock_instance_view = Mock()
        mock_instance_view.statuses = []
        mock_compute.virtual_machines.instance_view.return_value = mock_instance_view

        # Mock retrieve_boot_diagnostics_data
        serial_log_uri = "https://storage.blob.core.windows.net/serial.log"
        mock_boot_diagnostics = Mock()
        mock_boot_diagnostics.console_screenshot_blob_uri = "https://storage.blob.core.windows.net/screenshot.png"
        mock_boot_diagnostics.serial_console_log_blob_uri = serial_log_uri
        mock_compute.virtual_machines.retrieve_boot_diagnostics_data.return_value = mock_boot_diagnostics

        checker = AzureKernelPanicChecker(
            node=self.mock_node, vm_name=self.vm_name, region=self.region, resource_group=self.resource_group
        )

        # Set last_panic_blob to simulate already processed
        checker.last_panic_blob = serial_log_uri

        checker.start()
        time.sleep(0.5)
        checker.stop()
        checker.join(timeout=2)

        # Should not have tried to download the log via urlopen
        mock_urlopen.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
