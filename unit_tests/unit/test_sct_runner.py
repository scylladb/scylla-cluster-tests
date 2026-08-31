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
# Copyright (c) 2025 ScyllaDB

import base64
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import yaml

from sdcm.keystore import SSHKey
from sdcm.sct_runner import (
    list_sct_runners,
    clean_sct_runners,
    update_sct_runner_tags,
    SctRunnerInfo,
    AwsSctRunner,
    GceSctRunner,
    AzureSctRunner,
    OciSctRunner,
)

BASE_IMAGE_OCID = "ocid1.image.oc1.iad.aaaaaaaatestimage"


class TestListSctRunners:
    """Test the enhanced list_sct_runners function."""

    def setup_method(self):
        self.aws_runner = SctRunnerInfo(
            sct_runner_class=AwsSctRunner,
            cloud_service_instance=None,
            region_az="us-east-1a",
            instance={"Tags": [{"Key": "RunByUser", "Value": "user1"}], "InstanceId": "i-aws1"},
            instance_name="aws-runner-1",
            public_ips=["1.2.3.4"],
            test_id="test-id-1",
        )

        self.gce_runner = SctRunnerInfo(
            sct_runner_class=GceSctRunner,
            cloud_service_instance=None,
            region_az="us-central1-a",
            instance=MagicMock(metadata=MagicMock()),
            instance_name="gce-runner-1",
            public_ips=["5.6.7.8"],
            test_id="test-id-2",
        )

        self.azure_runner = SctRunnerInfo(
            sct_runner_class=AzureSctRunner,
            cloud_service_instance=None,
            region_az="eastus-1",
            instance=MagicMock(tags={"RunByUser": "user2"}),
            instance_name="azure-runner-1",
            public_ips=["9.10.11.12"],
            test_id="test-id-1",
        )

        self.oci_runner = SctRunnerInfo(
            sct_runner_class=OciSctRunner,
            cloud_service_instance=None,
            region_az="eastus-1",
            instance=MagicMock(tags={"RunByUser": "user2"}),
            instance_name="oci-runner-1",
            public_ips=["9.10.11.15"],
            test_id="test-id-1",
        )

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    def test_list_sct_runners_no_filters(self, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test listing all runners without filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        runners = list_sct_runners(verbose=False)

        assert len(runners) == 4
        assert self.aws_runner in runners
        assert self.gce_runner in runners
        assert self.azure_runner in runners
        assert self.oci_runner in runners

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner._get_runner_user_tag")
    def test_list_sct_runners_filter_by_user(self, mock_get_user_tag, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by user."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        def side_effect(runner_info):
            if runner_info == self.aws_runner:
                return "user1"
            elif runner_info == self.gce_runner:
                return "user1"
            elif runner_info == self.azure_runner:
                return "user2"
            elif runner_info == self.oci_runner:
                return "user2"
            return None

        mock_get_user_tag.side_effect = side_effect

        runners = list_sct_runners(user="user1", verbose=False)

        assert len(runners) == 2
        assert self.aws_runner in runners
        assert self.gce_runner in runners
        assert self.azure_runner not in runners
        assert self.oci_runner not in runners

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    def test_list_sct_runners_filter_by_test_id(self, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by test_id."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        runners = list_sct_runners(test_id="test-id-1", verbose=False)

        assert len(runners) == 3
        assert self.aws_runner in runners
        assert self.gce_runner not in runners
        assert self.azure_runner in runners
        assert self.oci_runner in runners

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    def test_list_sct_runners_filter_by_ip(self, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test filtering runners by IP address."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        runners = list_sct_runners(test_runner_ip="5.6.7.8", verbose=False)

        assert len(runners) == 1
        assert self.aws_runner not in runners
        assert self.gce_runner in runners
        assert self.azure_runner not in runners
        assert self.oci_runner not in runners

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner._get_runner_user_tag")
    def test_list_sct_runners_mixed_filters(self, mock_get_user_tag, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test user and test_id filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        def side_effect(runner_info):
            if runner_info == self.aws_runner:
                return "user1"
            elif runner_info == self.azure_runner:
                return "user1"
            elif runner_info == self.oci_runner:
                return "user1"
            return "other_user"

        mock_get_user_tag.side_effect = side_effect

        runners = list_sct_runners(user="user1", test_id="test-id-1", verbose=False)

        assert len(runners) == 3
        assert self.aws_runner in runners
        assert self.gce_runner not in runners
        assert self.azure_runner in runners
        assert self.oci_runner in runners

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.GceSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners")
    @patch("sdcm.sct_runner.OciSctRunner.list_sct_runners")
    def test_list_sct_runners_empty_results(self, mock_oci, mock_azure, mock_gce, mock_aws):
        """Test no runners match filters."""
        mock_aws.return_value = [self.aws_runner]
        mock_gce.return_value = [self.gce_runner]
        mock_azure.return_value = [self.azure_runner]
        mock_oci.return_value = [self.oci_runner]

        runners = list_sct_runners(test_id="non-existent-test-id", verbose=False)

        assert len(runners) == 0

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    def test_list_sct_runners_backend_filtering_aws(self, mock_aws):
        """Test backend-specific filtering for AWS."""
        mock_aws.return_value = [self.aws_runner]

        with patch("sdcm.sct_runner.GceSctRunner.list_sct_runners") as mock_gce:
            with patch("sdcm.sct_runner.AzureSctRunner.list_sct_runners") as mock_azure:
                runners = list_sct_runners(backend="aws", verbose=False)

                mock_aws.assert_called_once()
                mock_gce.assert_not_called()
                mock_azure.assert_not_called()
                assert len(runners) == 1
                assert self.aws_runner in runners


class TestCleanSctRunners:
    """Test the clean_sct_runners function."""

    def setup_method(self):
        self.mock_runner_with_keep = MagicMock(
            keep="24",
            keep_action="terminate",
            launch_time=datetime.now(timezone.utc),
            public_ips=["1.2.3.4"],
            cloud_provider="aws",
            instance_name="test-runner-1",
            region_az="us-east-1a",
            test_id="test-123",
        )

        self.mock_runner_no_keep = MagicMock(
            keep=None,
            keep_action=None,
            launch_time=datetime.now(timezone.utc),
            public_ips=["5.6.7.8"],
            cloud_provider="gce",
            instance_name="test-runner-2",
            region_az="us-central1-a",
            test_id="test-456",
        )

    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_by_user(self, mock_list_runners):
        """Test cleanup filtered by user."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(test_status="", user="test_user", dry_run=True)
        mock_list_runners.assert_called_once_with(backend=None, test_runner_ip=None, user="test_user", test_id=None)

    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_by_test_id(self, mock_list_runners):
        """Test cleanup filtered by test_id."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(test_status="", test_id="test-id-123", dry_run=True)
        mock_list_runners.assert_called_once_with(backend=None, test_runner_ip=None, user=None, test_id="test-id-123")

    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_mixed_filters(self, mock_list_runners):
        """Test mix of user and test_id filters."""
        mock_list_runners.return_value = [self.mock_runner_no_keep]

        clean_sct_runners(test_status="completed", user="test_user", test_id="test-id-123", backend="aws", dry_run=True)
        mock_list_runners.assert_called_once_with(
            backend="aws", test_runner_ip=None, user="test_user", test_id="test-id-123"
        )

    @patch("sdcm.sct_runner.ssh_run_cmd")
    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_force(self, mock_list_runners, mock_ssh_cmd):
        """Test force cleanup ignoring keep tags."""
        mock_list_runners.return_value = [self.mock_runner_with_keep]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", user="test_user", force=True, dry_run=True)
        mock_list_runners.assert_called_once()

    @patch("sdcm.sct_runner.ssh_run_cmd")
    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_respect_keep_tags(self, mock_list_runners, mock_ssh_cmd):
        """Test numeric keep tags are respected when clean is not forced."""
        # runner with numeric keep value and recent launch (not expired)
        mock_runner = MagicMock(
            keep="120",
            keep_action="terminate",
            launch_time=datetime.now(timezone.utc),
        )

        mock_list_runners.return_value = [mock_runner]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", user="test_user", force=False, dry_run=False)
        mock_runner.terminate.assert_not_called()

    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_no_runners_found(self, mock_list_runners):
        """Test when no runners match filters."""
        mock_list_runners.return_value = []

        clean_sct_runners(test_status="", user="nonexistent_user", dry_run=True)
        mock_list_runners.assert_called_once()

    @patch("sdcm.sct_runner.ssh_run_cmd")
    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_terminates_expired_runner(self, mock_list_runners, mock_ssh_cmd):
        """Test that runner past its numeric keep hours is terminated."""
        mock_runner = MagicMock(
            keep="24",
            keep_action="terminate",
            launch_time=datetime.now(timezone.utc) - timedelta(hours=25),
        )
        mock_list_runners.return_value = [mock_runner]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", force=False, dry_run=False)
        mock_runner.terminate.assert_called_once()

    @patch("sdcm.sct_runner.ssh_run_cmd")
    @patch("sdcm.sct_runner.list_sct_runners")
    def test_clean_sct_runners_skips_runner_without_terminate_action(self, mock_list_runners, mock_ssh_cmd):
        """Test that runner with keep_action != 'terminate' is not terminated."""
        mock_runner = MagicMock(
            keep="120",
            keep_action="none",
            launch_time=datetime.now(timezone.utc),
        )
        mock_list_runners.return_value = [mock_runner]
        mock_ssh_cmd.return_value = MagicMock(stdout="")

        clean_sct_runners(test_status="", force=False, dry_run=False)
        mock_runner.terminate.assert_not_called()


class TestFindRunnerInstance:
    """Test the find-runner-instance logic (list_sct_runners + update_sct_runner_tags orchestration)."""

    @pytest.fixture
    def runner_info(self):
        return SctRunnerInfo(
            sct_runner_class=AwsSctRunner,
            cloud_service_instance=None,
            region_az="us-east-1a",
            instance=MagicMock(),
            instance_name="reuse-runner-1",
            public_ips=["10.0.0.1"],
            test_id="original-test-id",
            keep="120",
            keep_action="terminate",
        )

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    def test_find_runner_by_test_id(self, mock_aws_list, runner_info):
        """Test that list_sct_runners finds a runner by test_id for reuse."""
        mock_aws_list.return_value = [runner_info]

        runners = list_sct_runners(backend="aws", test_id="original-test-id", verbose=False)

        assert len(runners) == 1
        assert runners[0].public_ips == ["10.0.0.1"]
        assert runners[0].instance_name == "reuse-runner-1"

    @patch("sdcm.sct_runner.AwsSctRunner.list_sct_runners")
    def test_find_runner_returns_empty_for_unknown_test_id(self, mock_aws_list, runner_info):
        """Test that no runner is found for an unknown test_id."""
        mock_aws_list.return_value = [runner_info]
        runners = list_sct_runners(backend="aws", test_id="nonexistent-test-id", verbose=False)
        assert len(runners) == 0

    @patch("sdcm.sct_runner.list_sct_runners")
    def test_update_tags_on_reuse(self, mock_list, runner_info):
        """Test that runner keep/keep_action tags can be updated on reuse."""
        mock_list.return_value = [runner_info]
        runner_info.sct_runner_class = MagicMock()

        update_sct_runner_tags(
            backend="aws",
            test_runner_ip="10.0.0.1",
            tags={"keep": "12", "keep_action": "terminate"},
        )

        runner_info.sct_runner_class.set_tags.assert_called_once_with(
            runner_info,
            tags={"keep": "12", "keep_action": "terminate"},
        )

    @pytest.mark.parametrize(
        "elapsed_hours,duration_minutes,expected",
        [
            (48, 360, "60"),
            (2, 120, "10"),
        ],
    )
    def test_keep_tag_calculation_from_duration(self, elapsed_hours, duration_minutes, expected):
        """Test the keep tag value: elapsed_hours + duration_minutes / 60 + 6 hours buffer."""
        assert str(elapsed_hours + int(duration_minutes / 60) + 6) == expected


# --- OCI runner boot volume sizing ---
#
# LaunchInstanceDetails has no root disk field: a bare `image_id' makes OCI inherit the image's own
# size (50G) and silently ignore both `root_disk_size_runner' and `--root-disk-size-gb'. The size has
# to travel in `source_details' instead, and the guest has to grow its filesystem onto it.


@pytest.fixture
def oci_runner():
    """Build a real OciSctRunner with only its cloud boundaries mocked.

    OciService and OciRegion are the network boundary; key_pair reaches the remote keystore.
    Everything else, including the availability-domain mapping, runs for real.
    """
    oci_region = MagicMock(compartment_id="ocid1.compartment.oc1..test")
    oci_region.availability_domains = ["us-ashburn-1-AD-1", "us-ashburn-1-AD-2", "us-ashburn-1-AD-3"]
    ssh_key = SSHKey(name="scylla_test_id_ed25519", public_key=b"ssh-ed25519 AAAATEST", private_key=b"dummy\n")

    with (
        patch("sdcm.sct_runner.OciService"),
        patch("sdcm.sct_runner.OciRegion", return_value=oci_region),
        patch.object(OciSctRunner, "key_pair", ssh_key),
    ):
        yield OciSctRunner(region_name="us-ashburn-1", availability_zone="a", params=None)


@pytest.fixture
def launch_oci_runner(oci_runner):
    """Return a factory that runs _create_instance and gives back the details OCI was asked to launch."""

    def _launch(**kwargs):
        with (
            patch("sdcm.sct_runner.wait_for_instance_state", return_value=MagicMock()),
            patch("sdcm.sct_runner.oci_public_addresses", return_value=["1.2.3.4"]),
        ):
            oci_runner._create_instance(
                instance_type="VM.Standard.E4.Flex-2-8",
                base_image=BASE_IMAGE_OCID,
                tags={"TestId": "test-id"},
                instance_name="sct-runner-test",
                **kwargs,
            )
        return oci_runner.compute_client.launch_instance.call_args.args[0]

    return _launch


@pytest.mark.parametrize(
    "test_duration,root_disk_size_gb,expected_gb",
    [
        pytest.param(600, 0, 80, id="regular-test-gets-default-80g"),
        pytest.param(3 * 24 * 60, 0, 120, id="test-over-1.5-days-gets-extra-40g"),
        pytest.param(600, 250, 250, id="explicit-size-overrides-default"),
    ],
)
def test_create_instance_sizes_boot_volume_from_duration_and_override(
    test_duration, root_disk_size_gb, expected_gb, launch_oci_runner
):
    """Test that the requested root disk size reaches OCI as an explicit boot volume size."""
    details = launch_oci_runner(test_duration=test_duration, root_disk_size_gb=root_disk_size_gb)

    assert details.source_details.boot_volume_size_in_gbs == expected_gb


def test_create_instance_passes_image_through_source_details_only(launch_oci_runner):
    """Test that the image is carried by source_details, leaving the deprecated image_id unset.

    The two are mutually exclusive on LaunchInstanceDetails, so keeping image_id would either be
    rejected by OCI or drop the boot volume size.
    """
    details = launch_oci_runner(test_duration=600)

    assert details.image_id is None
    assert details.source_details.image_id == BASE_IMAGE_OCID
    assert details.source_details.source_type == "image"


def test_create_instance_user_data_grows_root_filesystem(launch_oci_runner):
    """Test that cloud-init is told to grow the root filesystem onto the enlarged boot volume.

    A bigger volume is inert on its own: without growpart the partition keeps the image's size.
    """
    details = launch_oci_runner(test_duration=600)
    cloud_config = yaml.safe_load(base64.b64decode(details.metadata["user_data"]).decode())

    assert cloud_config["growpart"]["mode"] == "auto"
    assert "/" in cloud_config["growpart"]["devices"]
    assert cloud_config["resize_rootfs"] is True
