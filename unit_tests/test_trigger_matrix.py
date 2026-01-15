"""
Unit tests for trigger_matrix module.
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from sct import cli
from sdcm.utils.trigger_matrix import (
    load_matrix_config,
    determine_job_folder,
    filter_matching_jobs,
    find_equivalent_gce_image,
    find_equivalent_azure_image,
    build_job_parameters,
    extract_version_from_images,
    extract_scylla_version_from_ami,
    extract_scylla_version_from_gce_image,
    extract_scylla_version_from_azure_image,
    get_image_backend,
    detect_job_architecture,
    JobConfig,
)
from sdcm.provision.provisioner import VmArch


def test_load_matrix_config(tmp_path: Path):
    """Test loading matrix from YAML file."""
    matrix_file = tmp_path / "matrix.yaml"
    matrix_file.write_text(
        """
jobs:
  - job_name: '{JOB_FOLDER}/tier1/test-job'
    backend: aws
    region: us-east-1
    exclude_versions:
      - '2024.1'
    labels:
      - master-weekly
    test_config: test-config.yaml
"""
    )

    matrix = load_matrix_config(str(matrix_file))
    assert "jobs" in matrix
    assert len(matrix["jobs"]) == 1
    assert matrix["jobs"][0]["job_name"] == "{JOB_FOLDER}/tier1/test-job"
    assert matrix["jobs"][0]["exclude_versions"] == ["2024.1"]


def test_load_matrix_config_missing_jobs_key(tmp_path: Path):
    """Test error when matrix file is missing 'jobs' key."""
    matrix_file = tmp_path / "matrix.yaml"
    matrix_file.write_text("invalid: data\n")

    with pytest.raises(ValueError, match="Matrix file must contain 'jobs' key"):
        load_matrix_config(str(matrix_file))


def test_determine_job_folder_explicit():
    """Test job folder determination with explicit folder."""
    result = determine_job_folder("2025.4", "custom-folder")
    assert result == "custom-folder"


def test_determine_job_folder_master():
    """Test job folder determination for master."""
    assert determine_job_folder("master", None) == "scylla-master"
    assert determine_job_folder("master:latest", None) == "scylla-master"


def test_determine_job_folder_release():
    """Test job folder determination for release versions."""
    assert determine_job_folder("2025.4", None) == "branch-2025.4"
    assert determine_job_folder("2025.4.1", None) == "branch-2025.4"
    assert determine_job_folder("2024.2", None) == "branch-2024.2"
    assert determine_job_folder("2024.2.5", None) == "branch-2024.2"


def test_filter_matching_jobs_basic():
    """Test basic job filtering - jobs run on all versions by default."""
    matrix = {
        "jobs": [
            {
                "job_name": "job1",
                "exclude_versions": [],
                "labels": ["master-weekly"],
            },
            {
                "job_name": "job2",
                "exclude_versions": ["2025.4"],
                "labels": [],
            },
        ]
    }

    # Job1 should run on 2025.4
    matching = filter_matching_jobs(matrix, "2025.4", None, None, None)
    assert len(matching) == 1
    assert matching[0]["job_name"] == "job1"

    # Both jobs should run on master (not excluded)
    matching = filter_matching_jobs(matrix, "master", None, None, None)
    assert len(matching) == 2


def test_filter_matching_jobs_with_exclude_versions():
    """Test job filtering with exclude_versions."""
    matrix = {
        "jobs": [
            {
                "job_name": "job1",
                "exclude_versions": ["2024.1", "2024.2"],
                "labels": [],
            },
            {
                "job_name": "job2",
                "exclude_versions": [],
                "labels": [],
            },
        ]
    }

    # job1 is excluded for 2024.1
    matching = filter_matching_jobs(matrix=matrix, scylla_version="2024.1")
    assert len(matching) == 1
    assert matching[0]["job_name"] == "job2"

    # Both jobs run on 2025.4 (not excluded)
    matching = filter_matching_jobs(matrix=matrix, scylla_version="2025.4")
    assert len(matching) == 2


def test_filter_matching_jobs_with_label():
    """Test job filtering with label selector."""
    matrix = {
        "jobs": [
            {
                "job_name": "job1",
                "exclude_versions": [],
                "labels": ["master-weekly"],
            },
            {
                "job_name": "job2",
                "exclude_versions": [],
                "labels": ["other-label"],
            },
        ]
    }

    matching = filter_matching_jobs(matrix, "master", "master-weekly", None)
    assert len(matching) == 1
    assert matching[0]["job_name"] == "job1"


def test_filter_matching_jobs_with_skip():
    """Test job filtering with skip list."""
    matrix = {
        "jobs": [
            {
                "job_name": "job1",
                "exclude_versions": [],
                "labels": [],
            },
            {
                "job_name": "job2-skip-this",
                "exclude_versions": [],
                "labels": [],
            },
        ]
    }

    matching = filter_matching_jobs(matrix=matrix, scylla_version="master", skip_jobs="job2")
    assert len(matching) == 1
    assert matching[0]["job_name"] == "job1"


def test_build_job_parameters_aws(tmp_path: Path):
    """Test building job parameters for AWS backend."""

    test_file = tmp_path / "test.yaml"
    test_file.touch()
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "aws",
        "region": "us-east-1",
        "exclude_versions": [],
        "labels": ["master-weekly"],
        "test_config": str(test_file),
    }

    params = build_job_parameters(
        job=job,
        scylla_version="2025.4",
        ami_id="ami-12345",
        job_folder="branch-2025.4",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert params["job_name"] == "branch-2025.4/tier1/test-job"
    assert params["parameters"]["scylla_ami_id"] == "ami-12345"
    assert params["parameters"]["region"] == "us-east-1"
    assert params["parameters"]["use_job_throttling"] is True
    assert params["parameters"]["requested_by_user"] == "test-user"


def test_build_job_parameters_azure():
    """Test building job parameters for Azure backend."""
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "azure",
        "region": "",
        "exclude_versions": [],
        "labels": [],
        "test_config": "test.yaml",
    }

    params = build_job_parameters(
        job, scylla_version="2025.4", job_folder="branch-2025.4", use_job_throttling=True, requested_by_user="test-user"
    )

    assert params["job_name"] == "branch-2025.4/tier1/test-job"
    assert params["parameters"]["scylla_version"] == "2025.4"
    assert "azure_image_db" in params["parameters"]


def test_build_job_parameters_gce():
    """Test building job parameters for GCE backend."""
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "gce",
        "region": "",
        "exclude_versions": [],
        "labels": [],
        "test_config": "test.yaml",
    }

    params = build_job_parameters(
        job,
        scylla_version="2025.4",
        gce_image="https://www.googleapis.com/compute/v1/projects/gcp-sct-project-1/global/images/test-image",
        job_folder="branch-2025.4",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert params["job_name"] == "branch-2025.4/tier1/test-job"
    assert params["parameters"]["scylla_version"] == ""
    assert (
        params["parameters"]["gce_image_db"]
        == "https://www.googleapis.com/compute/v1/projects/gcp-sct-project-1/global/images/test-image"
    )


def test_build_job_parameters_with_scylla_repo(tmp_path: Path):
    """Test building job parameters with scylla_repo."""
    test_file = tmp_path / "test.yaml"
    test_file.touch()
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "aws",
        "region": "us-east-1",
        "exclude_versions": [],
        "labels": [],
        "test_config": str(test_file),
    }

    params = build_job_parameters(
        job,
        scylla_version="2025.4",
        scylla_repo="http://custom-repo.com",
        job_folder="branch-2025.4",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert params["parameters"]["scylla_repo"] == "http://custom-repo.com"


def test_build_job_parameters_azure_with_image():
    """Test building job parameters for Azure backend with azure_image_db."""
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "azure",
        "region": "eastus",
        "exclude_versions": [],
        "labels": [],
        "test_config": "test.yaml",
    }

    azure_image_id = (
        "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-image"
    )
    params = build_job_parameters(
        job=job,
        scylla_version="2025.4",
        azure_image=azure_image_id,
        job_folder="branch-2025.4",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert params["job_name"] == "branch-2025.4/tier1/test-job"
    # When azure_image is provided, scylla_version should be empty (image takes precedence)
    assert params["parameters"]["scylla_version"] == ""
    assert params["parameters"]["azure_image_db"] == azure_image_id


def test_build_job_parameters_gce_without_image():
    """Test building job parameters for GCE backend without image (should use scylla_version)."""
    job = {
        "job_name": "{JOB_FOLDER}/tier1/test-job",
        "backend": "gce",
        "region": "",
        "exclude_versions": [],
        "labels": [],
        "test_config": "test.yaml",
    }

    params = build_job_parameters(
        job=job,
        scylla_version="2025.4",
        job_folder="branch-2025.4",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert params["job_name"] == "branch-2025.4/tier1/test-job"
    assert params["parameters"]["scylla_version"] == "2025.4"
    assert params["parameters"]["gce_image_db"] == ""


def test_no_params():
    """Test that command fails when no version or images provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["trigger-matrix", "--dry-run"])

    if result.exit_code != 0:
        print("✓ Test 1 PASSED: Command fails without scylla_version or backend images")
        print(f"  Error message: {result.output[:200]}")
    else:
        print("✗ Test 1 FAILED: Command should fail without parameters")
        return False
    return True


@pytest.mark.integration
def test_with_scylla_version():
    """Test that command works with scylla_version."""
    runner = CliRunner()
    result = runner.invoke(cli, ["trigger-matrix", "--scylla-version", "master", "--dry-run"])

    # Check if validation passes (command should proceed to actual execution)
    if "Either --scylla-version or at least one" not in result.output:
        print("✓ Test 2 PASSED: Command accepts --scylla-version")
    else:
        print("✗ Test 2 FAILED: Command rejected valid --scylla-version")
        print(f"  Output: {result.output[:200]}")
        return False
    return True


@pytest.mark.integration
def test_with_ami_only():
    """Test that command works with only AMI ID."""
    runner = CliRunner()
    result = runner.invoke(cli, ["trigger-matrix", "--scylla-ami-id", "ami-12345678", "--dry-run"])

    if "Either --scylla-version or at least one" not in result.output:
        print("✓ Test 3 PASSED: Command accepts --scylla-ami-id without --scylla-version")
    else:
        print("✗ Test 3 FAILED: Command rejected valid --scylla-ami-id")
        print(f"  Output: {result.output[:200]}")
        return False
    return True


@pytest.mark.integration
def test_with_azure_image_only():
    """Test that command works with only Azure image."""
    runner = CliRunner()
    result = runner.invoke(cli, ["trigger-matrix", "--azure-image-db", "/subscriptions/.../image", "--dry-run"])

    if "Either --scylla-version or at least one" not in result.output:
        print("✓ Test 4 PASSED: Command accepts --azure-image-db without --scylla-version")
    else:
        print("✗ Test 4 FAILED: Command rejected valid --azure-image-db")
        print(f"  Output: {result.output[:200]}")
        return False
    return True


@pytest.mark.integration
def test_with_gce_image_only():
    """Test that command works with only GCE image."""
    runner = CliRunner()
    result = runner.invoke(
        cli, ["trigger-matrix", "--gce-image-db", "https://www.googleapis.com/.../image", "--dry-run"]
    )

    if "Either --scylla-version or at least one" not in result.output:
        print("✓ Test 5 PASSED: Command accepts --gce-image-db without --scylla-version")
    else:
        print("✗ Test 5 FAILED: Command rejected valid --gce-image-db")
        print(f"  Output: {result.output[:200]}")
        return False
    return True


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.get_branched_gce_images")
def test_find_equivalent_gce_image_with_build_id(mock_get_gce_images, mock_get_ami_tags):
    """Find equivalent GCE image when AMI has build-id tag."""
    mock_get_ami_tags.return_value = {
        "branch": "master",
        "build-id": "12345",
        "ScyllaVersion": "5.2.0",
    }

    mock_gce_image = Mock()
    mock_gce_image.name = "scylla-master-build-12345"
    mock_gce_image.self_link = (
        "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345"
    )
    mock_get_gce_images.return_value = [mock_gce_image]

    result = find_equivalent_gce_image("ami-12345", "us-east-1", "x86_64")

    assert result == mock_gce_image.self_link
    mock_get_ami_tags.assert_called_once_with("ami-12345", "us-east-1")
    mock_get_gce_images.assert_called_once_with(scylla_version="master:12345", arch="x86_64")


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.get_branched_gce_images")
def test_find_equivalent_gce_image_without_build_id(mock_get_gce_images, mock_get_ami_tags):
    """Use latest when AMI lacks build-id tag."""
    mock_get_ami_tags.return_value = {
        "branch": "enterprise",
        "ScyllaVersion": "2024.2.0",
    }

    mock_gce_image = Mock()
    mock_gce_image.name = "scylla-enterprise-latest"
    mock_gce_image.self_link = (
        "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-enterprise-latest"
    )
    mock_get_gce_images.return_value = [mock_gce_image]

    result = find_equivalent_gce_image("ami-67890", "us-east-1", "x86_64")

    assert result == mock_gce_image.self_link
    mock_get_gce_images.assert_called_once_with(scylla_version="enterprise:latest", arch="x86_64")


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.get_branched_gce_images")
def test_find_equivalent_gce_image_no_branch(mock_get_gce_images, mock_get_ami_tags):
    """Return None when AMI branch tag is missing."""
    mock_get_ami_tags.return_value = {"ScyllaVersion": "5.2.0"}

    assert find_equivalent_gce_image("ami-nobranch", "us-east-1", "x86_64") is None
    mock_get_gce_images.assert_not_called()


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.get_branched_gce_images")
def test_find_equivalent_gce_image_not_found(mock_get_gce_images, mock_get_ami_tags):
    """Return None when no matching GCE images are found."""
    mock_get_ami_tags.return_value = {
        "branch": "master",
        "build-id": "99999",
    }
    mock_get_gce_images.return_value = []

    assert find_equivalent_gce_image("ami-notfound", "us-east-1", "x86_64") is None


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.azure_utils.get_scylla_images")
@patch("sdcm.utils.trigger_matrix.aws_arch_to_vm_arch")
def test_find_equivalent_azure_image_with_build_id(mock_aws_arch_to_vm_arch, mock_get_azure_images, mock_get_ami_tags):
    """Find equivalent Azure image when AMI has build-id tag."""
    mock_get_ami_tags.return_value = {
        "branch": "master",
        "build-id": "12345",
        "ScyllaVersion": "5.2.0",
    }

    mock_azure_image = Mock()
    mock_azure_image.name = "scylla-master-build-12345"
    mock_azure_image.id = (
        "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345"
    )
    mock_get_azure_images.return_value = [mock_azure_image]

    mock_aws_arch_to_vm_arch.return_value = VmArch.X86

    result = find_equivalent_azure_image("ami-12345", "us-east-1", "eastus", "x86_64")

    assert result == mock_azure_image.id
    mock_get_ami_tags.assert_called_once_with("ami-12345", "us-east-1")
    mock_get_azure_images.assert_called_once()


@patch("sdcm.utils.trigger_matrix.get_ami_tags")
@patch("sdcm.utils.trigger_matrix.azure_utils.get_scylla_images")
@patch("sdcm.utils.trigger_matrix.aws_arch_to_vm_arch")
def test_find_equivalent_azure_image_not_found(mock_aws_arch_to_vm_arch, mock_get_azure_images, mock_get_ami_tags):
    """Return None when no matching Azure images are found."""
    mock_get_ami_tags.return_value = {
        "branch": "master",
        "build-id": "99999",
    }
    mock_get_azure_images.return_value = []
    mock_aws_arch_to_vm_arch.return_value = VmArch.X86

    assert find_equivalent_azure_image("ami-notfound", "us-east-1", "eastus", "x86_64") is None


@patch("sdcm.utils.trigger_matrix.find_equivalent_gce_image")
@patch("sdcm.utils.trigger_matrix.fetch_arm64_ami")
def test_gce_job_finds_equivalent_from_ami(mock_fetch_arm64_ami, mock_find_gce_image):
    """GCE job should auto-fill equivalent image when only AMI is provided."""
    mock_find_gce_image.return_value = (
        "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345"
    )
    mock_fetch_arm64_ami.return_value = None
    job = {
        "job_name": "{JOB_FOLDER}/longevity-test",
        "backend": "gce",
        "region": "us-east1",
        "arch": "x86_64",
    }

    result = build_job_parameters(
        job=job,
        scylla_version="master",
        scylla_repo=None,
        ami_id="ami-12345",
        gce_image=None,
        azure_image=None,
        job_folder="scylla-master",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert (
        result["parameters"]["gce_image_db"]
        == "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345"
    )
    assert result["parameters"]["scylla_version"] == ""
    mock_find_gce_image.assert_called_once_with("ami-12345", "us-east-1", "x86_64")


@patch("sdcm.utils.trigger_matrix.find_equivalent_azure_image")
@patch("sdcm.utils.trigger_matrix.fetch_arm64_ami")
def test_azure_job_finds_equivalent_from_ami(mock_fetch_arm64_ami, mock_find_azure_image):
    """Azure job should auto-fill equivalent image when only AMI is provided."""
    mock_find_azure_image.return_value = (
        "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345"
    )
    mock_fetch_arm64_ami.return_value = None
    job = {
        "job_name": "{JOB_FOLDER}/longevity-test",
        "backend": "azure",
        "region": "eastus",
        "arch": "x86_64",
    }

    result = build_job_parameters(
        job=job,
        scylla_version="master",
        scylla_repo=None,
        ami_id="ami-12345",
        gce_image=None,
        azure_image=None,
        job_folder="scylla-master",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert (
        result["parameters"]["azure_image_db"]
        == "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345"
    )
    assert result["parameters"]["scylla_version"] == ""
    mock_find_azure_image.assert_called_once_with("ami-12345", "us-east-1", "eastus", "x86_64")


@patch("sdcm.utils.trigger_matrix.find_equivalent_gce_image")
@patch("sdcm.utils.trigger_matrix.fetch_arm64_ami")
def test_gce_job_uses_provided_image_over_ami(mock_fetch_arm64_ami, mock_find_gce_image):
    """Provided GCE image should take precedence over derived ones."""
    mock_fetch_arm64_ami.return_value = None
    job = {
        "job_name": "{JOB_FOLDER}/longevity-test",
        "backend": "gce",
        "region": "us-east1",
        "arch": "x86_64",
    }

    result = build_job_parameters(
        job=job,
        scylla_version="master",
        scylla_repo=None,
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/my-custom-image",
        azure_image=None,
        job_folder="scylla-master",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert (
        result["parameters"]["gce_image_db"]
        == "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/my-custom-image"
    )
    mock_find_gce_image.assert_not_called()


@patch("sdcm.utils.trigger_matrix.fetch_arm64_ami")
def test_aws_job_uses_ami_directly(mock_fetch_arm64_ami):
    """AWS job should use provided AMI directly."""
    mock_fetch_arm64_ami.return_value = None
    job = {
        "job_name": "{JOB_FOLDER}/longevity-test",
        "backend": "aws",
        "region": "us-east-1",
        "arch": "x86_64",
    }

    result = build_job_parameters(
        job=job,
        scylla_version="master",
        scylla_repo=None,
        ami_id="ami-12345",
        gce_image=None,
        azure_image=None,
        job_folder="scylla-master",
        use_job_throttling=True,
        requested_by_user="test-user",
    )

    assert result["parameters"]["scylla_ami_id"] == "ami-12345"
    assert result["parameters"]["scylla_version"] == ""


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
def test_extract_version_from_ami_only(mock_extract_ami):
    """extract_version_from_images returns AMI tag when only AMI is provided."""
    mock_extract_ami.return_value = "5.2.1"

    result = extract_version_from_images(ami_id="ami-12345")

    assert result == "5.2.1"
    mock_extract_ami.assert_called_once_with("ami-12345")


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
def test_extract_version_from_gce_when_ami_fails(mock_extract_gce, mock_extract_ami):
    """Fallback to GCE image when AMI extraction returns None."""
    mock_extract_ami.return_value = None
    mock_extract_gce.return_value = "5.2.1"

    result = extract_version_from_images(
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
    )

    assert result == "5.2.1"
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_called_once()


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_azure_image")
def test_extract_version_from_azure_when_others_fail(mock_extract_azure, mock_extract_gce, mock_extract_ami):
    """Fallback to Azure image when AMI and GCE extractions return None."""
    mock_extract_ami.return_value = None
    mock_extract_gce.return_value = None
    mock_extract_azure.return_value = "5.2.1"

    result = extract_version_from_images(
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
        azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
    )

    assert result == "5.2.1"
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_called_once()
    mock_extract_azure.assert_called_once()


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
def test_extract_version_all_images_fail(mock_extract_ami):
    """Return None when every extraction path fails."""
    mock_extract_ami.return_value = None

    assert extract_version_from_images(ami_id="ami-12345") is None


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
def test_extract_version_stops_at_first_success(mock_extract_ami):
    """Do not call later extractors once a version is found."""
    mock_extract_ami.return_value = "5.2.1"

    with patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image") as mock_extract_gce:
        result = extract_version_from_images(
            ami_id="ami-12345",
            gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
        )

        assert result == "5.2.1"
        mock_extract_gce.assert_not_called()


@patch("sdcm.utils.trigger_matrix.boto3.resource")
@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
def test_extract_version_from_ami_tags_default_region(mock_get_scylla_resource, mock_boto3_resource):
    """Extract version from AMI tags using default region."""
    mock_ami = Mock()
    mock_ami.tags = [
        {"Key": "Name", "Value": "scylla-master-image"},
        {"Key": "ScyllaVersion", "Value": "5.2.1"},
        {"Key": "build-id", "Value": "12345"},
    ]
    mock_ami.load.return_value = None

    primary_ec2 = Mock()
    primary_ec2.Image.return_value = mock_ami
    mock_boto3_resource.return_value = primary_ec2

    mock_get_scylla_resource.return_value = Mock()

    result = extract_scylla_version_from_ami("ami-12345")

    assert result == "5.2.1"
    mock_boto3_resource.assert_called_with("ec2", region_name="us-east-1")


@patch("sdcm.utils.trigger_matrix.boto3.resource")
@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
def test_extract_version_from_ami_tags_custom_region(mock_get_scylla_resource, mock_boto3_resource):
    """Extract version from AMI tags using custom region."""
    mock_ami = Mock()
    mock_ami.tags = [{"Key": "ScyllaVersion", "Value": "2024.2.0"}]
    mock_ami.load.return_value = None

    primary_ec2 = Mock()
    primary_ec2.Image.return_value = mock_ami
    mock_boto3_resource.return_value = primary_ec2

    mock_get_scylla_resource.return_value = Mock()

    result = extract_scylla_version_from_ami("ami-12345", region="eu-west-1")

    assert result == "2024.2.0"
    mock_boto3_resource.assert_called_with("ec2", region_name="eu-west-1")


@patch("sdcm.utils.trigger_matrix.boto3.resource")
@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
def test_extract_version_from_ami_no_scylla_version_tag(mock_get_scylla_resource, mock_boto3_resource):
    """Return None when AMI lacks ScyllaVersion tag."""
    mock_ami = Mock()
    mock_ami.tags = [
        {"Key": "Name", "Value": "scylla-master-image"},
        {"Key": "build-id", "Value": "12345"},
    ]
    mock_ami.load.return_value = None

    primary_ec2 = Mock()
    primary_ec2.Image.return_value = mock_ami
    mock_boto3_resource.return_value = primary_ec2

    mock_get_scylla_resource.return_value = Mock()

    assert extract_scylla_version_from_ami("ami-12345") is None


@patch("sdcm.utils.trigger_matrix.boto3.resource")
@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
def test_extract_version_from_ami_no_tags(mock_get_scylla_resource, mock_boto3_resource):
    """Return None when AMI has no tags."""
    mock_ami = Mock()
    mock_ami.tags = None
    mock_ami.load.return_value = None

    primary_ec2 = Mock()
    primary_ec2.Image.return_value = mock_ami
    mock_boto3_resource.return_value = primary_ec2

    mock_get_scylla_resource.return_value = Mock()

    assert extract_scylla_version_from_ami("ami-12345") is None


@patch("sdcm.utils.trigger_matrix.boto3.resource")
@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
def test_extract_version_from_ami_with_exception(mock_get_scylla_resource, mock_boto3_resource):
    """Return None when boto3.resource raises."""
    mock_boto3_resource.side_effect = ValueError("AWS error")
    mock_get_scylla_resource.return_value = Mock()

    assert extract_scylla_version_from_ami("ami-invalid") is None


@patch("sdcm.utils.trigger_matrix.get_scylla_images_ec2_resource")
@patch("sdcm.utils.trigger_matrix.boto3.resource")
def test_extract_version_fallback_to_scylla_images(mock_boto3_resource, mock_scylla_resource):
    """Fallback to scylla images EC2 resource when primary tags missing."""
    primary_ami = Mock()
    primary_ami.tags = None
    primary_ami.load.return_value = None
    primary_ec2 = Mock()
    primary_ec2.Image.return_value = primary_ami
    mock_boto3_resource.return_value = primary_ec2

    fallback_ami = Mock()
    fallback_ami.tags = [{"Key": "ScyllaVersion", "Value": "5.2.1"}]
    fallback_ami.load.return_value = None
    fallback_ec2 = Mock()
    fallback_ec2.Image.return_value = fallback_ami
    mock_scylla_resource.return_value = fallback_ec2

    assert extract_scylla_version_from_ami("ami-12345") == "5.2.1"
    mock_scylla_resource.assert_called_once_with(region_name="us-east-1")


@patch("sdcm.utils.trigger_matrix.compute_v1.ImagesClient")
def test_extract_version_from_gce_image(mock_images_client_class):
    """Extract version from GCE image labels."""
    mock_image = Mock()
    mock_image.labels = {"scylla_version": "5.2.1"}

    mock_client = Mock()
    mock_client.get.return_value = mock_image
    mock_images_client_class.return_value = mock_client

    result = extract_scylla_version_from_gce_image(
        "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-img"
    )

    assert result == "5.2.1"
    mock_client.get.assert_called_once_with(project="scylla-images", image="scylla-master-img")


@patch("sdcm.utils.trigger_matrix.compute_v1.ImagesClient")
def test_extract_version_from_gce_image_no_labels(mock_images_client_class):
    """Return None when GCE image has no labels."""
    mock_image = Mock()
    mock_image.labels = None

    mock_client = Mock()
    mock_client.get.return_value = mock_image
    mock_images_client_class.return_value = mock_client

    assert (
        extract_scylla_version_from_gce_image(
            "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-img"
        )
        is None
    )


@patch("sdcm.utils.trigger_matrix.compute_v1.ImagesClient")
def test_extract_version_from_gce_image_no_scylla_version_label(mock_images_client_class):
    """Return None when GCE image labels lack scylla_version."""
    mock_image = Mock()
    mock_image.labels = {"project": "scylla", "region": "us-central1"}

    mock_client = Mock()
    mock_client.get.return_value = mock_image
    mock_images_client_class.return_value = mock_client

    assert (
        extract_scylla_version_from_gce_image(
            "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-img"
        )
        is None
    )


def test_extract_version_from_gce_image_invalid_link():
    """Return None for invalid GCE image link."""
    assert extract_scylla_version_from_gce_image("invalid-image-link") is None


@patch("sdcm.utils.trigger_matrix.compute_v1.ImagesClient")
def test_extract_version_from_gce_image_with_exception(mock_images_client_class):
    """Return None when ImagesClient.get raises."""
    mock_client = Mock()
    mock_client.get.side_effect = ValueError("GCE API error")
    mock_images_client_class.return_value = mock_client

    assert (
        extract_scylla_version_from_gce_image(
            "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-img"
        )
        is None
    )


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image(mock_get_image_tags):
    """Extract version from Azure image with lowercase tag."""
    image_id = "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-img"
    mock_get_image_tags.return_value = {"scylla_version": "5.2.1"}

    assert extract_scylla_version_from_azure_image(image_id) == "5.2.1"
    mock_get_image_tags.assert_called_once_with(image_id)


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_uppercase_tag(mock_get_image_tags):
    """Extract version when tag key is capitalized."""
    image_id = "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-img"
    mock_get_image_tags.return_value = {"ScyllaVersion": "5.2.1"}

    assert extract_scylla_version_from_azure_image(image_id) == "5.2.1"


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_gallery(mock_get_image_tags):
    """Extract version from gallery image id."""
    image_id = (
        "/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/SCYLLA-IMAGES/"
        "providers/Microsoft.Compute/galleries/scylladb_dev/images/master/versions/2026.0102.014600"
    )
    mock_get_image_tags.return_value = {"ScyllaVersion": "2024.2.0"}

    assert extract_scylla_version_from_azure_image(image_id) == "2024.2.0"
    mock_get_image_tags.assert_called_once_with(image_id)


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_no_tags(mock_get_image_tags):
    """Return None when Azure image has no tags."""
    image_id = "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-img"
    mock_get_image_tags.return_value = None

    assert extract_scylla_version_from_azure_image(image_id) is None


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_empty_tags(mock_get_image_tags):
    """Return None when Azure image tags dict is empty."""
    image_id = "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-img"
    mock_get_image_tags.return_value = {}

    assert extract_scylla_version_from_azure_image(image_id) is None


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_no_scylla_version_tag(mock_get_image_tags):
    """Return None when Azure tags lack scylla_version key."""
    image_id = "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-img"
    mock_get_image_tags.return_value = {"project": "scylla", "region": "eastus"}

    assert extract_scylla_version_from_azure_image(image_id) is None


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_invalid_id(mock_get_image_tags):
    """Return None when get_image_tags raises for invalid id."""
    mock_get_image_tags.side_effect = ValueError("invalid")

    assert extract_scylla_version_from_azure_image("invalid-azure-id") is None


@patch("sdcm.utils.trigger_matrix.get_image_tags")
def test_extract_version_from_azure_image_with_exception(mock_get_image_tags):
    """Return None when tag fetch raises an exception."""
    mock_get_image_tags.side_effect = ValueError("Azure API error")

    assert (
        extract_scylla_version_from_azure_image(
            "/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-img"
        )
        is None
    )


def test_get_image_backend_aws():
    assert get_image_backend(ami_id="ami-12345") == "aws"


def test_get_image_backend_gce():
    assert (
        get_image_backend(gce_image="https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/img")
        == "gce"
    )


def test_get_image_backend_azure():
    assert (
        get_image_backend(azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img")
        == "azure"
    )


def test_get_image_backend_aws_takes_precedence():
    assert (
        get_image_backend(
            ami_id="ami-12345",
            gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
            azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
        )
        == "aws"
    )


def test_get_image_backend_gce_precedence_over_azure():
    assert (
        get_image_backend(
            gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
            azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
        )
        == "gce"
    )


def test_get_image_backend_no_images():
    assert get_image_backend() is None


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_azure_image")
def test_version_extraction_priority_ami_first(mock_extract_azure, mock_extract_gce, mock_extract_ami):
    mock_extract_ami.return_value = "5.2.1"
    mock_extract_gce.return_value = "5.2.2"
    mock_extract_azure.return_value = "5.2.3"

    result = extract_version_from_images(
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
        azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
    )

    assert result == "5.2.1"
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_not_called()
    mock_extract_azure.assert_not_called()


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_azure_image")
def test_version_extraction_fallback_gce_when_ami_fails(mock_extract_azure, mock_extract_gce, mock_extract_ami):
    mock_extract_ami.return_value = None
    mock_extract_gce.return_value = "5.2.2"
    mock_extract_azure.return_value = "5.2.3"

    result = extract_version_from_images(
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
        azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
    )

    assert result == "5.2.2"
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_called_once()
    mock_extract_azure.assert_not_called()


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_azure_image")
def test_version_extraction_fallback_azure_when_previous_fail(mock_extract_azure, mock_extract_gce, mock_extract_ami):
    mock_extract_ami.return_value = None
    mock_extract_gce.return_value = None
    mock_extract_azure.return_value = "5.2.3"

    result = extract_version_from_images(
        ami_id="ami-12345",
        gce_image="https://www.googleapis.com/compute/v1/projects/proj/global/images/img",
        azure_image="/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Compute/images/img",
    )

    assert result == "5.2.3"
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_called_once()
    mock_extract_azure.assert_called_once()


@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_ami")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_gce_image")
@patch("sdcm.utils.trigger_matrix.extract_scylla_version_from_azure_image")
def test_version_extraction_all_fail_returns_none(mock_extract_azure, mock_extract_gce, mock_extract_ami):
    mock_extract_ami.return_value = None
    mock_extract_gce.return_value = None
    mock_extract_azure.return_value = None

    assert (
        extract_version_from_images(
            ami_id="ami-invalid",
            gce_image="https://invalid.googleapis.com/image",
            azure_image="/invalid/azure/image",
        )
        is None
    )
    mock_extract_ami.assert_called_once()
    mock_extract_gce.assert_called_once()
    mock_extract_azure.assert_called_once()


# Tests for detect_job_architecture function
@patch("sdcm.utils.trigger_matrix.get_arch_from_instance_type")
def test_detect_job_architecture_single_file(mock_get_arch, tmp_path: Path):
    """Test architecture detection from single test_config file."""

    # Create test config file
    test_config = tmp_path / "test-config.yaml"
    test_config.write_text(
        """
instance_type_db: 'i8ge.xlarge'
n_db_nodes: 3
"""
    )

    mock_get_arch.return_value = "arm64"

    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=str(test_config),
        params={},
    )

    arch = detect_job_architecture(job)

    assert arch == "arm64"
    mock_get_arch.assert_called_once_with("i8ge.xlarge", "us-east-1")


@patch("sdcm.utils.trigger_matrix.get_arch_from_instance_type")
def test_detect_job_architecture_multiple_files(mock_get_arch, tmp_path: Path):
    """Test architecture detection from JSON array of multiple config files."""

    # Create base config
    base_config = tmp_path / "base.yaml"
    base_config.write_text(
        """
n_db_nodes: 3
instance_type_db: 'i4i.xlarge'
"""
    )

    # Create overlay config that overrides instance_type_db
    overlay_config = tmp_path / "overlay.yaml"
    overlay_config.write_text(
        """
instance_type_db: 'i8ge.xlarge'
stress_duration: 60
"""
    )

    mock_get_arch.return_value = "arm64"

    # Test config as JSON array
    test_config_str = json.dumps([str(base_config), str(overlay_config)])

    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=test_config_str,
        params={},
    )

    arch = detect_job_architecture(job)

    # Should use instance_type_db from overlay (merged config)
    assert arch == "arm64"
    mock_get_arch.assert_called_once_with("i8ge.xlarge", "us-east-1")


@patch("sdcm.utils.trigger_matrix.get_arch_from_instance_type")
def test_detect_job_architecture_config_merging(mock_get_arch, tmp_path: Path):
    """Test that later config files override earlier ones."""

    base_config = tmp_path / "base.yaml"
    base_config.write_text(
        """
instance_type_db: 'i4i.xlarge'
other_param: 'value1'
"""
    )

    overlay_config = tmp_path / "overlay.yaml"
    overlay_config.write_text(
        """
other_param: 'value2'
"""
    )

    mock_get_arch.return_value = "x86_64"

    test_config_str = json.dumps([str(base_config), str(overlay_config)])

    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=test_config_str,
        params={},
    )

    arch = detect_job_architecture(job)

    # Should still find instance_type_db from base (not overridden in overlay)
    assert arch == "x86_64"
    mock_get_arch.assert_called_once_with("i4i.xlarge", "us-east-1")


def test_detect_job_architecture_missing_file(tmp_path: Path):
    """Test error handling when config file doesn't exist."""
    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=str(tmp_path / "nonexistent.yaml"),
        params={},
    )

    # Should raise RuntimeError for missing file
    with pytest.raises(RuntimeError, match="Test config file not found"):
        detect_job_architecture(job)


def test_detect_job_architecture_no_instance_type(tmp_path: Path):
    """Test behavior when instance_type_db is missing from config."""
    test_config = tmp_path / "test-config.yaml"
    test_config.write_text(
        """
n_db_nodes: 3
# No instance_type_db field
"""
    )

    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=str(test_config),
        params={},
    )

    arch = detect_job_architecture(job)

    # Should return None when instance_type_db is missing
    assert arch is None


def test_detect_job_architecture_no_test_config():
    """Test behavior when job has no test_config."""
    job = JobConfig(
        job_name="test-job",
        backend="aws",
        region="us-east-1",
        exclude_versions=[],
        labels=[],
        test_config=None,
        params={},
    )

    arch = detect_job_architecture(job)

    # Should return None when no test_config
    assert arch is None
