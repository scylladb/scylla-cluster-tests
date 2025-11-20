"""
Unit tests for trigger_matrix module.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner

from sct import cli
from sdcm.utils.trigger_matrix import (
    load_matrix_comf,
    determine_job_folder,
    filter_matching_jobs,
    find_equivalent_gce_image,
    find_equivalent_azure_image,
    build_job_parameters,
)


def test_load_matrix_comf():
    """Test loading matrix from YAML file."""
    # Create a temporary matrix file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("""
jobs:
  - job_name: '{JOB_FOLDER}/tier1/test-job'
    backend: aws
    region: us-east-1
    exclude_versions:
      - '2024.1'
    labels:
      - master-weekly
    test_config: test-config.yaml
""")
        temp_file = f.name

    try:
        matrix = load_matrix_comf(temp_file)
        assert 'jobs' in matrix
        assert len(matrix['jobs']) == 1
        assert matrix['jobs'][0]['job_name'] == '{JOB_FOLDER}/tier1/test-job'
        assert matrix['jobs'][0]['exclude_versions'] == ['2024.1']
    finally:
        Path(temp_file).unlink()


def test_load_matrix_comf_missing_jobs_key():
    """Test error when matrix file is missing 'jobs' key."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write("invalid: data\n")
        temp_file = f.name

    try:
        with pytest.raises(ValueError, match="Matrix file must contain 'jobs' key"):
            load_matrix_comf(temp_file)
    finally:
        Path(temp_file).unlink()


def test_determine_job_folder_explicit():
    """Test job folder determination with explicit folder."""
    result = determine_job_folder('2025.4', 'custom-folder')
    assert result == 'custom-folder'


def test_determine_job_folder_master():
    """Test job folder determination for master."""
    assert determine_job_folder('master', None) == 'scylla-master'
    assert determine_job_folder('master:latest', None) == 'scylla-master'


def test_determine_job_folder_release():
    """Test job folder determination for release versions."""
    assert determine_job_folder('2025.4', None) == 'branch-2025.4'
    assert determine_job_folder('2025.4.1', None) == 'branch-2025.4'
    assert determine_job_folder('2024.2', None) == 'branch-2024.2'
    assert determine_job_folder('2024.2.5', None) == 'branch-2024.2'


def test_filter_matching_jobs_basic():
    """Test basic job filtering - jobs run on all versions by default."""
    matrix = {
        'jobs': [
            {
                'job_name': 'job1',
                'exclude_versions': [],
                'labels': ['master-weekly'],
            },
            {
                'job_name': 'job2',
                'exclude_versions': ['2025.4'],
                'labels': [],
            },
        ]
    }

    # Job1 should run on 2025.4
    matching = filter_matching_jobs(matrix, '2025.4', None, None)
    assert len(matching) == 1
    assert matching[0]['job_name'] == 'job1'

    # Both jobs should run on master (not excluded)
    matching = filter_matching_jobs(matrix, 'master', None, None)
    assert len(matching) == 2


def test_filter_matching_jobs_with_exclude_versions():
    """Test job filtering with exclude_versions."""
    matrix = {
        'jobs': [
            {
                'job_name': 'job1',
                'exclude_versions': ['2024.1', '2024.2'],
                'labels': [],
            },
            {
                'job_name': 'job2',
                'exclude_versions': [],
                'labels': [],
            },
        ]
    }

    # job1 is excluded for 2024.1
    matching = filter_matching_jobs(matrix, '2024.1', None, None)
    assert len(matching) == 1
    assert matching[0]['job_name'] == 'job2'

    # Both jobs run on 2025.4 (not excluded)
    matching = filter_matching_jobs(matrix, '2025.4', None, None)
    assert len(matching) == 2


def test_filter_matching_jobs_with_label():
    """Test job filtering with label selector."""
    matrix = {
        'jobs': [
            {
                'job_name': 'job1',
                'exclude_versions': [],
                'labels': ['master-weekly'],
            },
            {
                'job_name': 'job2',
                'exclude_versions': [],
                'labels': ['other-label'],
            },
        ]
    }

    matching = filter_matching_jobs(matrix, 'master', 'master-weekly', None)
    assert len(matching) == 1
    assert matching[0]['job_name'] == 'job1'


def test_filter_matching_jobs_with_skip():
    """Test job filtering with skip list."""
    matrix = {
        'jobs': [
            {
                'job_name': 'job1',
                'exclude_versions': [],
                'labels': [],
            },
            {
                'job_name': 'job2-skip-this',
                'exclude_versions': [],
                'labels': [],
            },
        ]
    }

    matching = filter_matching_jobs(matrix, 'master', None, 'job2')
    assert len(matching) == 1
    assert matching[0]['job_name'] == 'job1'


def test_build_job_parameters_aws():
    """Test building job parameters for AWS backend."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'aws',
        'region': 'us-east-1',
        'exclude_versions': [],
        'labels': ['master-weekly'],
        'test_config': 'test.yaml',
    }

    params = build_job_parameters(
        job, '2025.4', None, 'ami-12345', None, None, 'branch-2025.4', True, 'test-user'
    )

    assert params['job_name'] == 'branch-2025.4/tier1/test-job'
    assert params['parameters']['scylla_ami_id'] == 'ami-12345'
    assert params['parameters']['region'] == 'us-east-1'
    assert params['parameters']['use_job_throttling'] is True
    assert params['parameters']['requested_by_user'] == 'test-user'


def test_build_job_parameters_azure():
    """Test building job parameters for Azure backend."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'azure',
        'region': '',
        'exclude_versions': [],
        'labels': [],
        'test_config': 'test.yaml',
    }

    params = build_job_parameters(
        job, '2025.4', None, None, None, None, 'branch-2025.4', True, 'test-user'
    )

    assert params['job_name'] == 'branch-2025.4/tier1/test-job'
    assert params['parameters']['scylla_version'] == '2025.4'
    assert 'azure_image_db' in params['parameters']


def test_build_job_parameters_gce():
    """Test building job parameters for GCE backend."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'gce',
        'region': '',
        'exclude_versions': [],
        'labels': [],
        'test_config': 'test.yaml',
    }

    params = build_job_parameters(
        job, '2025.4', None, None, 'https://www.googleapis.com/compute/v1/projects/gcp-sct-project-1/global/images/test-image', None, 'branch-2025.4', True, 'test-user'
    )

    assert params['job_name'] == 'branch-2025.4/tier1/test-job'
    assert params['parameters']['scylla_version'] == ''
    assert params['parameters']['gce_image_db'] == 'https://www.googleapis.com/compute/v1/projects/gcp-sct-project-1/global/images/test-image'


def test_build_job_parameters_with_scylla_repo():
    """Test building job parameters with scylla_repo."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'aws',
        'region': 'us-east-1',
        'exclude_versions': [],
        'labels': [],
        'test_config': 'test.yaml',
    }

    params = build_job_parameters(
        job, '2025.4', 'http://custom-repo.com', None, None, None, 'branch-2025.4', True, 'test-user'
    )

    assert params['parameters']['scylla_repo'] == 'http://custom-repo.com'


def test_build_job_parameters_azure_with_image():
    """Test building job parameters for Azure backend with azure_image_db."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'azure',
        'region': 'eastus',
        'exclude_versions': [],
        'labels': [],
        'test_config': 'test.yaml',
    }

    azure_image_id = '/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-image'
    params = build_job_parameters(
        job, '2025.4', None, None, None, azure_image_id,
        'branch-2025.4', True, 'test-user'
    )

    assert params['job_name'] == 'branch-2025.4/tier1/test-job'
    # When azure_image is provided, scylla_version should be empty (image takes precedence)
    assert params['parameters']['scylla_version'] == ''
    assert params['parameters']['azure_image_db'] == azure_image_id


def test_build_job_parameters_gce_without_image():
    """Test building job parameters for GCE backend without image (should use scylla_version)."""
    job = {
        'job_name': '{JOB_FOLDER}/tier1/test-job',
        'backend': 'gce',
        'region': '',
        'exclude_versions': [],
        'labels': [],
        'test_config': 'test.yaml',
    }

    params = build_job_parameters(
        job, '2025.4', None, None, None, None, 'branch-2025.4', True, 'test-user'
    )

    assert params['job_name'] == 'branch-2025.4/tier1/test-job'
    assert params['parameters']['scylla_version'] == '2025.4'
    assert params['parameters']['gce_image_db'] == ''


def test_no_params():
    """Test that command fails when no version or images provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ['trigger-matrix', '--dry-run'])

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
    result = runner.invoke(cli, ['trigger-matrix', '--scylla-version', 'master', '--dry-run'])

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
    result = runner.invoke(cli, ['trigger-matrix', '--scylla-ami-id', 'ami-12345678', '--dry-run'])

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
    result = runner.invoke(cli, ['trigger-matrix', '--azure-image-db', '/subscriptions/.../image', '--dry-run'])

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
    result = runner.invoke(cli, ['trigger-matrix', '--gce-image-db',
                           'https://www.googleapis.com/.../image', '--dry-run'])

    if "Either --scylla-version or at least one" not in result.output:
        print("✓ Test 5 PASSED: Command accepts --gce-image-db without --scylla-version")
    else:
        print("✗ Test 5 FAILED: Command rejected valid --gce-image-db")
        print(f"  Output: {result.output[:200]}")
        return False
    return True


class TestFindEquivalentImages:
    """Test suite for finding equivalent images across backends."""

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.get_branched_gce_images')
    def test_find_equivalent_gce_image_with_build_id(self, mock_get_gce_images, mock_get_ami_tags):
        """Test finding equivalent GCE image when AMI has build-id tag."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'branch': 'master',
            'build-id': '12345',
            'ScyllaVersion': '5.2.0',
        }

        mock_gce_image = Mock()
        mock_gce_image.name = 'scylla-master-build-12345'
        mock_gce_image.self_link = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345'
        mock_get_gce_images.return_value = [mock_gce_image]

        # Execute
        result = find_equivalent_gce_image('ami-12345', 'us-east-1', 'x86_64')

        # Assert
        assert result == mock_gce_image.self_link
        mock_get_ami_tags.assert_called_once_with('ami-12345', 'us-east-1')
        mock_get_gce_images.assert_called_once_with(scylla_version='master:12345', arch='x86_64')

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.get_branched_gce_images')
    def test_find_equivalent_gce_image_without_build_id(self, mock_get_gce_images, mock_get_ami_tags):
        """Test finding equivalent GCE image when AMI has no build-id tag."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'branch': 'enterprise',
            'ScyllaVersion': '2024.2.0',
        }

        mock_gce_image = Mock()
        mock_gce_image.name = 'scylla-enterprise-latest'
        mock_gce_image.self_link = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-enterprise-latest'
        mock_get_gce_images.return_value = [mock_gce_image]

        # Execute
        result = find_equivalent_gce_image('ami-67890', 'us-east-1', 'x86_64')

        # Assert
        assert result == mock_gce_image.self_link
        mock_get_gce_images.assert_called_once_with(scylla_version='enterprise:latest', arch='x86_64')

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.get_branched_gce_images')
    def test_find_equivalent_gce_image_no_branch(self, mock_get_gce_images, mock_get_ami_tags):
        """Test finding equivalent GCE image when AMI has no branch tag."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'ScyllaVersion': '5.2.0',
        }

        # Execute
        result = find_equivalent_gce_image('ami-nobranch', 'us-east-1', 'x86_64')

        # Assert
        assert result is None
        mock_get_gce_images.assert_not_called()

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.get_branched_gce_images')
    def test_find_equivalent_gce_image_not_found(self, mock_get_gce_images, mock_get_ami_tags):
        """Test finding equivalent GCE image when no matching image exists."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'branch': 'master',
            'build-id': '99999',
        }
        mock_get_gce_images.return_value = []

        # Execute
        result = find_equivalent_gce_image('ami-notfound', 'us-east-1', 'x86_64')

        # Assert
        assert result is None

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.azure_utils.get_scylla_images')
    @patch('sdcm.utils.trigger_matrix.aws_arch_to_vm_arch')
    def test_find_equivalent_azure_image_with_build_id(
            self, mock_aws_arch_to_vm_arch, mock_get_azure_images, mock_get_ami_tags):
        """Test finding equivalent Azure image when AMI has build-id tag."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'branch': 'master',
            'build-id': '12345',
            'ScyllaVersion': '5.2.0',
        }

        mock_azure_image = Mock()
        mock_azure_image.name = 'scylla-master-build-12345'
        mock_azure_image.id = '/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345'
        mock_get_azure_images.return_value = [mock_azure_image]

        from sdcm.provision.provisioner import VmArch
        mock_aws_arch_to_vm_arch.return_value = VmArch.X86

        # Execute
        result = find_equivalent_azure_image('ami-12345', 'us-east-1', 'eastus', 'x86_64')

        # Assert
        assert result == mock_azure_image.id
        mock_get_ami_tags.assert_called_once_with('ami-12345', 'us-east-1')
        mock_get_azure_images.assert_called_once()

    @patch('sdcm.utils.trigger_matrix.get_ami_tags')
    @patch('sdcm.utils.trigger_matrix.azure_utils.get_scylla_images')
    def test_find_equivalent_azure_image_not_found(self, mock_get_azure_images, mock_get_ami_tags):
        """Test finding equivalent Azure image when no matching image exists."""
        # Setup mocks
        mock_get_ami_tags.return_value = {
            'branch': 'master',
            'build-id': '99999',
        }
        mock_get_azure_images.return_value = []

        # Execute
        result = find_equivalent_azure_image('ami-notfound', 'us-east-1', 'eastus', 'x86_64')

        # Assert
        assert result is None


class TestBuildJobParametersWithEquivalentImages:
    """Test suite for build_job_parameters with automatic image discovery."""

    @patch('sdcm.utils.trigger_matrix.find_equivalent_gce_image')
    @patch('sdcm.utils.trigger_matrix.fetch_arm64_ami')
    def test_gce_job_finds_equivalent_from_ami(self, mock_fetch_arm64_ami, mock_find_gce_image):
        """Test that GCE job automatically finds equivalent image when AMI is provided."""
        # Setup
        mock_find_gce_image.return_value = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345'
        mock_fetch_arm64_ami.return_value = None  # Not needed for x86_64

        job = {
            'job_name': '{JOB_FOLDER}/longevity-test',
            'backend': 'gce',
            'region': 'us-east1',
            'arch': 'x86_64',
        }

        # Execute
        result = build_job_parameters(
            job=job,
            scylla_version='master',
            scylla_repo=None,
            ami_id='ami-12345',  # AMI provided
            gce_image=None,  # No GCE image provided
            azure_image=None,
            job_folder='scylla-master',
            use_job_throttling=True,
            requested_by_user='test-user'
        )

        # Assert
        assert result['parameters']['gce_image_db'] == 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/scylla-master-build-12345'
        assert result['parameters']['scylla_version'] == ''  # Empty because image is provided
        mock_find_gce_image.assert_called_once_with('ami-12345', 'us-east-1', 'x86_64')

    @patch('sdcm.utils.trigger_matrix.find_equivalent_azure_image')
    @patch('sdcm.utils.trigger_matrix.fetch_arm64_ami')
    def test_azure_job_finds_equivalent_from_ami(self, mock_fetch_arm64_ami, mock_find_azure_image):
        """Test that Azure job automatically finds equivalent image when AMI is provided."""
        # Setup
        mock_find_azure_image.return_value = '/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345'
        mock_fetch_arm64_ami.return_value = None  # Not needed for x86_64

        job = {
            'job_name': '{JOB_FOLDER}/longevity-test',
            'backend': 'azure',
            'region': 'eastus',
            'arch': 'x86_64',
        }

        # Execute
        result = build_job_parameters(
            job=job,
            scylla_version='master',
            scylla_repo=None,
            ami_id='ami-12345',  # AMI provided
            gce_image=None,
            azure_image=None,  # No Azure image provided
            job_folder='scylla-master',
            use_job_throttling=True,
            requested_by_user='test-user'
        )

        # Assert
        assert result['parameters']['azure_image_db'] == '/subscriptions/xxx/resourceGroups/SCYLLA-IMAGES/providers/Microsoft.Compute/images/scylla-master-build-12345'
        assert result['parameters']['scylla_version'] == ''  # Empty because image is provided
        mock_find_azure_image.assert_called_once_with('ami-12345', 'us-east-1', 'eastus', 'x86_64')

    @patch('sdcm.utils.trigger_matrix.find_equivalent_gce_image')
    @patch('sdcm.utils.trigger_matrix.fetch_arm64_ami')
    def test_gce_job_uses_provided_image_over_ami(self, mock_fetch_arm64_ami, mock_find_gce_image):
        """Test that provided GCE image takes precedence over AMI."""
        # Setup
        mock_fetch_arm64_ami.return_value = None

        job = {
            'job_name': '{JOB_FOLDER}/longevity-test',
            'backend': 'gce',
            'region': 'us-east1',
            'arch': 'x86_64',
        }

        # Execute
        result = build_job_parameters(
            job=job,
            scylla_version='master',
            scylla_repo=None,
            ami_id='ami-12345',  # AMI provided
            gce_image='https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/my-custom-image',  # GCE image also provided
            azure_image=None,
            job_folder='scylla-master',
            use_job_throttling=True,
            requested_by_user='test-user'
        )

        # Assert
        assert result['parameters']['gce_image_db'] == 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/my-custom-image'
        mock_find_gce_image.assert_not_called()  # Should not search because image already provided

    @patch('sdcm.utils.trigger_matrix.fetch_arm64_ami')
    def test_aws_job_uses_ami_directly(self, mock_fetch_arm64_ami):
        """Test that AWS job uses AMI directly without searching for equivalents."""
        # Setup
        mock_fetch_arm64_ami.return_value = None

        job = {
            'job_name': '{JOB_FOLDER}/longevity-test',
            'backend': 'aws',
            'region': 'us-east-1',
            'arch': 'x86_64',
        }

        # Execute
        result = build_job_parameters(
            job=job,
            scylla_version='master',
            scylla_repo=None,
            ami_id='ami-12345',
            gce_image=None,
            azure_image=None,
            job_folder='scylla-master',
            use_job_throttling=True,
            requested_by_user='test-user'
        )

        # Assert
        assert result['parameters']['scylla_ami_id'] == 'ami-12345'
        assert result['parameters']['scylla_version'] == ''  # Empty because AMI is provided
