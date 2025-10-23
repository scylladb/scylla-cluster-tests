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

"""
Unit tests for AWS SSM Runner module.
"""

import pytest
from unittest.mock import patch, MagicMock, mock_open
from sdcm.utils.aws_ssm_runner import SSMCommandRunner


@pytest.fixture
def region():
    """AWS region fixture."""
    return 'us-east-1'


@pytest.fixture
def instance_id():
    """EC2 instance ID fixture."""
    return 'i-1234567890abcdef0'


@pytest.fixture
def mock_boto_clients():
    """Mock boto3 clients."""
    mock_ssm = MagicMock()
    mock_ec2 = MagicMock()
    return mock_ssm, mock_ec2


@patch('sdcm.utils.aws_ssm_runner.boto3.client')
def test_init(mock_boto_client, region, instance_id):
    """Test SSMCommandRunner initialization."""
    runner = SSMCommandRunner(region, instance_id)

    assert runner.region_name == region
    assert runner.instance_id == instance_id
    assert mock_boto_client.call_count == 2
    mock_boto_client.assert_any_call('ssm', region_name=region)
    mock_boto_client.assert_any_call('ec2', region_name=region)


@patch('sdcm.utils.aws_ssm_runner.boto3.client')
def test_check_ssm_prerequisites_success(mock_boto_client, region, instance_id, mock_boto_clients):
    """Test successful SSM prerequisites check."""
    mock_ssm, mock_ec2 = mock_boto_clients
    mock_boto_client.side_effect = [mock_ssm, mock_ec2]

    # Mock EC2 response
    mock_ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'State': {'Name': 'running'}
            }]
        }]
    }

    # Mock SSM response
    mock_ssm.describe_instance_information.return_value = {
        'InstanceInformationList': [{
            'PingStatus': 'Online'
        }]
    }

    runner = SSMCommandRunner(region, instance_id)
    success, message = runner.check_ssm_prerequisites()

    assert success is True
    assert message == 'SUCCESS'


@patch('sdcm.utils.aws_ssm_runner.boto3.client')
def test_check_ssm_prerequisites_not_running(mock_boto_client, region, instance_id, mock_boto_clients):
    """Test SSM prerequisites check with non-running instance."""
    mock_ssm, mock_ec2 = mock_boto_clients
    mock_boto_client.side_effect = [mock_ssm, mock_ec2]

    # Mock EC2 response with stopped instance
    mock_ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'State': {'Name': 'stopped'}
            }]
        }]
    }

    runner = SSMCommandRunner(region, instance_id)
    success, message = runner.check_ssm_prerequisites()

    assert success is False
    assert 'stopped' in message


@patch('sdcm.utils.aws_ssm_runner.boto3.client')
@patch('sdcm.utils.aws_ssm_runner.time.sleep')
def test_run_success(mock_sleep, mock_boto_client, region, instance_id, mock_boto_clients):
    """Test successful command execution."""
    mock_ssm, mock_ec2 = mock_boto_clients
    mock_boto_client.side_effect = [mock_ssm, mock_ec2]

    # Mock prerequisites check
    mock_ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'State': {'Name': 'running'}
            }]
        }]
    }
    mock_ssm.describe_instance_information.return_value = {
        'InstanceInformationList': [{
            'PingStatus': 'Online'
        }]
    }

    # Mock command execution
    mock_ssm.send_command.return_value = {
        'Command': {'CommandId': 'cmd-123'}
    }
    mock_ssm.get_command_invocation.return_value = {
        'Status': 'Success',
        'StandardOutputContent': 'Hello World',
        'StandardErrorContent': ''
    }

    runner = SSMCommandRunner(region, instance_id)
    result = runner.run(cmd='echo "Hello World"')

    assert result.ok is True
    assert result.stdout == 'Hello World'
    assert result.exited == 0


@patch('sdcm.utils.aws_ssm_runner.boto3.client')
@patch('builtins.open', new_callable=mock_open)
@patch('sdcm.utils.aws_ssm_runner.time.sleep')
def test_run_with_log_file(mock_sleep, mock_file_open, mock_boto_client, region, instance_id, mock_boto_clients):
    """Test command execution with output saved to file."""
    mock_ssm, mock_ec2 = mock_boto_clients
    mock_boto_client.side_effect = [mock_ssm, mock_ec2]

    # Mock prerequisites and command execution
    mock_ec2.describe_instances.return_value = {
        'Reservations': [{
            'Instances': [{
                'State': {'Name': 'running'}
            }]
        }]
    }
    mock_ssm.describe_instance_information.return_value = {
        'InstanceInformationList': [{
            'PingStatus': 'Online'
        }]
    }
    mock_ssm.send_command.return_value = {
        'Command': {'CommandId': 'cmd-123'}
    }
    mock_ssm.get_command_invocation.return_value = {
        'Status': 'Success',
        'StandardOutputContent': 'Test output',
        'StandardErrorContent': ''
    }

    runner = SSMCommandRunner(region, instance_id)
    result = runner.run(
        cmd='echo "Test"',
        log_file='/tmp/output.txt'
    )

    assert result.ok is True
    assert result.stdout == 'Test output'
    mock_file_open.assert_called_once_with('/tmp/output.txt', 'w', encoding='utf-8')
