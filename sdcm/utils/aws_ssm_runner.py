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
AWS Systems Manager (SSM) command runner utility.

This module provides functionality to run commands on EC2 instances using
AWS Systems Manager Run Command, replacing the AWS CLI with boto3 API calls.
"""

import logging
import time
from typing import Optional, Tuple, List
import boto3
from botocore.exceptions import ClientError
from invoke.runners import Result
from invoke.watchers import StreamWatcher

LOGGER = logging.getLogger(__name__)


class SSMCommandRunner:
    """Run commands on EC2 instances using AWS Systems Manager."""

    def __init__(self, region_name: str, instance_id: str):
        """
        Initialize SSM command runner.

        Args:
            region_name: AWS region name where the instance is located
            instance_id: EC2 instance ID
        """
        self.region_name = region_name
        self.instance_id = instance_id
        self.ssm_client = boto3.client('ssm', region_name=region_name)
        self.ec2_client = boto3.client('ec2', region_name=region_name)

    def check_ssm_prerequisites(self) -> Tuple[bool, str]:
        """
        Check if the instance is ready for SSM commands.

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Check EC2 instance state
            instance_data = self.ec2_client.describe_instances(InstanceIds=[self.instance_id])
            instance_status = instance_data['Reservations'][0]['Instances'][0]['State']['Name']

            if instance_status != 'running':
                msg = f"Instance is in state '{instance_status}', must be 'running' for SSM commands"
                LOGGER.warning(msg)
                return False, msg

            LOGGER.debug("Instance %s state check: SUCCESS (Status is '%s')", self.instance_id, instance_status)

            # Check SSM Agent connectivity
            ssm_info = self.ssm_client.describe_instance_information(
                InstanceInformationFilterList=[
                    {
                        'key': 'InstanceIds',
                        'valueSet': [self.instance_id]
                    }
                ]
            )

            info_list = ssm_info.get('InstanceInformationList', [])

            if not info_list:
                msg = ("SSM Agent is NOT reporting in. Check IAM Instance Profile includes "
                       "AmazonSSMManagedInstanceCore policy")
                LOGGER.warning(msg)
                return False, msg

            # Check Agent Ping Status
            ping_status = info_list[0].get('PingStatus')

            if ping_status != 'Online':
                msg = f"SSM Agent status is '{ping_status}', must be 'Online'"
                LOGGER.warning(msg)
                return False, msg

            LOGGER.debug("SSM Agent status for %s: SUCCESS (Status is '%s')", self.instance_id, ping_status)
            return True, "SUCCESS"

        except ClientError as e:
            error_code = e.response['Error']['Code']
            msg = f"AWS client error during SSM prerequisites check: {error_code} - {e}"
            LOGGER.error(msg)
            return False, msg
        except (KeyError, IndexError) as e:
            msg = f"Unexpected response format during SSM prerequisites check: {e}"
            LOGGER.error(msg)
            return False, msg

    def run(
        self,
        cmd: str,
        timeout: Optional[float] = 300,
        ignore_status: bool = False,
        verbose: bool = True,
        log_file: Optional[str] = None,
        retry: int = 1,
        watchers: Optional[List[StreamWatcher]] = None,
        comment: Optional[str] = None
    ) -> Result:
        """
        Run a shell command on an EC2 instance using SSM.

        Args:
            cmd: Shell command to execute
            timeout: Command execution timeout in seconds (default: 300)
            ignore_status: If True, do not raise exception on command failure
            verbose: If True, log command output
            log_file: Optional file path to save command output
            retry: Number of retry attempts (currently not implemented for SSM)
            watchers: Stream watchers (not used for SSM)
            comment: Optional comment for the SSM command

        Returns:
            Result object with stdout, stderr, exited (status code), ok, etc.
        """
        # Check prerequisites first
        prerequisites_ok, prerequisites_msg = self.check_ssm_prerequisites()
        if not prerequisites_ok:
            LOGGER.error("SSM prerequisites check failed for %s: %s", self.instance_id, prerequisites_msg)
            return self._create_result(
                command=cmd,
                stdout='',
                stderr=prerequisites_msg,
                exited=255,
                ignore_status=ignore_status
            )

        try:
            if verbose:
                LOGGER.debug("Sending SSM command to instance %s: %s", self.instance_id, cmd)

            # Send the command
            send_params = {
                'InstanceIds': [self.instance_id],
                'DocumentName': 'AWS-RunShellScript',
                'Parameters': {'commands': [cmd]},
                'TimeoutSeconds': int(timeout) if timeout else 300,
            }
            if comment:
                send_params['Comment'] = comment

            response = self.ssm_client.send_command(**send_params)

            command_id = response['Command']['CommandId']
            if verbose:
                LOGGER.debug("SSM Command ID: %s. Waiting for command execution...", command_id)

            # Poll for command completion
            status = 'Pending'
            invocation_response = None
            max_wait_attempts = int((timeout or 300) / 5) if timeout else 60

            for attempt in range(max_wait_attempts):
                time.sleep(5)

                invocation_response = self.ssm_client.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=self.instance_id
                )

                status = invocation_response['Status']
                if verbose:
                    LOGGER.debug("SSM command %s status (attempt %d/%d): %s",
                                 command_id, attempt + 1, max_wait_attempts, status)

                if status in ['Success', 'Failed', 'Cancelled', 'TimedOut']:
                    break

            # Retrieve the output
            stdout = invocation_response.get('StandardOutputContent', '')
            stderr = invocation_response.get('StandardErrorContent', '')

            # Map SSM status to exit code
            exit_code = 0 if status == 'Success' else 1
            if status == 'TimedOut':
                exit_code = 124
            elif status == 'Cancelled':
                exit_code = 130

            if status != 'Success' and verbose:
                LOGGER.error("SSM command failed (Status: %s). Error: %s", status, stderr)

            # Save to log file if requested
            if log_file:
                try:
                    with open(log_file, 'w', encoding='utf-8') as f:
                        if stdout:
                            f.write(stdout)
                        if stderr:
                            f.write('\n--- STDERR ---\n')
                            f.write(stderr)
                except IOError as e:
                    LOGGER.error("Failed to save SSM output to file %s: %s", log_file, e)

            return self._create_result(
                command=cmd,
                stdout=stdout,
                stderr=stderr,
                exited=exit_code,
                ignore_status=ignore_status
            )

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = f"AWS error during SSM command execution: {error_code} - {e}"
            LOGGER.error(error_msg)
            return self._create_result(
                command=cmd,
                stdout='',
                stderr=error_msg,
                exited=255,
                ignore_status=ignore_status
            )
        except (KeyError, IndexError) as e:
            error_msg = f"Unexpected error during SSM command execution: {e}"
            LOGGER.error(error_msg)
            return self._create_result(
                command=cmd,
                stdout='',
                stderr=str(e),
                exited=255,
                ignore_status=ignore_status
            )

    @staticmethod
    def _create_result(command: str, stdout: str, stderr: str, exited: int, ignore_status: bool = False) -> Result:
        """
        Create a Result object compatible with invoke.runners.Result.

        Args:
            command: The command that was executed
            stdout: Standard output content
            stderr: Standard error content
            exited: Exit code
            ignore_status: If True, do not raise exception on failure

        Returns:
            Result object
        """
        # Create a Result object by manually setting its attributes
        # Result objects are normally created by invoke's Runner, but we need to create one manually
        result = object.__new__(Result)
        result.command = command
        result.stdout = stdout
        result.stderr = stderr
        result.exited = exited
        result.encoding = 'utf-8'
        result.hide = False
        result.pty = False
        result.env = {}

        # Note: 'ok', 'return_code', and 'exit_status' are read-only properties
        # that are computed from 'exited', so we don't need to set them

        return result

    def run_command_and_save_output(
        self,
        command: str,
        local_output_file: str,
        comment: Optional[str] = None,
        timeout: Optional[float] = 300,
        ignore_status: bool = False
    ) -> Result:
        """
        Run a command on an EC2 instance and save its output to a local file.

        This is a convenience wrapper around run() that automatically saves to log_file.

        Args:
            command: Shell command to execute
            local_output_file: Local file path to save the command output
            comment: Optional comment for the SSM command
            timeout: Command execution timeout in seconds
            ignore_status: If True, do not raise exception on command failure

        Returns:
            Result object from the command execution
        """
        return self.run(
            cmd=command,
            timeout=timeout,
            ignore_status=ignore_status,
            verbose=True,
            log_file=local_output_file,
            comment=comment or f'Run command and save to {local_output_file}'
        )
