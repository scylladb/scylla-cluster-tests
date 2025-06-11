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
# Copyright (c) 2022 ScyllaDB
import logging
import os
import pty
import shlex
import socket
import subprocess
import sys
from functools import singledispatch, cached_property

import click
import questionary
from questionary import Choice
from google.cloud import compute_v1
from azure.mgmt.compute.models import VirtualMachine

from sdcm.cluster import BaseScyllaCluster, BaseCluster, BaseNode
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.azure_utils import AzureService, list_instances_azure
from sdcm.utils.common import (
    list_instances_aws,
    get_free_port,
    list_instances_gce,
    gce_meta_to_dict,
    SSH_KEY_AWS_DEFAULT,
    SSH_KEY_GCE_DEFAULT,
    SSH_KEY_AZURE_DEFAULT,
    download_dir_from_cloud,
)
from sdcm.utils.gce_utils import (
    gce_public_addresses,
    gce_private_addresses,
    get_gce_compute_instances_client,
    gce_set_tags,
)
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


def get_region(instance: dict) -> str:
    return instance.get('Placement').get('AvailabilityZone')[:-1]


@singledispatch
def get_tags(instance) -> dict:
    raise NotImplementedError()


@get_tags.register
def _(instance: dict) -> dict:
    return {i['Key']: i['Value'] for i in instance['Tags']}


@get_tags.register
def _(instance: compute_v1.Instance) -> dict:
    return gce_meta_to_dict(instance.metadata)


@get_tags.register
def _(instance: VirtualMachine) -> dict:
    return instance.tags


@singledispatch
def get_name(instance):
    raise NotImplementedError()


@get_name.register(dict)
def _(instance: dict):
    return get_tags(instance).get('Name')


@get_name.register(compute_v1.Instance)
def _(instance: compute_v1.Instance):
    return instance.name


@get_name.register(VirtualMachine)
def _(instance: VirtualMachine):
    return instance.name


@singledispatch
def get_target_ip(instance):
    raise NotImplementedError()


@get_target_ip.register(dict)
def _(instance: dict):
    return instance['PrivateIpAddress']


@get_target_ip.register(compute_v1.Instance)
def _(instance: compute_v1.Instance):
    return list(gce_private_addresses(instance))[0]


@get_target_ip.register(VirtualMachine)
def _(instance: VirtualMachine):
    return AzureService().get_virtual_machine_ips(virtual_machine=instance).private_ip


def aws_find_bastion_for_instance(instance: dict) -> dict:
    region = get_region(instance)
    tags = {'bastion': 'true'}
    bastions = list_instances_aws(tags, running=True, region_name=region)
    bastions = list(filter(lambda vm: vm['KeyName'] == instance['KeyName'], bastions))
    assert bastions, f"No bastion found for region: {region}"
    return bastions[0]


def gce_get_user_and_ssh_keys(instance: compute_v1.Instance):
    res = [item.value for item in instance.metadata.items if item.key == 'ssh-keys'][0]
    username, ssh_key = res.split(':')
    ssh_key = ssh_key.split(' ')[0:-1]
    return username, ssh_key


def gce_find_bastion_for_instance(instance: compute_v1.Instance) -> compute_v1.Instance:
    tags = {'bastion': 'true'}
    bastions = list_instances_gce(tags, running=True, verbose=False)

    _, instance_key = gce_get_user_and_ssh_keys(instance)
    bastions = [b for b in bastions if gce_get_user_and_ssh_keys(b)[1] == instance_key]
    assert bastions, "No bastion found"

    return bastions[-1]


def azure_find_bastion_for_instance(instance: VirtualMachine) -> VirtualMachine:
    tags = {'bastion': 'true', 'TestId': get_tags(instance).get('TestId')}
    bastions = list_instances_azure(tags, running=True)
    assert bastions, "No bastion found"
    return bastions[0]


def guess_username(instance: dict | compute_v1.Instance | VirtualMachine) -> str:
    user_name = get_tags(instance).get('UserName')
    if isinstance(instance, compute_v1.Instance):
        user_name, _ = gce_get_user_and_ssh_keys(instance)
    if user_name:
        return user_name

    node_type = get_tags(instance).get('NodeType')
    node_type = node_type.lower() if node_type else node_type
    if node_type == 'builder':
        return 'jenkins'
    elif node_type == 'db-cluster':
        return 'scyllaadm'
    else:
        return 'ubuntu'


def get_proxy_command(instance: dict | compute_v1.Instance,
                      force_use_public_ip: bool,
                      strict_host_checking: bool = False) -> [str, str, str]:
    if isinstance(instance, compute_v1.Instance):
        return gce_get_proxy_command(instance,
                                     strict_host_checking=strict_host_checking)
    elif isinstance(instance, VirtualMachine):
        return azure_get_proxy_command(instance,
                                       force_use_public_ip=force_use_public_ip,
                                       strict_host_checking=strict_host_checking)
    else:
        return aws_get_proxy_command(instance=instance,
                                     force_use_public_ip=force_use_public_ip,
                                     strict_host_checking=strict_host_checking)


def get_proxy_connect_info(instance: dict | compute_v1.Instance) -> tuple:
    if isinstance(instance, compute_v1.Instance):
        key = f'~/.ssh/{SSH_KEY_GCE_DEFAULT}'
        bastion = gce_find_bastion_for_instance(instance)
        username, ip = guess_username(bastion), list(gce_public_addresses(bastion))[0]
    else:
        key = f'~/.ssh/{SSH_KEY_AWS_DEFAULT}'
        bastion = aws_find_bastion_for_instance(instance)
        username, ip = guess_username(bastion), bastion["PublicIpAddress"]
    return ip, username, key


def gce_get_proxy_command(instance: compute_v1.Instance, strict_host_checking: bool):
    target_key = f'~/.ssh/{SSH_KEY_GCE_DEFAULT}'
    if "sct-network-only" in instance.tags.items and "sct-allow-public" not in instance.tags.items:
        target_username = guess_username(instance)
        bastion = gce_find_bastion_for_instance(instance)
        bastion_username, bastion_ip = guess_username(bastion), list(gce_public_addresses(bastion))[0]
        target_ip = list(gce_private_addresses(instance))[0]
        strict_host_check = ""
        if not strict_host_checking:
            strict_host_check = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        proxy_command = f'-o ProxyCommand="ssh {strict_host_check} -i {target_key} -W %h:%p {bastion_username}@{bastion_ip}"'
    else:
        target_ip = list(gce_public_addresses(instance))[0]
        proxy_command = ''
        target_username = guess_username(instance)
    return proxy_command, target_ip, target_username, target_key


def azure_get_proxy_command(instance: VirtualMachine, force_use_public_ip: bool, strict_host_checking: bool):
    target_key = f'~/.ssh/{SSH_KEY_AZURE_DEFAULT}'
    if not force_use_public_ip:
        target_username = guess_username(instance)
        bastion = azure_find_bastion_for_instance(instance)
        bastion_username, bastion_ip = guess_username(bastion), AzureService(
        ).get_virtual_machine_ips(virtual_machine=bastion).public_ip
        target_ip = get_target_ip(instance)
        strict_host_check = ""
        if not strict_host_checking:
            strict_host_check = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        proxy_command = f'-o ProxyCommand="ssh {strict_host_check} -i {target_key} -W %h:%p {bastion_username}@{bastion_ip}"'
    else:
        target_ip = AzureService().get_virtual_machine_ips(virtual_machine=instance).public_ip
        proxy_command = ''
        target_username = guess_username(instance)
    return proxy_command, target_ip, target_username, target_key


def aws_get_proxy_command(instance: dict, force_use_public_ip: bool, strict_host_checking: bool = False) -> [str, str, str]:
    aws_region = AwsRegion(get_region(instance))
    target_key = f'~/.ssh/{SSH_KEY_AWS_DEFAULT}'
    if aws_region.sct_vpc.vpc_id == instance["VpcId"] and not force_use_public_ip:
        # if we are the current VPC setup, proxy via bastion needed
        bastion = aws_find_bastion_for_instance(instance)
        bastion_username,  bastion_ip = guess_username(bastion), bastion["PublicIpAddress"]
        target_ip = instance["PrivateIpAddress"]
        strict_host_check = ""
        if not strict_host_checking:
            strict_host_check = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        proxy_command = f'-o ProxyCommand="ssh {strict_host_check} -i {target_key} -W %h:%p {bastion_username}@{bastion_ip}"'
    else:
        # all other older machine/builders, we connect via public address
        target_ip = instance["PublicIpAddress"]
        proxy_command = ''

    target_username = guess_username(instance)
    return proxy_command, target_ip, target_username, target_key


def select_instance(region: str = None, **tags) -> dict | None:
    user = tags.get('user')
    test_id = tags.get('test_id')
    node_name = tags.get('node_name')
    tags = {}
    if user:
        tags.update({"RunByUser": user})
    if test_id:
        tags.update({"TestId": test_id})
    if node_name:
        tags.update({'Name': node_name})
    aws_vms = list_instances_aws(tags, running=True, region_name=region, verbose=False)

    gce_vms = list_instances_gce(tags, running=True, verbose=False)

    azure_vms = list_instances_azure(tags, running=True, verbose=False)

    if len(aws_vms + gce_vms + azure_vms) == 1:
        return (aws_vms + gce_vms + azure_vms)[0]

    if not aws_vms and not gce_vms and not azure_vms:
        click.echo(click.style("Found no matching instances", fg='red'))
        return {}
    # create the question object
    question = questionary.select(
        "Select machine: ",
        choices=[
            Choice(f"aws - {get_tags(vm).get('Name')} - {vm.get('PublicIpAddress')} {vm['PrivateIpAddress']} - {get_region(vm)}",
                   value=vm) for vm in aws_vms
        ] + [
            Choice(f"gce - {vm.name} - {list(gce_public_addresses(vm))[0]} {list(gce_private_addresses(vm))[0]} - {vm.zone.split('/')[-1]}",
                   value=vm) for vm in gce_vms
        ] + [
            Choice(
                f"azure - {vm.name} - {AzureService().get_virtual_machine_ips(virtual_machine=vm).public_ip} {AzureService().get_virtual_machine_ips(virtual_machine=vm).private_ip} - {vm.location + '' if not vm.zones else vm.zones[0]}",
                value=vm) for vm in azure_vms
        ],
        show_selected=True,
    )

    @question.application.key_bindings.add('x', eager=True)
    @question.application.key_bindings.add('q', eager=True)
    def other(event):
        event.app.exit(exception=KeyboardInterrupt, style="class:aborting")

    # prompt the user for an answer
    return question.ask()


def select_instance_group(region: str = None, backends: list | None = None, **tags) -> list:
    user = tags.get('user')
    test_id = tags.get('test_id')
    node_name = tags.get('node_name')
    node_type = tags.get('node_type')
    tags = {}
    if user:
        tags.update({"RunByUser": user})
    if test_id:
        tags.update({"TestId": test_id})
    if node_name:
        tags.update({'Name': node_name})
    if node_type:
        tags.update({'NodeType': node_type})

    backends = backends or ['aws', 'gce']
    aws_vms = []
    gce_vms = []
    azure_vms = []

    if 'aws' in backends:
        aws_vms = list_instances_aws(tags, running=True, region_name=region)

    if 'gce' in backends:
        gce_vms = list_instances_gce(tags, running=True, verbose=False)

    if 'azure' in backends:
        azure_vms = list_instances_azure(tags, running=True, verbose=False)

    if len(aws_vms + gce_vms + azure_vms) == 1:
        return aws_vms + gce_vms + azure_vms

    if not aws_vms and not gce_vms and not azure_vms:
        click.echo(click.style("Found no matching instances", fg='red'))
        return []

    choices = [Choice(
        f"aws - {get_tags(vm).get('Name')} - {vm.get('PublicIpAddress')} {vm['PrivateIpAddress']} - {get_region(vm)}",
        value=vm) for vm in aws_vms
    ] + [
        Choice(
            f"gce - {vm.name} - {list(gce_public_addresses(vm))[0]} {list(gce_private_addresses(vm))[0]} - {vm.zone.split('/')[-1]}",
            value=vm) for vm in gce_vms
    ] + [
        Choice(
            f"azure - {vm.name} - {AzureService().get_virtual_machine_ips(virtual_machine=vm).public_ip} {AzureService().get_virtual_machine_ips(virtual_machine=vm).private_ip} - {vm.location + '' if not vm.zones else vm.zones[0]}",
            value=vm) for vm in azure_vms
    ]
    # create the question object
    question = questionary.checkbox(
        "Select machine: ",
        choices=choices,
    )

    @question.application.key_bindings.add('x', eager=True)
    @question.application.key_bindings.add('q', eager=True)
    def other(event):
        event.app.exit(exception=KeyboardInterrupt, style="class:aborting")

    # prompt the user for an answer
    return question.ask()


@click.command("ssh", help="Connect to any SCT machine on AWS")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-P", "--force-use-public-ip", is_flag=True, show_default=True, default=False,
              help="Force usage of public address")
@click.argument("node_name", required=False)
def ssh(user, test_id, region, force_use_public_ip, node_name):
    assert user or test_id or node_name
    connect_vm = select_instance(region=region, test_id=test_id, user=user, node_name=node_name)

    if connect_vm:
        proxy_command, target_ip, target_username, target_key = get_proxy_command(connect_vm, force_use_public_ip)
        click.echo(click.style(f"ssh into: {get_name(connect_vm)}",
                               fg='green', bold=True))
        rows = os.environ.get("LINES") or subprocess.check_output(['tput', 'lines'], text=True).strip()
        cols = os.environ.get("COLUMNS") or subprocess.check_output(['tput', 'cols'], text=True).strip()
        tty_options = f'stty rows {rows} cols {cols}'
        cmd = (f'bash -c \'{tty_options}; ssh -tt {proxy_command}'
               f' -i {target_key} -o "UserKnownHostsFile=/dev/null" '
               f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 {target_username}@{target_ip} \'')
        click.echo(cmd)
        pty.spawn(shlex.split(cmd))


@click.command("ssh-cmd", context_settings={"ignore_unknown_options": True})
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-P", "--force-use-public-ip", is_flag=True, show_default=True, default=False,
              help="Force usage of public address")
@click.option("-n", "--node", default=None, help="Node name to execute command on")
@click.argument("command", required=True, nargs=-1)
def ssh_cmd(user, test_id, region, force_use_public_ip, node, command):
    command = ' '.join(command)
    output = ssh_run_cmd(node, command, user, test_id, region, force_use_public_ip)
    if output.stderr:
        click.echo(click.style(output.stderr, fg='red'))
    if output.stdout:
        click.echo(output.stdout)
    if not output.returncode == 0:
        click.echo(click.style(f'{output.returncode=}', fg='red', bold=True))
    return output


def ssh_run_cmd(node_name: str, command: str, user: str = None,
                test_id: str = None, region: str = None,
                force_use_public_ip: bool = None) -> subprocess.CompletedProcess | None:
    assert user or test_id or (node_name and command)
    connect_vm = select_instance(region=region, test_id=test_id, user=user, node_name=node_name)
    cmd_out = None

    if connect_vm:
        proxy_command, target_ip, target_username, target_key = get_proxy_command(connect_vm, force_use_public_ip,
                                                                                  strict_host_checking=False)
        click.echo(click.style(f"run command {command} via ssh into: {get_name(connect_vm)}",
                               fg='green', bold=True))

        cmd = (f'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {proxy_command} '
               f'-i {target_key} '
               f' -o ServerAliveInterval=10 {target_username}@{target_ip} '
               f'{command}')
        cmd_out = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
    return cmd_out


@click.command("tunnel", help="Tunnel ports to any SCT machine on AWS")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-p", "--port", default=3000, help="remote port to tunnel")
@click.option("--wait-for-port/--no-wait-for-port", default=True, help="wait for port to be opened")
@click.argument("node_name", required=False)
def tunnel(user, test_id, region, port, wait_for_port, node_name):
    assert user or test_id or node_name
    connect_vm = select_instance(region=region, test_id=test_id, user=user, node_name=node_name)

    if connect_vm:
        if isinstance(connect_vm, compute_v1.Instance):
            target_key = f'~/.ssh/{SSH_KEY_GCE_DEFAULT}'

            bastion = gce_find_bastion_for_instance(connect_vm)
            bastion_username, bastion_ip = guess_username(bastion), list(gce_public_addresses(bastion))[0]
            target_ip = list(gce_private_addresses(connect_vm))[0]
        elif isinstance(connect_vm, VirtualMachine):
            target_key = f'~/.ssh/{SSH_KEY_AZURE_DEFAULT}'
            bastion = azure_find_bastion_for_instance(connect_vm)
            bastion_username, bastion_ip = guess_username(bastion), AzureService(
            ).get_virtual_machine_ips(virtual_machine=bastion).public_ip
            target_ip = get_target_ip(connect_vm)
        else:
            target_key = f'~/.ssh/{SSH_KEY_AWS_DEFAULT}'
            aws_region = AwsRegion(get_region(connect_vm))

            bastion = aws_find_bastion_for_instance(connect_vm)
            bastion_username, bastion_ip = guess_username(bastion), bastion["PublicIpAddress"]
            if aws_region.sct_vpc.vpc_id == connect_vm["VpcId"]:
                target_ip = connect_vm["PrivateIpAddress"]
            else:
                target_ip = connect_vm["PublicIpAddress"]
        click.echo(click.style(f"tunnel into: {get_name(connect_vm)}", fg='green'))
        local_port = get_free_port()
        cmd = f'ssh -i {target_key} -N -L {local_port}:{target_ip}:{port} -o "UserKnownHostsFile=/dev/null" ' \
            f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 {bastion_username}@{bastion_ip}'
        click.echo(cmd)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if wait_for_port:
            @retrying(n=120, sleep_time=10, allowed_exceptions=(OSError,))
            def wait_for_port(host, port):
                click.echo(click.style('.', fg='bright_yellow'), nl=False)
                socket.create_connection((host, port), timeout=1).close()

            click.echo(click.style('checking connectivity .', fg='bright_yellow'), nl=False)
            wait_for_port('127.0.0.1', int(local_port))
            click.echo('')

        if port == 3000:
            click.echo(click.style(f"connect to: http://127.0.0.1:{local_port}", fg='yellow'))
        if port == 22:
            target_username = guess_username(connect_vm)
            click.echo(click.style(
                f"connect to:\nssh -i {target_key} -p {local_port} {target_username}@127.0.0.1", fg='yellow'))

        process.wait()


@click.command("cp", help="copy files")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-P", "--force-use-public-ip", is_flag=True, show_default=True, default=False,
              help="Force usage of public address")
@click.argument("src")
@click.argument("dest")
def copy_cmd(user, test_id, region, force_use_public_ip, src, dest):
    assert user or test_id
    connect_vm = select_instance(region=region, test_id=test_id, user=user)

    if connect_vm:
        proxy_command, target_ip, target_username, target_key = get_proxy_command(connect_vm, force_use_public_ip)
        target = f'{target_username}@{target_ip}:'
        if ':' in src:
            src = target + src.split(':', maxsplit=1)[1]
        elif ':' in dest:
            dest = target + dest.split(':', maxsplit=1)[1]
        else:
            click.echo(click.style("Not [src] nor [dest] has target host in them", fg='red'))
        pty.spawn(shlex.split(f'scp {proxy_command}'
                              f' -i {target_key} -o "UserKnownHostsFile=/dev/null" '
                              f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 -C {src} {dest}'))


@click.command("attach-test-sg", help="Attach test default security group to a group of instances")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-g", "--group-id", default=None, help="GroupId to use, default to create one base on TestId")
def attach_test_sg_cmd(user, test_id, region, group_id):
    assert user or test_id
    instances = select_instance_group(region=region, backends=['aws'], test_id=test_id, user=user)

    for i in instances:
        aws_region: AwsRegion = AwsRegion(region or get_region(i))
        instance = aws_region.resource.Instance(i['InstanceId'])
        click.echo(click.style(f"attaching test SG to {get_name(i)}", fg='green'))
        if group_id:
            group_id_to_add = group_id
        else:
            group_id_to_add = aws_region.provide_sct_test_security_group(get_tags(i).get('TestId', 'N/A')).group_id
        all_sg_ids = list(set([sg['GroupId'] for sg in instance.security_groups] + [group_id_to_add]))
        instance.modify_attribute(Groups=all_sg_ids)


@click.command("gce-allow-public", help="Attach test default security group to a group of instances")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
def gcp_allow_public(user, test_id):
    assert user or test_id
    instances = select_instance_group(backends=['gce'], test_id=test_id, user=user)
    for i in instances:
        instances_client, info = get_gce_compute_instances_client()
        gce_set_tags(instances_client=instances_client,
                     instance=i,
                     new_tags=['sct-allow-public'],
                     project=info['project_id'],
                     zone=i.zone.split('/')[-1])
        click.echo(click.style(f"set netwrok tag 'sct-allow-public' to {get_name(i)}", fg='green'))


@click.command("update-scylla-packages", help="Update ScyllaDB cluster packages ...")
@click.option("--test-id", type=str, help='Find cluster by test-id', required=True)
@click.option(
    "-p", "--packages-location", type=str,
    help='Location where new packages are placed. Can be local directory, s3:// or gs:// urls')
@click.option('--backend', type=str, help='Backend to search nodes in. Defaults to aws', default="aws")
@click.option('-r', '--region', type=str, help="Cloud region to search nodes in", default=None)
def update_scylla_packages(test_id: str, backend: str, region: str, packages_location: str) -> None:
    """Update scylla DB packages on selected nodes."""
    if not (packages_location := packages_location or os.getenv('SCT_UPDATE_DB_PACKAGES')):
        LOGGER.error("No location is provided where new packages are placed")
        sys.exit(1)
    if packages_location.startswith(('s3', 'gs')):
        packages_location = download_dir_from_cloud(packages_location)
    if not packages_location.endswith('/'):
        packages_location += '/'

    instances = select_instance_group(region=region, backends=[backend], test_id=test_id, node_type='scylla-db')
    if not instances:
        LOGGER.error("No scylla DB nodes were selected. Exiting")
        sys.exit(1)

    cluster = ScyllaDBCluster(test_id, backend=backend)
    cluster.init_nodes(instances)
    cluster.params['update_db_packages'] = packages_location
    cluster.update_db_packages(cluster.nodes)


class ScyllaDBCluster(BaseScyllaCluster, BaseCluster):
    """
    Simple composition of cluster classes to get access to update_db_packages
    API of cluster objects
    """
    nodes: list[BaseNode]

    def __init__(self, test_id, *args, **kwargs):
        self.uuid = test_id
        self.nodes = []
        self.params = kwargs.get('params') or SCTConfiguration()
        self.backend = kwargs.get('params', 'aws')
        self.log = logging.getLogger(self.__class__.__name__)
        logging.getLogger("paramiko").setLevel(logging.WARNING)

    @cached_property
    def key_name(self):
        key = SSH_KEY_AWS_DEFAULT if self.backend == 'aws' else SSH_KEY_GCE_DEFAULT
        return f'~/.ssh/{key}'

    def init_nodes(self, instances):
        proxy_host, proxy_user, proxy_key = get_proxy_connect_info(instances[0])
        for instance in instances:
            login_info = {
                'hostname': get_target_ip(instance),
                'user': guess_username(instance),
                'key_file': self.key_name
            }
            node = BaseNode(name=get_name(instance), parent_cluster=self, ssh_login_info=login_info)
            node.set_seed_flag(True)
            RemoteCmdRunnerBase.set_default_ssh_transport('fabric')
            node.remoter = RemoteCmdRunnerBase.create_remoter(
                **node.ssh_login_info, proxy_host=proxy_host, proxy_user=proxy_user, proxy_key=proxy_key)
            self.nodes.append(node)
