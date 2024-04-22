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
import os
import pty
import shlex
import subprocess

import click
import questionary
from questionary import Choice

from sdcm.utils.common import list_instances_aws, get_free_port
from sdcm.utils.aws_region import AwsRegion


def get_region(instance: dict) -> str:
    return instance.get('Placement').get('AvailabilityZone')[:-1]


def get_tags(instance: dict) -> dict:
    return {i['Key']: i['Value'] for i in instance['Tags']}


def find_bastion_for_instance(instance: dict) -> dict:
    region = get_region(instance)
    tags = {'bastion': 'true'}
    bastions = list_instances_aws(tags, running=True, region_name=region)
    assert bastions, f"No bastion found for region: {region}"
    return bastions[0]


def guess_username(instance: dict) -> str:
    user_name = get_tags(instance).get('UserName')
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


def get_proxy_command(instance: dict, force_use_public_ip: bool, strict_host_checking: bool = False) -> [str, str, str]:
    aws_region = AwsRegion(get_region(instance))

    if aws_region.sct_vpc.vpc_id == instance["VpcId"] and not force_use_public_ip:
        # if we are the current VPC setup, proxy via bastion needed
        bastion = find_bastion_for_instance(instance)
        bastion_username,  bastion_ip = guess_username(bastion), bastion["PublicIpAddress"]
        target_ip = instance["PrivateIpAddress"]
        strict_host_check = ""
        if not strict_host_checking:
            strict_host_check = "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        proxy_command = f'-o ProxyCommand="ssh {strict_host_check} -i ~/.ssh/scylla-qa-ec2 -W %h:%p {bastion_username}@{bastion_ip}"'
    else:
        # all other older machine/builders, we connect via public address
        target_ip = instance["PublicIpAddress"]
        proxy_command = ''

    target_username = guess_username(instance)
    return proxy_command, target_ip, target_username


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
    vms = list_instances_aws(tags, running=True, region_name=region)

    if len(vms) == 1:
        return vms[0]

    if not vms:
        click.echo(click.style("Found no matching instances", fg='red'))
        return {}
    # create the question object
    question = questionary.select(
        "Select machine: ",
        choices=[
            Choice(f"{get_tags(vm).get('Name')} - {vm['PublicIpAddress']} {vm['PrivateIpAddress']} - {get_region(vm)}",
                   value=vm) for vm in vms
        ],
        show_selected=True,
    )

    @question.application.key_bindings.add('x', eager=True)
    @question.application.key_bindings.add('q', eager=True)
    def other(event):
        event.app.exit(exception=KeyboardInterrupt, style="class:aborting")

    # prompt the user for an answer
    return question.ask()


def select_instance_group(region: str = None, **tags) -> list:
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
    vms = list_instances_aws(tags, running=True, region_name=region)

    if len(vms) == 1:
        return vms

    if not vms:
        click.echo(click.style("Found no matching instances", fg='red'))
        return []

    choices = [
        Choice(f"{get_tags(vm).get('Name')} - {vm['PublicIpAddress']} {vm['PrivateIpAddress']} - {get_region(vm)}",
               value=vm, checked=True) for vm in vms
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


# pylint: disable=too-many-arguments
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
        proxy_command, target_ip, target_username = get_proxy_command(connect_vm, force_use_public_ip)
        click.echo(click.style(f"ssh into: {get_tags(connect_vm).get('Name')}",
                               fg='green', bold=True))
        rows = os.environ.get("LINES") or subprocess.check_output(['tput', 'lines'], text=True).strip()
        cols = os.environ.get("COLUMNS") or subprocess.check_output(['tput', 'cols'], text=True).strip()
        tty_options = f'stty rows {rows} cols {cols}'
        cmd = (f'bash -c \'{tty_options}; ssh -tt {proxy_command}'
               f' -i ~/.ssh/scylla-qa-ec2 -o "UserKnownHostsFile=/dev/null" '
               f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 {target_username}@{target_ip} \'')
        click.echo(cmd)
        pty.spawn(shlex.split(cmd))


@click.command("ssh-cmd")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-P", "--force-use-public-ip", is_flag=True, show_default=True, default=False,
              help="Force usage of public address")
@click.argument("node_name", required=False)
@click.argument("command", required=True)
def ssh_cmd(user, test_id, region, force_use_public_ip, node_name, command):
    return ssh_run_cmd(node_name, command, user, test_id, region, force_use_public_ip)


def ssh_run_cmd(node_name: str, command: str, user: str = None,
                test_id: str = None, region: str = None,
                force_use_public_ip: bool = None) -> subprocess.CompletedProcess | None:
    assert user or test_id or (node_name and command)
    connect_vm = select_instance(region=region, test_id=test_id, user=user, node_name=node_name)
    cmd_out = None

    if connect_vm:
        proxy_command, target_ip, target_username = get_proxy_command(connect_vm, force_use_public_ip,
                                                                      strict_host_checking=False)
        click.echo(click.style(f"run command {command} via ssh into: {get_tags(connect_vm).get('Name')}",
                               fg='green', bold=True))

        cmd = (f'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {proxy_command} '
               f'-i ~/.ssh/scylla-qa-ec2 '
               f' -o ServerAliveInterval=10 {target_username}@{target_ip} '
               f'{command}')
        cmd_out = subprocess.run(cmd, shell=True, capture_output=True, check=False)
    return cmd_out


@click.command("tunnel", help="Tunnel ports to any SCT machine on AWS")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-p", "--port", default=3000, help="remote port to tunnel")
@click.argument("node_name", required=False)
def tunnel(user, test_id, region, port, node_name):
    assert user or test_id or node_name
    connect_vm = select_instance(region=region, test_id=test_id, user=user, node_name=node_name)

    if connect_vm:
        aws_region = AwsRegion(get_region(connect_vm))

        bastion = find_bastion_for_instance(connect_vm)
        bastion_username, bastion_ip = guess_username(bastion), bastion["PublicIpAddress"]
        if aws_region.sct_vpc.vpc_id == connect_vm["VpcId"]:
            target_ip = connect_vm["PrivateIpAddress"]
        else:
            target_ip = connect_vm["PublicIpAddress"]
        click.echo(click.style(f"tunnel into: {get_tags(connect_vm).get('Name')}", fg='green'))
        local_port = get_free_port()
        cmd = f'ssh -i ~/.ssh/scylla-qa-ec2 -N -L {local_port}:{target_ip}:{port} -o "UserKnownHostsFile=/dev/null" ' \
              f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 {bastion_username}@{bastion_ip}'
        click.echo(cmd)
        if port == 3000:
            click.echo(click.style(f"connect to: http://127.0.0.1:{local_port}", fg='yellow'))
        if port == 22:
            target_username = guess_username(connect_vm)
            click.echo(click.style(
                f"connect to:\nssh -i ~/.ssh/scylla-qa-ec2 -p {local_port} {target_username}@127.0.0.1", fg='yellow'))
        subprocess.check_output(cmd, shell=True)


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
        proxy_command, target_ip, target_username = get_proxy_command(connect_vm, force_use_public_ip)
        target = f'{target_username}@{target_ip}:'
        if ':' in src:
            src = target + src.split(':', maxsplit=1)[1]
        elif ':' in dest:
            dest = target + dest.split(':', maxsplit=1)[1]
        else:
            click.echo(click.style("Not [src] nor [dest] has target host in them", fg='red'))
        pty.spawn(shlex.split(f'scp {proxy_command}'
                              f' -i ~/.ssh/scylla-qa-ec2 -o "UserKnownHostsFile=/dev/null" '
                              f'-o "StrictHostKeyChecking=no" -o ServerAliveInterval=10 -C {src} {dest}'))


@click.command("attach-test-sg", help="Attach test default security group to a group of instances")
@click.option("-u", "--user", default=None,
              help="User to search for (RunByUser tag)")
@click.option("-t", "--test-id", default=None, help="test id to search for")
@click.option("-r", "--region", default=None, help="region to use, default search across all regions")
@click.option("-g", "--group-id", default=None, help="GroupId to use, default to create one base on TestId")
def attach_test_sg_cmd(user, test_id, region, group_id):
    assert user or test_id
    instances = select_instance_group(region=region, test_id=test_id, user=user)

    for i in instances:
        aws_region: AwsRegion = AwsRegion(get_region(i))
        instance = aws_region.resource.Instance(i['InstanceId'])
        click.echo(click.style(f"attaching test SG to {get_tags(i).get('Name', 'N/A')}", fg='green'))
        if group_id:
            group_id_to_add = group_id
        else:
            group_id_to_add = aws_region.provide_sct_test_security_group(get_tags(i).get('TestId', 'N/A')).group_id
        all_sg_ids = list(set([sg['GroupId'] for sg in instance.security_groups] + [group_id_to_add]))
        instance.modify_attribute(Groups=all_sg_ids)
