#!/usr/bin/env python
import logging
import os
from collections import namedtuple

import click
import click_completion
import boto3
from prettytable import PrettyTable

from sdcm.tester import ClusterTester
from sct_config import SCTConfiguration
from sdcm.utils import list_instances_aws, list_instances_gce, clean_cloud_instances, aws_regions

click_completion.init()


def sct_option(name, sct_name):
    sct_opt = SCTConfiguration.get_config_option(sct_name)
    return click.option(name, type=sct_opt['type'], default=sct_opt['default'], help=sct_opt['help'])


def install_callback(ctx, _, value):
    if not value or ctx.resilient_parsing:
        return value
    shell, path = click_completion.core.install()
    click.echo('%s completion installed in %s' % (shell, path))
    return exit(0)


@click.group()
@click.option('--install-bash-completion', is_flag=True, callback=install_callback, expose_value=False,
              help="Install completion for the current shell. Make sure to have psutil installed.")
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends), default='aws')
def cli(backend):
    os.environ['SCT_CLUSTER_BACKEND'] = backend


@cli.command()
@click.option('--scylla-version', type=str, default='3.0.3')
@sct_option('--db-nodes', 'n_db_nodes')
@sct_option('--loader-nodes', 'n_loaders')
@sct_option('--monitor-nodes', 'n_monitor_nodes')
def provision(**kwargs):
    print kwargs
    logging.basicConfig(level=logging.INFO)
    # click.secho('Going to install scylla cluster version={}'.format(kwargs['scylla_version']), reverse=True, fg='bright_yellow')
    # TODO: find a better way for ctrl+c to kill this process
    os.environ['SCT_NEW_CONFIG'] = 'yes'
    test = ClusterTester(methodName='setUp')
    from avocado.utils import runtime as avocado_runtime
    avocado_runtime.CURRENT_TEST = namedtuple('MockedAvocadoConf', ['name'])(name='sct_provision_command')
    test._setup_environment_variables()
    test.setUp()


@cli.command('clean-resources')
@click.option('--user', type=str)
@click.option('--test-id', type=str)
@click.pass_context
def clean_resources(ctx, user, test_id):
    params = dict()

    if user:
        params['RunByUser'] = user
    if test_id:
        params['TestId'] = test_id

    if params:
        clean_cloud_instances(params)
        click.echo('cleaned instances for {}'.format(params))
    else:
        click.echo(clean_resources.get_help(ctx))


@cli.command('list-resources')
@click.option('--user', type=str, default=None)
@click.option('--test-id', type=str)
@click.pass_context
def list_resources(ctx, user, test_id):
    params = dict()

    if user:
        params['RunByUser'] = user
    if test_id:
        params['TestId'] = test_id

    if params:

        instances = list_instances_aws(params)
        instances = [i for i in instances if not i['State']['Name'] == 'terminated']
        if instances:
            x = PrettyTable(["InstanceId", "Name", "PublicIpAddress", "TestId", "LaunchTime"])
            x.align = "l"
            x.sortby = 'TestId'

            for instance in instances:
                x.add_row([instance['InstanceId'],
                          [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'Name'][0],
                          instance['PublicDnsName'],
                          [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'TestId'][0],
                          instance['LaunchTime'].ctime()])
            click.echo(x.get_string(title="Resources used by '{}' in AWS".format(user)))
        else:
            click.secho("No resources found on AWS", fg='green')

        instances = list_instances_gce({'RunByUser': user})

        if instances:

            x = PrettyTable(["Name", "TestId", "LaunchTime", "PublicIps"])
            x.align = "l"
            x.sortby = 'TestId'
            for instance in instances:
                tags = instance.extra['metadata'].get('items', [])
                test_id = [t['value'] for t in tags if t['key'] == 'TestId']
                test_id = test_id[0] if test_id else 'N/A'
                x.add_row([instance.name,
                           test_id,
                           instance.extra['creationTimestamp'],
                           ", ".join(instance.public_ips)])
            click.echo(x.get_string(title="Resources used by '{}' in GCE".format(user)))
        else:
            click.secho("No resources found on GCE", fg='green')
    else:
        click.echo(list_resources.get_help(ctx))


@cli.command('list-versions')
@click.option('-r', '--region', type=click.Choice(aws_regions), default='eu-west-1')
def list_versions(region):
    EC2 = boto3.client('ec2', region_name=region)
    response = EC2.describe_images(
        Owners=['797456418907'],  # CentOS
        Filters=[
          {'Name': 'name', 'Values': ['ScyllaDB *']},
        ],
    )

    amis = sorted(response['Images'],
                  key=lambda x: x['CreationDate'],
                  reverse=True)

    x = PrettyTable(["Name", "ImageId", "CreationDate"])
    x.align["Name"] = "l"
    x.align["ImageId"] = "l"
    x.align["CreationDate"] = "l"

    for ami in amis:
        x.add_row([ami['Name'], ami['ImageId'], ami['CreationDate']])

    click.echo(x.get_string(title="Scylla AMI versions"))


@cli.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends), default='aws')
def conf(config_file, backend):
    if backend:
        os.environ['SCT_CLUSTER_BACKEND'] = backend
    os.environ['SCT_CONFIG_FILES'] = config_file
    config = SCTConfiguration()
    try:
        config.verify_configuration()
    except Exception as ex:
        click.secho(str(ex), fg='red')
        exit(1)
    else:
        click.secho(config.dump_config(), fg='green')
        exit(0)


if __name__ == '__main__':
    cli()  # pylint: disable=no-value-parameter
