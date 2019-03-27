#!/usr/bin/env python
import os

import click
import click_completion
from prettytable import PrettyTable

from sdcm.results_analyze import PerformanceResultsAnalyzer
from sdcm.sct_config import SCTConfiguration
from sdcm.utils import (list_instances_aws, list_instances_gce, clean_cloud_instances,
                        aws_regions, get_scylla_ami_versions, get_s3_scylla_repos_mapping,
                        list_logs_by_test_id)

click_completion.init()


def sct_option(name, sct_name, **kwargs):
    sct_opt = SCTConfiguration.get_config_option(sct_name)
    sct_opt.update(kwargs)
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


'''
Work in progress

from sdcm.tester import ClusterTester

@cli.command()
@click.option('--scylla-version', type=str, default='3.0.3')
@sct_option('--db-nodes', 'n_db_nodes')
@sct_option('--loader-nodes', 'n_loaders')
@sct_option('--monitor-nodes', 'n_monitor_nodes')
def provision(**kwargs):
    logging.basicConfig(level=logging.INFO)
    # click.secho('Going to install scylla cluster version={}'.format(kwargs['scylla_version']), reverse=True, fg='bright_yellow')
    # TODO: find a better way for ctrl+c to kill this process
    os.environ['SCT_NEW_CONFIG'] = 'yes'
    test = ClusterTester(methodName='setUp')
    from avocado.utils import runtime as avocado_runtime
    avocado_runtime.CURRENT_TEST = namedtuple('MockedAvocadoConf', ['name'])(name='sct_provision_command')
    test._setup_environment_variables()
    test.setUp()
'''


@cli.command('clean-resources', help='clean tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@sct_option('--test-id', 'test_id', help='test id to filter by')
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


@cli.command('list-resources', help='list tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@sct_option('--test-id', 'test_id', help='test id to filter by')
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
                name = [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'Name']
                test_id = [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'TestId']
                x.add_row([instance['InstanceId'],
                           name[0] if name else 'N/A',
                           instance['PublicDnsName'],
                           test_id[0] if test_id else 'N/A',
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


@cli.command('list-ami-versions', help='list Amazon Scylla formal AMI versions')
@click.option('-r', '--region', type=click.Choice(aws_regions), default='eu-west-1')
def list_ami_versions(region):

    amis = get_scylla_ami_versions(region)

    x = PrettyTable(["Name", "ImageId", "CreationDate"])
    x.align = "l"

    for ami in amis:
        x.add_row([ami['Name'], ami['ImageId'], ami['CreationDate']])

    click.echo(x.get_string(title="Scylla AMI versions"))


@cli.command('list-repos', help='List repos url of Scylla formal versions')
@click.option('-d', '--dist-type', type=click.Choice(['centos', 'ubuntu', 'debian']), default='centos', help='Distribution type')
@click.option('-v', '--dist-version', type=click.Choice(['xenial', 'trusty', 'bionic', 'jessie', 'stretch']), default=None, help='deb style versions')
def list_repos(dist_type, dist_version):
    if not dist_type == 'centos' and dist_version is None:
        click.secho("when passing --dist-type=debian/ubutnu need to pass --dist-version as well", fg='red')
        exit(1)

    repo_maps = get_s3_scylla_repos_mapping(dist_type, dist_version)

    x = PrettyTable(["Version Family", "Repo Url"])
    x.align = "l"

    for version_prefix, repo_url in repo_maps.items():
        x.add_row([version_prefix, repo_url])

    click.echo(x.get_string(title="Scylla Repos"))


@cli.command(help="Check test configuration file")
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


@cli.command('conf-docs', help="Show all available configuration in yaml/markdown format")
@click.option('-o', '--output-format', type=click.Choice(["yaml", "markdown"]), default="yaml", help="type of the output")
def conf_docs(output_format):
    if output_format == 'markdown':
        click.secho(SCTConfiguration().dump_help_config_markdown())
    elif output_format == 'yaml':
        click.secho(SCTConfiguration().dump_help_config_yaml())


@cli.command("perf-regression-report", help="Generate and send performance regression report")
@click.option("-i", "--es-id", required=True, type=str, help="Id of the run in Elastic Search")
@click.option("-e", "--emails", required=True, type=str, help="Comma separated list of emails. Example a@b.com,c@d.com")
@click.option("-l", "--debug-log", required=False, default=False, is_flag=True, help="Print debug logs")
def perf_regression_report(es_id, emails, debug_log):
    email_list = emails.split(",")
    click.secho(message="Will send Performance Regression report to %s" % email_list, fg="green")
    rootLogger = None
    if debug_log:
        import sys
        import logging
        rootLogger = logging.getLogger()
        rootLogger.setLevel(logging.DEBUG)
        rootLogger.addHandler(logging.StreamHandler(sys.stdout))
    ra = PerformanceResultsAnalyzer(es_index="performanceregressiontest", es_doc_type="test_stats",
                                    send_email=True, email_recipients=email_list, logger=rootLogger)
    click.secho(message="Checking regression comparing to: %s" % es_id, fg="green")
    ra.check_regression(es_id)
    click.secho(message="Done." % email_list, fg="yellow")


@click.group(help="Group of commands for investigating testrun")
def investigate():
    pass


@investigate.command('show-logs', help="Show logs collected for testrun filtered by test-id")
@click.argument('test_id')
def show_log(test_id):
    x = PrettyTable(["Log type", "Link"])
    x.align = "l"
    files = list_logs_by_test_id(test_id)
    for log in files:
        x.add_row([log["type"], log["link"]])
    click.echo(x.get_string(title="Log links for testrun with test id {}".format(test_id)))


@investigate.command('show-monitor', help="Show link to prometheus data snapshot")
@click.argument('test_id')
def show_monitor(test_id):
    x = PrettyTable(["Link to prometheus snapshot"])
    x.align = "l"
    files = list_logs_by_test_id(test_id)
    for log in files:
        if log["type"] == "prometheus":
            x.add_row([log["link"]])
    click.echo(x.get_string())


cli.add_command(investigate)


if __name__ == '__main__':
    cli()  # pylint: disable=no-value-parameter
