#!/usr/bin/env python
import logging
import os
import sys
import unittest

import click
import click_completion
from prettytable import PrettyTable
import xmlrunner

from sdcm.results_analyze import PerformanceResultsAnalyzer
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.cloud_monitor import cloud_report
from sdcm.utils.common import (list_instances_aws, list_instances_gce, clean_cloud_instances,
                               AWS_REGIONS, get_scylla_ami_versions, get_s3_scylla_repos_mapping,
                               list_logs_by_test_id, restore_monitoring_stack, get_branched_ami, gce_meta_to_dict,
                               aws_tags_to_dict)
from sdcm.cluster import Setup
from sdcm.utils.log import setup_stdout_logger

LOGGER = setup_stdout_logger()

click_completion.init()


def sct_option(name, sct_name, **kwargs):
    sct_opt = SCTConfiguration.get_config_option(sct_name)
    multimple_use = kwargs.pop('multiple', False)
    sct_opt.update(kwargs)
    return click.option(name, type=sct_opt['type'], help=sct_opt['help'], multiple=multimple_use)


def install_callback(ctx, _, value):
    if not value or ctx.resilient_parsing:
        return value
    shell, path = click_completion.core.install()
    click.echo('%s completion installed in %s' % (shell, path))
    return exit(0)


@click.group()
@click.option('--install-bash-completion', is_flag=True, callback=install_callback, expose_value=False,
              help="Install completion for the current shell. Make sure to have psutil installed.")
def cli():
    pass


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
    test._setup_environment_variables()
    test.setUp()
'''


@cli.command('clean-resources', help='clean tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@sct_option('--test-id', 'test_id', help='test id to filter by. Could be used multiple times', multiple=True)
@click.pass_context
def clean_resources(ctx, user, test_id):
    params = dict()

    if not (user or test_id):
        click.echo(clean_resources.get_help(ctx))

    if user:
        params['RunByUser'] = user

    if not test_id:
        clean_cloud_instances(params)
        click.echo('cleaned instances for {}'.format(params))

    if test_id:
        for _test_id in test_id:
            params['TestId'] = _test_id
            clean_cloud_instances(params)
            click.echo('cleaned instances for {}'.format(params))


@cli.command('list-resources', help='list tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@click.option('--get-all', is_flag=True, default=False, help='All resources')
@click.option('--get-all-running', is_flag=True, default=False, help='All running resources')
@sct_option('--test-id', 'test_id', help='test id to filter by')
@click.pass_context
def list_resources(ctx, user, test_id, get_all, get_all_running):
    params = dict()

    if user:
        params['RunByUser'] = user
    if test_id:
        params['TestId'] = test_id
    if all([not get_all, not get_all_running, not user, not test_id]):
        click.echo(list_resources.get_help(ctx))

    if get_all_running:
        table_header = ["Name", "Region-AZ", "PublicIP", "TestId", "RunByUser", "LaunchTime"]
    else:
        table_header = ["Name", "Region-AZ", "State", "TestId", "RunByUser", "LaunchTime"]

    click.secho("Checking EC2...", fg='green')

    aws_instances = list_instances_aws(tags_dict=params, running=get_all_running)
    click.secho("Checking AWS EC2...", fg='green')
    if aws_instances:
        aws_table = PrettyTable(table_header)
        aws_table.align = "l"
        aws_table.sortby = 'LaunchTime'
        for instance in aws_instances:
            tags = aws_tags_to_dict(instance.get('Tags'))
            name = tags.get("Name", "N/A")
            test_id = tags.get("TestId", "N/A")
            run_by_user = tags.get("RunByUser", "N/A")
            aws_table.add_row([
                name,
                instance['Placement']['AvailabilityZone'],
                instance.get('PublicIpAddress', 'N/A') if get_all_running else instance['State']['Name'],
                test_id,
                run_by_user,
                instance['LaunchTime'].ctime()])
        click.echo(aws_table.get_string(title="Resources used on AWS"))
    else:
        click.secho("Nothing found for selected filters in AWS!", fg="yellow")

    click.secho("Checking GCE...", fg='green')
    gce_instances = list_instances_gce(tags_dict=params, running=get_all_running)
    if gce_instances:
        gce_table = PrettyTable(table_header)
        gce_table.align = "l"
        gce_table.sortby = 'LaunchTime'
        for instance in gce_instances:
            tags = gce_meta_to_dict(instance.extra['metadata'])
            public_ips = ", ".join(instance.public_ips) if None not in instance.public_ips else "N/A"
            gce_table.add_row([instance.name,
                               instance.extra["zone"].name,
                               public_ips if get_all_running else instance.state,
                               tags.get('TestId', 'N/A') if tags else "N/A",
                               tags.get('RunByUser', 'N/A') if tags else "N/A",
                               instance.extra['creationTimestamp'],
                               ])
        click.echo(gce_table.get_string(title="Resources used on GCE"))
    else:
        click.secho("Nothing found for selected filters in GCE!", fg="yellow")


@cli.command('list-ami-versions', help='list Amazon Scylla formal AMI versions')
@click.option('-r', '--region', type=click.Choice(AWS_REGIONS), default='eu-west-1')
def list_ami_versions(region):

    amis = get_scylla_ami_versions(region)

    x = PrettyTable(["Name", "ImageId", "CreationDate"])
    x.align = "l"

    for ami in amis:
        x.add_row([ami['Name'], ami['ImageId'], ami['CreationDate']])

    click.echo(x.get_string(title="Scylla AMI versions"))


@cli.command('list-ami-branch', help="""list Amazon Scylla branched AMI versions
    \n\n[VERSION] is a branch version to look for, ex. 'branch-2019.1:latest', 'branch-3.1:all'""")
@click.option('-r', '--region', type=click.Choice(AWS_REGIONS), default='eu-west-1')
@click.argument('version', type=str, default='branch-3.1:all')
def list_ami_branch(region, version):
    def get_tags(ami):
        return {i['Key']: i['Value'] for i in ami.tags}

    amis = get_branched_ami(version, region_name=region)

    x = PrettyTable(["Name", "ImageId", "CreationDate", "BuildId"])
    x.align = "l"

    for ami in amis:
        x.add_row([ami.name, ami.id, ami.creation_date, get_tags(ami)['build-id']])

    click.echo(x.get_string(title="Scylla AMI branch versions"))


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
def perf_regression_report(es_id, emails):
    email_list = emails.split(",")
    click.secho(message="Will send Performance Regression report to %s" % email_list, fg="green")
    LOGGER.setLevel(logging.DEBUG)
    ra = PerformanceResultsAnalyzer(es_index="performanceregressiontest", es_doc_type="test_stats",
                                    send_email=True, email_recipients=email_list, logger=LOGGER)
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
@click.option("-l", "--debug-log", required=False, default=False, is_flag=True, help="Print debug logs")
def show_monitor(test_id, debug_log):
    click.echo('Search monitor stack archive files for test id {} and restoring...'.format(test_id))
    if debug_log:
        LOGGER.setLevel(logging.DEBUG)
    status = restore_monitoring_stack(test_id)
    x = PrettyTable(['Name', 'container', 'Link'])
    x.align = 'l'
    if status:
        click.echo('Monitoring stack restored')

        x.add_row(['Prometheus server', 'aprom', 'http://localhost:9090'])
        x.add_row(['Grafana server', 'agraf', 'http://localhost:3000'])
        click.echo(x.get_string(title='Grafana monitoring stack'))
    else:
        click.echo('Docker containers were not started. Please rerun comand with flag -l')


cli.add_command(investigate)


@cli.command('unit-tests', help="Run all the SCT internal unit-tests")
@click.option("-t", "--test", required=False, default="test*.py",
              help="Run specific test file from unit-tests directory")
def unit_tests(test):
    import unittest

    test_suite = unittest.TestLoader().discover('unit_tests', pattern=test, top_level_dir='.')
    result = unittest.TextTestRunner(verbosity=2).run(test_suite)
    sys.exit(not result.wasSuccessful())


class OutputLogger(object):
    def __init__(self, filename, terminal):
        self.terminal = terminal
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()


@cli.command('run-test', help="Run SCT test using unittest")
@click.argument('argv')
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends), help="Backend to use")
@click.option('-c', '--config', multiple=True, type=click.Path(exists=True), help="Test config .yaml to use, can have multiple of those")
@click.option('-l', '--logdir', help="Directory to use for logs")
def run_test(argv, backend, config, logdir):

    if config:
        os.environ['SCT_CONFIG_FILES'] = str(list(config))
    if backend:
        os.environ['SCT_CLUSTER_BACKEND'] = backend

    if logdir:
        os.environ['_SCT_LOGDIR'] = logdir
    logfile = os.path.join(Setup.logdir(), 'output.log')
    sys.stdout = OutputLogger(logfile, sys.stdout)
    sys.stderr = OutputLogger(logfile, sys.stderr)

    unittest.main(module=None, argv=['python -m unittest', argv],
                  testRunner=xmlrunner.XMLTestRunner(stream=sys.stderr, output=os.path.join(Setup.logdir(), 'test-reports')),
                  failfast=False, buffer=False, catchbreak=True)


@cli.command("cloud-usage-report", help="Generate and send Cloud usage report")
@click.option("-e", "--emails", required=True, type=str, help="Comma separated list of emails. Example a@b.com,c@d.com")
def cloud_usage_report(emails):
    email_list = emails.split(",")
    click.secho(message="Will send Cloud Usage report to %s" % email_list, fg="green")
    cloud_report(mail_to=email_list)
    click.secho(message="Done." % email_list, fg="yellow")


if __name__ == '__main__':
    cli()  # pylint: disable=no-value-parameter
