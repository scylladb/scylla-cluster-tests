#!/usr/bin/env python3
import os
import sys
import unittest
import logging

import pytest
import click
import click_completion
from prettytable import PrettyTable

from sdcm.results_analyze import PerformanceResultsAnalyzer
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.cloud_monitor import cloud_report
from sdcm.utils.common import (list_instances_aws, list_instances_gce, clean_cloud_instances,
                               AWS_REGIONS, get_scylla_ami_versions, get_s3_scylla_repos_mapping,
                               list_logs_by_test_id, get_branched_ami, gce_meta_to_dict,
                               aws_tags_to_dict, list_elastic_ips_aws, get_builder_by_test_id,
                               clean_aws_instances_according_post_behavior,
                               clean_gce_instances_according_post_behavior,
                               search_test_id_in_latest, get_testrun_dir)
from sdcm.utils.monitorstack import restore_monitor_stack
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
    return sys.exit(0)


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
'''  # pylint: disable=pointless-string-statement


@cli.command('clean-resources', help='clean tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@sct_option('--test-id', 'test_id', help='test id to filter by. Could be used multiple times', multiple=True)
@click.option('--logdir', type=str, help='directory with test run')
@click.option('--config-file', multiple=True, type=click.Path(exists=True), help="Test config .yaml to use, can have multiple of those")
@click.option('--backend', type=str, help="")
@click.pass_context
def clean_resources(ctx, user, test_id, logdir, config_file, backend):  # pylint: disable=too-many-arguments,too-many-branches
    params = dict()

    if config_file or logdir:

        if not logdir:
            logdir = os.path.expandvars("$HOME/sct-results")

        if logdir and not test_id:
            test_id = (search_test_id_in_latest(logdir), )

        if not logdir or not all(test_id):
            click.echo(clean_resources.get_help(ctx))
            return

        if not backend:
            backend = "aws"

        if not os.environ.get('SCT_CLUSTER_BACKEND', None):
            os.environ['SCT_CLUSTER_BACKEND'] = backend

        if config_file:
            os.environ['SCT_CONFIG_FILES'] = str(list(config_file))

        config = SCTConfiguration()

        for _test_id in test_id:
            params['TestId'] = _test_id
            if 'aws' in backend:
                clean_aws_instances_according_post_behavior(params, config, logdir)
            if 'gce' in backend:
                clean_gce_instances_according_post_behavior(params, config, logdir)

    else:
        if not (user or test_id):
            click.echo(clean_resources.get_help(ctx))
            return

        if user:
            params['RunByUser'] = user

        if test_id:
            for _test_id in test_id:
                params['TestId'] = _test_id
                clean_cloud_instances(params)
                click.echo('cleaned instances for {}'.format(params))
        else:
            clean_cloud_instances(params)
            click.echo('cleaned instances for {}'.format(params))


@cli.command('list-resources', help='list tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@click.option('--get-all', is_flag=True, default=False, help='All resources')
@click.option('--get-all-running', is_flag=True, default=False, help='All running resources')
@sct_option('--test-id', 'test_id', help='test id to filter by')
@click.option('--verbose', is_flag=True, default=False, help='if enable, will log progress')
@click.pass_context
def list_resources(ctx, user, test_id, get_all, get_all_running, verbose):
    # pylint: disable=too-many-locals,too-many-arguments,too-many-branches,too-many-statements

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

    click.secho("Checking AWS EC2...", fg='green')
    aws_instances = list_instances_aws(tags_dict=params, running=get_all_running, verbose=verbose)

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
        click.echo(aws_table.get_string(title="Instances used on AWS"))
    else:
        click.secho("Nothing found for selected filters in AWS!", fg="yellow")

    click.secho("Checking AWS Elastic IPs...", fg='green')
    elastic_ips_aws = list_elastic_ips_aws(tags_dict=params, verbose=verbose)
    if elastic_ips_aws:
        aws_table = PrettyTable(["AllocationId", "PublicIP", "TestId", "RunByUser", "InstanceId (attached to)"])
        aws_table.align = "l"
        aws_table.sortby = 'AllocationId'
        for eip in elastic_ips_aws:
            tags = aws_tags_to_dict(eip.get('Tags'))
            test_id = tags.get("TestId", "N/A")
            run_by_user = tags.get("RunByUser", "N/A")
            aws_table.add_row([
                eip['AllocationId'],
                eip['PublicIp'],
                test_id,
                run_by_user,
                eip.get('InstanceId', 'N/A')])
        click.echo(aws_table.get_string(title="EIPs used on AWS"))
    else:
        click.secho("No elastic ips found for selected filters in AWS!", fg="yellow")

    click.secho("Checking GCE...", fg='green')
    gce_instances = list_instances_gce(tags_dict=params, running=get_all_running, verbose=verbose)
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

    tbl = PrettyTable(["Name", "ImageId", "CreationDate"])
    tbl.align = "l"

    for ami in amis:
        tbl.add_row([ami['Name'], ami['ImageId'], ami['CreationDate']])

    click.echo(tbl.get_string(title="Scylla AMI versions"))


@cli.command('list-ami-branch', help="""list Amazon Scylla branched AMI versions
    \n\n[VERSION] is a branch version to look for, ex. 'branch-2019.1:latest', 'branch-3.1:all'""")
@click.option('-r', '--region', type=click.Choice(AWS_REGIONS), default='eu-west-1')
@click.argument('version', type=str, default='branch-3.1:all')
def list_ami_branch(region, version):
    def get_tags(ami):
        return {i['Key']: i['Value'] for i in ami.tags}

    amis = get_branched_ami(version, region_name=region)
    tbl = PrettyTable(["Name", "ImageId", "CreationDate", "BuildId", "Test Status"])
    tbl.align = "l"

    for ami in amis:
        tags = get_tags(ami)
        test_status = [(k, v) for k, v in tags.items() if k.startswith('JOB:')]
        test_status = [click.style(k, fg='green') for k, v in test_status if v == 'PASSED'] + \
                      [click.style(k, fg='red') for k, v in test_status if not v == 'PASSED']
        test_status = ", ".join(test_status) if test_status else click.style('Unknown', fg='yellow')
        tbl.add_row([ami.name, ami.id, ami.creation_date, tags['build-id'], test_status])

    click.echo(tbl.get_string(title="Scylla AMI branch versions"))


@cli.command('list-repos', help='List repos url of Scylla formal versions')
@click.option('-d', '--dist-type', type=click.Choice(['centos', 'ubuntu', 'debian']), default='centos', help='Distribution type')
@click.option('-v', '--dist-version', type=click.Choice(['xenial', 'trusty', 'bionic', 'jessie', 'stretch']), default=None, help='deb style versions')
def list_repos(dist_type, dist_version):
    if not dist_type == 'centos' and dist_version is None:
        click.secho("when passing --dist-type=debian/ubutnu need to pass --dist-version as well", fg='red')
        sys.exit(1)

    repo_maps = get_s3_scylla_repos_mapping(dist_type, dist_version)

    tbl = PrettyTable(["Version Family", "Repo Url"])
    tbl.align = "l"

    for version_prefix, repo_url in repo_maps.items():
        tbl.add_row([version_prefix, repo_url])

    click.echo(tbl.get_string(title="Scylla Repos"))


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
    except Exception as ex:  # pylint: disable=broad-except
        click.secho(str(ex), fg='red')
        sys.exit(1)
    else:
        click.secho(config.dump_config(), fg='green')
        sys.exit(0)


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
    results_analyzer = PerformanceResultsAnalyzer(es_index="performanceregressiontest", es_doc_type="test_stats",
                                                  send_email=True, email_recipients=email_list, logger=LOGGER)
    click.secho(message="Checking regression comparing to: %s" % es_id, fg="green")
    results_analyzer.check_regression(es_id)
    click.secho(message="Done." % email_list, fg="yellow")


@click.group(help="Group of commands for investigating testrun")
def investigate():
    pass


@investigate.command('show-logs', help="Show logs collected for testrun filtered by test-id")
@click.argument('test_id')
def show_log(test_id):
    table = PrettyTable(["Date", "Log type", "Link"])
    table.align = "l"
    files = list_logs_by_test_id(test_id)
    for log in files:
        table.add_row([log["date"].strftime("%Y%m%d_%H%M%S"), log["type"], log["link"]])
    click.echo(table.get_string(title="Log links for testrun with test id {}".format(test_id)))


@investigate.command('show-monitor', help="Show link to prometheus data snapshot")
@click.argument('test_id')
@click.option("--date-time", type=str, required=False, help='Datetime of monitor collecting')
def show_monitor(test_id, date_time):
    click.echo('Search monitor stack archive files for test id {} and restoring...'.format(test_id))
    # if debug_log:
    #     LOGGER.setLevel(logging.DEBUG)
    status = restore_monitor_stack(test_id, date_time)
    table = PrettyTable(['Name', 'container', 'Link'])
    table.align = 'l'
    if status:
        click.echo('Monitoring stack restored')

        table.add_row(['Prometheus server', 'aprom', 'http://localhost:9090'])
        table.add_row(['Grafana server', 'agraf', 'http://localhost:3000'])
        click.echo(table.get_string(title='Grafana monitoring stack'))
    else:
        click.echo('Docker containers were not started')


@investigate.command('search-builder', help='Search builder where test run with test-id located')
@click.argument('test-id')
def search_builder(test_id):
    results = get_builder_by_test_id(test_id)
    tbl = PrettyTable(['Builder Name', "Public IP", "path"])
    tbl.align = 'l'
    for result in results:
        tbl.add_row([result['builder']['name'], result['builder']['public_ip'], result['path']])

    click.echo(tbl.get_string(title='Found builders for Test-id: {}'.format(test_id)))


cli.add_command(investigate)


@cli.command('unit-tests', help="Run all the SCT internal unit-tests")
@click.option("-t", "--test", required=False, default="",
              help="Run specific test file from unit-tests directory")
def unit_tests(test):
    sys.exit(pytest.main(['-v', '-p', 'no:warnings', 'unit_tests/{}'.format(test)]))


class OutputLogger():
    def __init__(self, filename, terminal):
        self.terminal = terminal
        self.log = open(filename, "a")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def isatty(self):  # pylint: disable=no-self-use
        return False


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
                  failfast=False, buffer=False, catchbreak=True)


@cli.command("cloud-usage-report", help="Generate and send Cloud usage report")
@click.option("-e", "--emails", required=True, type=str, help="Comma separated list of emails. Example a@b.com,c@d.com")
def cloud_usage_report(emails):
    email_list = emails.split(",")
    click.secho(message="Will send Cloud Usage report to %s" % email_list, fg="green")
    cloud_report(mail_to=email_list)
    click.secho(message="Done." % email_list, fg="yellow")


@cli.command('collect-logs', help='Collect logs from cluster by test-id')
@click.option('--test-id', help='Find cluster by test-id')
@click.option('--logdir', help='Path to directory with sct results')
@click.option('--backend', help='Cloud where search nodes', default='aws')
@click.option('--config-file', type=str, help='config test file path')
def collect_logs(test_id=None, logdir=None, backend='aws', config_file=None):
    from sdcm.logcollector import Collector
    if not os.environ.get('SCT_CLUSTER_BACKEND', None):
        os.environ['SCT_CLUSTER_BACKEND'] = backend
    if config_file and not os.environ.get('SCT_CONFIG_FILES', None):
        os.environ['SCT_CONFIG_FILES'] = config_file

    config = SCTConfiguration()

    collector = Collector(test_id=test_id, params=config, test_dir=logdir)

    collected_logs = collector.run()

    table = PrettyTable(['Cluster set', 'Link'])
    table.align = 'l'
    for cluster_type, s3_link in collected_logs.items():
        table.add_row([cluster_type, s3_link])
    click.echo(table.get_string(title="Collected logs by test-id: {} Directory: {}".format(collector.test_id,
                                                                                           collector.storage_dir,)))


@cli.command('send-email', help='Send email with results for testrun')
@click.option('--test-id', help='Test-id of run')
@click.option('--email-recipients', help="Send email to next recipients")
@click.option('--logdir', help='Directory where to find testrun folder')
def send_email(test_id=None, email_recipients=None, logdir=None):
    from sdcm.send_email import (GeminiEmailReporter, LongevityEmailReporter,
                                 get_running_instances_for_email_report,
                                 read_email_data_from_file)

    if not logdir:
        logdir = os.path.expanduser('~/sct-results')
    testrun_dir = get_testrun_dir(test_id=test_id, base_dir=logdir)

    email_results_file = os.path.join(testrun_dir, "email_data.json")
    test_results = read_email_data_from_file(email_results_file)
    if not test_results:
        LOGGER.warning("File with email results data not found")
        return

    reporter = test_results.get('reporter')
    email_recipients = email_recipients.split(',')
    if not reporter:
        LOGGER.warning("No reporter found")
    else:
        test_results['nodes'] = get_running_instances_for_email_report(test_id)
        if "Gemini" in reporter:
            reporter = GeminiEmailReporter(email_recipients, logdir=testrun_dir)
            reporter.send_report(test_results)
        elif "Longevity" in reporter:
            reporter = LongevityEmailReporter(email_recipients, logdir=testrun_dir)
            reporter.send_report(test_results)
        else:
            LOGGER.warning("No reporter found")


if __name__ == '__main__':
    cli()
