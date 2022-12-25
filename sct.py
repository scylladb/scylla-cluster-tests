#!/usr/bin/env python3

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
# Copyright (c) 2021 ScyllaDB
# pylint: disable=too-many-lines

import os
import sys
import unittest
import logging
import glob
import time
import subprocess
import traceback
from pathlib import Path
from functools import partial

import pytest
import click
import click_completion
from prettytable import PrettyTable

from sdcm.results_analyze import PerformanceResultsAnalyzer
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_runner import SctRunner
from sdcm.utils.cloud_monitor import cloud_report
from sdcm.utils.common import (list_instances_aws, list_instances_gce, list_resources_docker, clean_cloud_resources,
                               all_aws_regions, get_scylla_ami_versions, get_s3_scylla_repos_mapping,
                               list_logs_by_test_id, get_branched_ami, gce_meta_to_dict,
                               aws_tags_to_dict, list_elastic_ips_aws, get_builder_by_test_id,
                               clean_resources_according_post_behavior, clean_sct_runners,
                               search_test_id_in_latest, get_testrun_dir, format_timestamp, list_clusters_gke)
from sdcm.utils.jepsen import JepsenResults
from sdcm.utils.monitorstack import (restore_monitoring_stack, get_monitoring_stack_services,
                                     kill_running_monitoring_stack_services)
from sdcm.cluster import Setup
from sdcm.utils.log import setup_stdout_logger
from sdcm.utils.prepare_region import AwsRegion
from sdcm.utils.get_username import get_username
from utils.build_system.create_test_release_jobs import JenkinsPipelines

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


def install_package_from_dir(ctx, _, directories):
    if directories or not ctx.resilient_parsing:
        for directory in directories:
            subprocess.check_call(["sudo", sys.executable, "-m", "pip", "install", directory])
    return directories


def add_file_logger(level: int = logging.DEBUG) -> None:
    cmd_path = "-".join(click.get_current_context().command_path.split()[1:])
    logdir = Setup.make_new_logdir(update_latest_symlink=False, postfix=f"-{cmd_path}")
    handler = logging.FileHandler(os.path.join(logdir, "hydra.log"))
    handler.setLevel(level)
    LOGGER.addHandler(handler)


@click.group()
@click.option('--install-bash-completion', is_flag=True, callback=install_callback, expose_value=False,
              help="Install completion for the current shell. Make sure to have psutil installed.")
@click.option('--install-package-from-directory', callback=install_package_from_dir, multiple=True, envvar='PACKAGES_PATHS',
              type=click.Path(), expose_value=False,
              help="Install paths for extra python pacakges to install, scylla-cluster-plugins for example")
def cli():
    pass


# '''
# Work in progress

# from sdcm.tester import ClusterTester

# @cli.command()
# @click.option('--scylla-version', type=str, default='3.0.3')
# @sct_option('--db-nodes', 'n_db_nodes')
# @sct_option('--loader-nodes', 'n_loaders')
# @sct_option('--monitor-nodes', 'n_monitor_nodes')
# def provision(**kwargs):
#     logging.basicConfig(level=logging.INFO)
#     # click.secho('Going to install scylla cluster version={}'.format(kwargs['scylla_version']), reverse=True, fg='bright_yellow')
#     # TODO: find a better way for ctrl+c to kill this process
#     test = ClusterTester(methodName='setUp')
#     test._setup_environment_variables()
#     test.setUp()
# '''  # pylint: disable=pointless-string-statement


@cli.command('clean-resources', help='clean tagged instances in both clouds (AWS/GCE)')
@click.option('--post-behavior', is_flag=True, default=False, help="clean all resources according to post behavior")
@click.option('--user', type=str, help='user name to filter instances by')
@sct_option('--test-id', 'test_id', help='test id to filter by. Could be used multiple times', multiple=True)
@click.option('--logdir', type=str, help='directory with test run')
@click.option('--dry-run', is_flag=True, default=False, help='dry run')
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends), help="Backend to use")
@click.pass_context
def clean_resources(ctx, post_behavior, user, test_id, logdir, dry_run, backend):  # pylint: disable=too-many-arguments,too-many-branches
    """Clean cloud resources.

    There are different options how to run clean up:
      - To clean resources for the latest run according to post behavior
        $ hydra clean-resources --post-behavior
      - The same as above but with altered logdir
        $ hydra clean-resources --post-behavior --logdir /path/to/logdir
      - To clean resources for some Test ID according to post behavior (test run status extracted from logdir)
        $ hydra clean-resources --post-behavior --test-id TESTID
      - The same as above but with altered logdir
        $ hydra clean-resources --post-behavior --test-id TESTID --logdir /path/to/logdir
      - To clean resources for the latest run ignoring post behavior
        $ hydra clean-resources
      - The same as above but with altered logdir
        $ hydra clean-resources --logdir /path/to/logdir
      - To clean all resources belong to some Test ID:
        $ hydra clean-resources --test-id TESTID
      - To clean all resources belong to some user:
        $ hydra clean-resources --user vasya.pupkin

    Also you can add --dry-run option to see what should be cleaned.
    """
    add_file_logger()

    user_param = {"RunByUser": user} if user else {}

    if not post_behavior and user and not test_id and not logdir:
        click.echo(f"Clean all resources belong to user `{user}'")
        params = (user_param, )
    else:
        if not logdir and (post_behavior or not test_id):
            logdir = Setup.base_logdir()

        if not test_id and (latest_test_id := search_test_id_in_latest(logdir)):
            click.echo(f"Latest TestId in {logdir} is {latest_test_id}")
            test_id = (latest_test_id, )

        if not test_id:
            click.echo(clean_resources.get_help(ctx))
            return

        if post_behavior:
            click.echo(f"Clean resources according to post behavior for following Test IDs: {test_id}")
        else:
            click.echo(f"Clean all resources for following Test IDs: {test_id}")

        params = ({"TestId": tid, **user_param} for tid in test_id)

    if backend is None:
        if os.environ.get('SCT_CLUSTER_BACKEND', None) is None:
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
    else:
        os.environ['SCT_CLUSTER_BACKEND'] = backend

    if post_behavior:
        click.echo(f"Use {logdir} as a logdir")
        clean_func = partial(clean_resources_according_post_behavior, config=SCTConfiguration(), logdir=logdir)
    else:
        clean_func = clean_cloud_resources

    if dry_run:
        click.echo("Make a dry-run")

    for param in params:
        clean_func(param, dry_run=dry_run)
        click.echo(f"Resources for {param} have cleaned")


@cli.command('list-resources', help='list tagged instances in both clouds (AWS/GCE)')
@click.option('--user', type=str, help='user name to filter instances by')
@click.option('--get-all', is_flag=True, default=False, help='All resources')
@click.option('--get-all-running', is_flag=True, default=False, help='All running resources')
@sct_option('--test-id', 'test_id', help='test id to filter by')
@click.option('--verbose', is_flag=True, default=False, help='if enable, will log progress')
@click.pass_context
def list_resources(ctx, user, test_id, get_all, get_all_running, verbose):
    # pylint: disable=too-many-locals,too-many-arguments,too-many-branches,too-many-statements

    add_file_logger()

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
    gke_clusters = list_clusters_gke(tags_dict=params, verbose=verbose)
    if gke_clusters:
        gke_table = PrettyTable(["Name", "Region-AZ", "TestId", "RunByUser", "CreateTime"])
        gke_table.align = "l"
        gke_table.sortby = 'CreateTime'
        for cluster in gke_clusters:
            tags = gce_meta_to_dict(cluster.extra['metadata'])
            gke_table.add_row([cluster.name,
                               cluster.zone,
                               tags.get('TestId', 'N/A') if tags else "N/A",
                               tags.get('RunByUser', 'N/A') if tags else "N/A",
                               cluster.cluster_info['createTime'],
                               ])
        click.echo(gke_table.get_string(title="GKE clusters"))
    else:
        click.secho("Nothing found for selected filters in GKE!", fg="yellow")
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

    click.secho("Checking Docker...", fg="green")
    docker_resources = \
        list_resources_docker(tags_dict=params, running=get_all_running, group_as_builder=True, verbose=verbose)

    if any(docker_resources.values()):
        if docker_resources.get("containers"):
            docker_table = PrettyTable(["Name", "Builder", "Public IP" if get_all_running else "Status",
                                        "TestId", "RunByUser", "Created"])
            docker_table.align = "l"
            docker_table.sortby = "Created"
            for builder_name, docker_containers in docker_resources["containers"].items():
                for container in docker_containers:
                    container.reload()
                    docker_table.add_row([
                        container.name,
                        builder_name,
                        container.attrs["NetworkSettings"]["IPAddress"] if get_all_running else container.status,
                        container.labels.get("TestId", "N/A"),
                        container.labels.get("RunByUser", "N/A"),
                        container.attrs.get("Created", "N/A"),
                    ])
            click.echo(docker_table.get_string(title="Containers used on Docker"))
        if docker_resources.get("images"):
            docker_table = PrettyTable(["Name", "Builder", "TestId", "RunByUser", "Created"])
            docker_table.align = "l"
            docker_table.sortby = "Created"
            for builder_name, docker_images in docker_resources["images"].items():
                for image in docker_images:
                    image.reload()
                    for tag in image.tags:
                        docker_table.add_row([
                            tag,
                            builder_name,
                            image.labels.get("TestId", "N/A"),
                            image.labels.get("RunByUser", "N/A"),
                            image.attrs.get("Created", "N/A"),
                        ])
            click.echo(docker_table.get_string(title="Images used on Docker"))
    else:
        click.secho("Nothing found for selected filters in Docker!", fg="yellow")


@cli.command('list-ami-versions', help='list Amazon Scylla formal AMI versions')
@click.option('-r', '--region', type=click.Choice(all_aws_regions(cached=True)), default='eu-west-1')
def list_ami_versions(region):
    add_file_logger()

    amis = get_scylla_ami_versions(region)

    tbl = PrettyTable(["Name", "ImageId", "CreationDate"])
    tbl.align = "l"

    for ami in amis:
        tbl.add_row([ami['Name'], ami['ImageId'], ami['CreationDate']])

    click.echo(tbl.get_string(title="Scylla AMI versions"))


@cli.command('list-ami-branch', help="""list Amazon Scylla branched AMI versions
    \n\n[VERSION] is a branch version to look for, ex. 'branch-2019.1:latest', 'branch-3.1:all'""")
@click.option('-r', '--region', type=click.Choice(all_aws_regions(cached=True)), default='eu-west-1')
@click.argument('version', type=str, default='branch-3.1:all')
def list_ami_branch(region, version):
    add_file_logger()

    def get_tags(ami):
        return {i['Key']: i['Value'] for i in ami.tags}

    if ":" not in version:
        version += ":all"

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
@click.option('-d', '--dist-type', type=click.Choice(['centos', 'ubuntu', 'debian']),
              default='centos', help='Distribution type')
@click.option('-v', '--dist-version', type=click.Choice(['xenial', 'trusty', 'bionic', 'focal'  # Ubuntu
                                                         'jessie', 'stretch', 'buster']),       # Debian
              default=None, help='deb style versions')
def list_repos(dist_type, dist_version):
    add_file_logger()

    if not dist_type == 'centos' and dist_version is None:
        click.secho("when passing --dist-type=debian/ubuntu need to pass --dist-version as well", fg='red')
        sys.exit(1)

    repo_maps = get_s3_scylla_repos_mapping(dist_type, dist_version)

    tbl = PrettyTable(["Version Family", "Repo Url"])
    tbl.align = "l"

    for version_prefix, repo_url in repo_maps.items():
        tbl.add_row([version_prefix, repo_url])

    click.echo(tbl.get_string(title="Scylla Repos"))


@cli.command('output-conf', help="Output test configuration readed from the file")
@click.argument('config_files', type=str, default='')
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends))
def output_conf(config_files, backend):
    add_file_logger()

    if backend:
        os.environ['SCT_CLUSTER_BACKEND'] = backend
    if config_files:
        os.environ['SCT_CONFIG_FILES'] = config_files
    config = SCTConfiguration()
    click.secho(config.dump_config(), fg='green')
    sys.exit(0)


@cli.command(help="Check test configuration file")
@click.argument('config_file', type=str, default='')
@click.option('-b', '--backend', type=click.Choice(SCTConfiguration.available_backends), default='aws')
def conf(config_file, backend):
    add_file_logger()

    if backend:
        os.environ['SCT_CLUSTER_BACKEND'] = backend
    if config_file:
        os.environ['SCT_CONFIG_FILES'] = config_file
    config = SCTConfiguration()
    try:
        config.verify_configuration()
        config.check_required_files()
    except Exception as ex:  # pylint: disable=broad-except
        logging.exception(str(ex))
        click.secho(str(ex), fg='red')
        sys.exit(1)
    else:
        click.secho(config.dump_config(), fg='green')
        sys.exit(0)


@cli.command('conf-docs', help="Show all available configuration in yaml/markdown format")
@click.option('-o', '--output-format', type=click.Choice(["yaml", "markdown"]), default="yaml", help="type of the output")
def conf_docs(output_format):
    add_file_logger()

    os.environ['SCT_CLUSTER_BACKEND'] = "aws"  # just to pass SCTConfiguration() verification.

    config_logger = logging.getLogger('sdcm.sct_config')
    config_logger.setLevel(logging.ERROR)
    if output_format == 'markdown':
        click.secho(SCTConfiguration().dump_help_config_markdown())
    elif output_format == 'yaml':
        click.secho(SCTConfiguration().dump_help_config_yaml())


@cli.command("perf-regression-report", help="Generate and send performance regression report")
@click.option("-i", "--es-id", required=True, type=str, help="Id of the run in Elastic Search")
@click.option("-e", "--emails", required=True, type=str, help="Comma separated list of emails. Example a@b.com,c@d.com")
def perf_regression_report(es_id, emails):
    add_file_logger()

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
@click.option('-o', '--output-format', type=click.Choice(["table", "markdown"]), default="table", help="type of the output")
def show_log(test_id, output_format):
    add_file_logger()

    files = list_logs_by_test_id(test_id)

    if output_format == 'table':
        table = PrettyTable(["Date", "Log type", "Link"])
        table.align = "l"
        for log in files:
            table.add_row([log["date"].strftime("%Y%m%d_%H%M%S"), log["type"], log["link"]])
        click.echo(table.get_string(title="Log links for testrun with test id {}".format(test_id)))
    elif output_format == 'markdown':
        click.echo("\n## Logs\n")
        for log in files:
            click.echo(f'* **{log["type"]}** - {log["link"]}')


@investigate.command('show-monitor', help="Run monitoring stack with saved data locally")
@click.argument('test_id')
@click.option("--date-time", type=str, required=False, help='Datetime of monitor-set archive is collected')
@click.option("--kill", type=bool, required=False, help='Kill and remove containers')
def show_monitor(test_id, date_time, kill):
    add_file_logger()

    click.echo('Search monitoring stack archive files for test id {} and restoring...'.format(test_id))
    # if debug_log:
    #     LOGGER.setLevel(logging.DEBUG)
    try:
        status = restore_monitoring_stack(test_id, date_time)
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.error("%s", details)
        status = False

    table = PrettyTable(['Service', 'Container', 'Link'])
    table.align = 'l'
    if status:
        click.echo('Monitoring stack restored')
        for docker in get_monitoring_stack_services():
            table.add_row([docker['service'], docker["name"], 'http://localhost:{}'.format(docker["port"])])
        click.echo(table.get_string(title='Monitoring stack services'))
        if kill:
            kill_running_monitoring_stack_services()

    else:
        click.echo('Errors were found when restoring Scylla monitoring stack')
        kill_running_monitoring_stack_services()
        sys.exit(1)


@investigate.command('show-jepsen-results', help="Run a server with Jepsen results")
@click.argument('test_id')
def show_jepsen_results(test_id):
    add_file_logger()

    click.secho(message=f"\nSearch Jepsen results archive files for test id {test_id} and restoring...\n", fg="green")
    jepsen = JepsenResults()
    if jepsen.restore_jepsen_data(test_id):
        click.secho(
            message=f"\nJepsen data restored, starting web server on http://localhost:{jepsen.jepsen_results_port}/\n"
                    f"Press Ctrl-C to stop the server.\n",
            fg="green",
        )
        jepsen.run_jepsen_web_server()


@investigate.command('search-builder', help='Search builder where test run with test-id located')
@click.argument('test-id')
def search_builder(test_id):
    logging.getLogger("paramiko").setLevel(logging.CRITICAL)
    add_file_logger()

    results = get_builder_by_test_id(test_id)
    tbl = PrettyTable(['Builder Name', "Public IP", "path"])
    tbl.align = 'l'
    for result in results:
        tbl.add_row([result['builder']['name'], result['builder']['public_ip'], result['path']])

    click.echo(tbl.get_string(title='Found builders for Test-id: {}'.format(test_id)))


@investigate.command('show-events', help='Return content of file events_log/events for running job by test-id')
@click.argument('test-id')
@click.option("--follow", type=bool, required=False, is_flag=True, default=False,
              help="Follow job events log file (similar tail -f <file>)")
@click.option("--last-n", type=int, required=False, help="return last n lines from events.log file")
@click.option("--save-to", type=str, required=False, help="Download events.log file and save to provided dir")
def show_events(test_id: str, follow: bool = False, last_n: int = None, save_to: str = None):
    logging.getLogger("paramiko").setLevel(logging.CRITICAL)
    add_file_logger()
    builders = get_builder_by_test_id(test_id)

    if not builders:
        LOGGER.info("Builder was not found for provided test-id %s", test_id)

    for builder in builders:
        LOGGER.info(
            "Applying action for events.log on builder %s:%s...", builder['builder']['name'], builder['builder']['public_ip'])
        remoter = builder["builder"]["remoter"]

        if follow or last_n:
            options = "-f " if follow else ""
            options += f"-n {last_n} " if last_n else ""
            try:
                remoter.run(f"tail {options} {builder['path']}/events_log/events.log")
            except KeyboardInterrupt:
                LOGGER.info('Monitoring events.log for test-id %s stopped!', test_id)
        elif save_to:
            remoter.receive_files(f"{builder['path']}/events_log/events.log", save_to)
            LOGGER.info("Events saved to %s", save_to)
        else:
            remoter.run(f"cat {builder['path']}/events_log/events.log")
    click.echo("Show events done.")


cli.add_command(investigate)


@cli.command('unit-tests', help="Run all the SCT internal unit-tests")
@click.option("-t", "--test", required=False, default="",
              help="Run specific test file from unit-tests directory")
def unit_tests(test):
    sys.exit(pytest.main(['-v', '-p', 'no:warnings', 'unit_tests/{}'.format(test)]))


@cli.command('pre-commit', help="Run pre-commit checkers")
def pre_commit():
    result = 0
    result += os.system(
        'bash -c "git show -s --format=%B > /tmp/commit-msg; '
        'pre-commit run --hook-stage commit-msg --commit-msg-filename /tmp/commit-msg"'
    )
    result += os.system('pre-commit run -a --show-diff-on-failure')
    result = 1 if result else 0
    sys.exit(result)


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
    add_file_logger()

    email_list = emails.split(",")
    click.secho(message="Will send Cloud Usage report to %s" % email_list, fg="green")
    cloud_report(mail_to=email_list)
    click.secho(message="Done." % email_list, fg="yellow")


@cli.command('collect-logs', help='Collect logs from cluster by test-id')
@click.option('--test-id', help='Find cluster by test-id')
@click.option('--logdir', help='Path to directory with sct results')
@click.option('--backend', help='Cloud where search nodes', default=None)
@click.option('--config-file', type=str, help='config test file path')
def collect_logs(test_id=None, logdir=None, backend=None, config_file=None):
    add_file_logger()

    from sdcm.logcollector import Collector  # pylint: disable=import-outside-toplevel
    logging.getLogger("paramiko").setLevel(logging.CRITICAL)
    if backend is None:
        if os.environ.get('SCT_CLUSTER_BACKEND', None) is None:
            os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
    else:
        os.environ['SCT_CLUSTER_BACKEND'] = backend

    if config_file and not os.environ.get('SCT_CONFIG_FILES', None):
        os.environ['SCT_CONFIG_FILES'] = config_file

    config = SCTConfiguration()

    collector = Collector(test_id=test_id, params=config, test_dir=logdir)

    collected_logs = collector.run()

    table = PrettyTable(['Cluster set', 'Link'])
    table.align = 'l'
    for cluster_type, s3_links in collected_logs.items():
        for link in s3_links:
            table.add_row([cluster_type, link])

    click.echo(table.get_string(title="Collected logs by test-id: {}".format(collector.test_id)))


@cli.command('send-email', help='Send email with results for testrun')
@click.option('--test-id', help='Test-id of run')
@click.option('--test-status', help='Override test status FAILED|ABORTED')
@click.option('--start-time', help='Override test start time')
@click.option('--started-by', help='Default user that started the test')
@click.option('--email-recipients', help="Send email to next recipients")
@click.option('--logdir', help='Directory where to find testrun folder')
def send_email(test_id=None, test_status=None, start_time=None, started_by=None, email_recipients=None, logdir=None):  # pylint: disable=too-many-arguments,too-many-branches
    if started_by is None:
        started_by = get_username()
    add_file_logger()

    from sdcm.send_email import get_running_instances_for_email_report, read_email_data_from_file, build_reporter  # pylint: disable=import-outside-toplevel

    if not email_recipients:
        LOGGER.warning("No email recipients. Email will not be sent")
        return
    LOGGER.info('Email will be sent to next recipients: %s', email_recipients)
    if not logdir:
        logdir = os.path.expanduser('~/sct-results')
    test_results = None
    if start_time is None:
        start_time = format_timestamp(time.time())
    else:
        start_time = format_timestamp(int(start_time))
    testrun_dir = get_testrun_dir(test_id=test_id, base_dir=logdir)
    if testrun_dir:
        with open(os.path.join(testrun_dir, 'test_id'), 'r') as file:
            test_id = file.read().strip()
        email_results_file = os.path.join(testrun_dir, "email_data.json")
        test_results = read_email_data_from_file(email_results_file)
    else:
        LOGGER.warning("Failed to find test directory for %s", test_id)

    if test_results:
        reporter = test_results.get("reporter", "")
        test_results['nodes'] = get_running_instances_for_email_report(test_id)
    else:
        LOGGER.warning("Failed to read test results for %s", test_id)
        reporter = "TestAborted"
        if not test_status:
            test_status = 'ABORTED'
        test_results = {
            "job_url": os.environ.get("BUILD_URL"),
            "subject": f"{test_status}: {os.environ.get('JOB_NAME')}: {start_time}",
            "start_time": start_time,
            "end_time": format_timestamp(time.time()),
            "grafana_screenshots": "",
            "grafana_snapshots": "",
            "nodes": "",
            "test_id": "",
            "username": ""
        }
        if started_by:
            test_results["username"] = started_by
        if test_id:
            test_results.update({
                "test_id": test_id,
                "nodes": get_running_instances_for_email_report(test_id)
            })
    test_results['logs_links'] = list_logs_by_test_id(test_results.get('test_id', test_id))
    email_recipients = email_recipients.split(',')
    reporter = build_reporter(reporter, email_recipients, testrun_dir)
    if not reporter:
        LOGGER.warning("No reporter found")
        sys.exit(1)
    try:
        reporter.send_report(test_results)
    except Exception:  # pylint: disable=broad-except
        LOGGER.error("Failed to create email due to the following error:\n%s", traceback.format_exc())
        build_reporter("TestAborted", email_recipients, testrun_dir).send_report({
            "job_url": os.environ.get("BUILD_URL"),
            "subject": f"FAILED: {os.environ.get('JOB_NAME')}: {start_time}",
        })


@cli.command('create-test-release-jobs', help="Create pipeline jobs for a new branch")
@click.argument('branch', type=str)
@click.argument('username', envvar='JENKINS_USERNAME', type=str)
@click.argument('password', envvar='JENKINS_PASSWORD', type=str)
@click.option('--sct_branch', default='master', type=str)
@click.option('--sct_repo', default='git@github.com:scylladb/scylla-cluster-tests.git', type=str)
def create_test_release_jobs(branch, username, password, sct_branch, sct_repo):
    add_file_logger()

    base_job_dir = f'{branch}'
    server = JenkinsPipelines(username=username, password=password, base_job_dir=base_job_dir,
                              sct_branch_name=sct_branch, sct_repo=sct_repo)

    for group_name, group_desc in [('longevity', 'SCT Longevity Tests'),
                                   ('rolling-upgrade', 'SCT Rolling Upgrades'),
                                   ('gemini-', 'SCT Gemini Tests'),
                                   ('features-', 'SCT Feature Tests'),
                                   ('artifacts', 'SCT Artifacts Tests'),
                                   ('load-test', 'SCT Load Tests'),
                                   ('k8s', 'SCT Kubernetes Tests'),
                                   ]:
        server.create_directory(name=group_name, display_name=group_desc)

        for jenkins_file in glob.glob(f'{server.base_sct_dir}/jenkins-pipelines/{group_name}*.jenkinsfile'):
            server.create_pipeline_job(jenkins_file, group_name)

        if group_name == 'load-test':
            for jenkins_file in glob.glob(f'{server.base_sct_dir}/jenkins-pipelines/admission_control_overload*'):
                server.create_pipeline_job(jenkins_file, group_name)

    server.create_directory(name='artifacts-offline-install', display_name='SCT Artifacts Offline Install Tests')
    for jenkins_file in glob.glob(f'{server.base_sct_dir}/jenkins-pipelines/artifacts-*.jenkinsfile'):
        if any((f'-{i}.jenkinsfile' in jenkins_file for i in ['ami', 'amazon2', 'docker', 'gce-image'])):
            continue
        server.create_pipeline_job(jenkins_file, 'artifacts-offline-install')
    for jenkins_file in glob.glob(f'{server.base_sct_dir}/jenkins-pipelines/nonroot-offline-install/*.jenkinsfile'):
        server.create_pipeline_job(jenkins_file, 'artifacts-offline-install',
                                   job_name=str(Path(jenkins_file).stem) + '-nonroot')


@cli.command('create-test-release-jobs-enterprise', help="Create pipeline jobs for a new branch")
@click.argument('branch', type=str)
@click.argument('username', envvar='JENKINS_USERNAME', type=str)
@click.argument('password', envvar='JENKINS_PASSWORD', type=str)
@click.option('--sct_branch', default='master', type=str)
@click.option('--sct_repo', default='git@github.com:scylladb/scylla-cluster-tests.git', type=str)
def create_test_release_jobs_enterprise(branch, username, password, sct_branch, sct_repo):
    add_file_logger()

    base_job_dir = f'{branch}'
    server = JenkinsPipelines(username=username, password=password, base_job_dir=base_job_dir,
                              sct_branch_name=sct_branch, sct_repo=sct_repo)

    server.create_directory('SCT_Enterprise_Features', 'SCT Enterprise Features')
    for group_name, match, group_desc in [
        ('EncryptionAtRest', 'EaR-*', 'Encryption At Rest'),
        ('ICS', '*ics*', 'ICS'),
        ('Workload_Prioritization', 'features-sla-*', 'Workload Prioritization')
    ]:
        current_dir = f'SCT_Enterprise_Features/{group_name}'
        server.create_directory(name=current_dir, display_name=group_desc)

        for jenkins_file in glob.glob(f'{server.base_sct_dir}/jenkins-pipelines/{match}.jenkinsfile'):
            server.create_pipeline_job(jenkins_file, current_dir)

    server.create_pipeline_job(
        f'{server.base_sct_dir}/jenkins-pipelines/longevity-in-memory-36gb-1d.jenkinsfile', 'SCT_Enterprise_Features')


@cli.command("prepare-aws-region", help="Create and configure VPC in selected AWS region")
@click.option("-r", "--region", required=True, type=str, help="Name of the region")
def prepare_aws_region(region):
    add_file_logger()
    aws_region = AwsRegion(region_name=region)
    aws_region.configure()


@cli.command("create-runner-image", help="Create an SCT runner image in selected AWS region. "
                                         f"If the requested region is not a source region "
                                         f"({SctRunner.SOURCE_IMAGE_REGION}) the image will be first created in the"
                                         f" source region and then copied to the chosen one.")
@click.option("-r", "--region", required=True, type=click.Choice(all_aws_regions(cached=True)),
              help="Name of the region")
def create_runner_image(region):
    add_file_logger()
    sct_runner = SctRunner(region_name=region)
    sct_runner.create_image()


@cli.command("create-runner-instance", help="Create an SCT runner instance in selected AWS region")
@click.option("-c", "--cloud-provider", required=True, type=str, default="aws",
              help="Cloud provider, currently only AWS is supported")
@click.option("-r", "--region", required=True, type=click.Choice(all_aws_regions(cached=True)),
              help="Name of the region")
@click.option("-z", "--availability-zone", required=False, default="", type=str,
              help="Name of availability zone, ex. 'a'")
@click.option("-t", "--test-id", required=True, type=str, help="Test ID")
@click.option("-d", "--duration", required=True, type=int, help="Test duration in MINUTES")
def create_runner_instance(cloud_provider, region, availability_zone, test_id, duration):
    assert cloud_provider.lower() == "aws", "Only AWS is supported"
    assert len(availability_zone) == 1, f"Invalid AZ: {availability_zone}"
    add_file_logger()
    sct_runner_ip_path = Path("sct_runner_ip")
    sct_runner_ip_path.unlink(missing_ok=True)
    sct_runner = SctRunner(region_name=region)
    instance = sct_runner.create_instance(test_id=test_id, test_duration=duration, region_az=region + availability_zone)
    LOGGER.info("Verifying SSH connectivity...")
    remoter = sct_runner.get_remoter(host=instance.public_ip_address, connect_timeout=120)
    result = remoter.run("true", timeout=100, verbose=False, ignore_status=True)
    if result.exit_status == 0:
        LOGGER.info("Successfully connected the SCT Runner. Public IP: %s", instance.public_ip_address)
        with sct_runner_ip_path.open("w") as sct_runner_ip_file:
            sct_runner_ip_file.write(instance.public_ip_address)
    else:
        LOGGER.error("Unable to SSH to %s! Exiting...", instance.public_ip_address)
        sys.exit(1)


@cli.command("clean-runner-instances", help="Clean all unused SCT runner instances")
def clean_runner_instances():
    add_file_logger()
    clean_sct_runners()


if __name__ == '__main__':
    cli()
