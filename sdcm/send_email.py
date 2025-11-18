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
# Copyright (c) 2020 ScyllaDB

import smtplib
import os.path
import subprocess
import logging
import tempfile
import json
import copy
import traceback
from typing import Optional, Sequence, Tuple
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import cached_property

import jinja2

from sdcm.keystore import KeyStore
from sdcm.utils.common import list_instances_gce, list_instances_aws, list_resources_docker, format_timestamp
from sdcm.utils.gce_utils import gce_public_addresses
from sdcm.utils.docker_utils import get_ip_address_of_container

LOGGER = logging.getLogger(__name__)


class AttachementSizeExceeded(Exception):
    def __init__(self, current_size, limit):
        self.current_size = current_size
        self.limit = limit
        super().__init__()


class BodySizeExceeded(Exception):
    def __init__(self, current_size, limit):
        self.current_size = current_size
        self.limit = limit
        super().__init__()


class Email():
    """
    Responsible for sending emails
    """
    _attachments_size_limit = 10485760  # 10Mb = 20 * 1024 * 1024
    _body_size_limit = 26214400  # 25Mb = 20 * 1024 * 1024

    def __init__(self):
        self.sender = "qa@scylladb.com"
        self._password = ""
        self._user = ""
        self._server_host = "smtp.gmail.com"
        self._server_port = "587"
        self._conn = None
        self._retrieve_credentials()
        self._connect()

    def _retrieve_credentials(self):
        keystore = KeyStore()
        creds = keystore.get_email_credentials()
        self._user = creds["user"]
        self._password = creds["password"]

    def _connect(self):
        self.conn = smtplib.SMTP(host=self._server_host, port=self._server_port)
        self.conn.ehlo()
        self.conn.starttls()
        self.conn.login(user=self._user, password=self._password)

    def prepare_email(self, subject, content, recipients, html=True, files=()):
        msg = MIMEMultipart()
        msg['subject'] = subject
        msg['from'] = self.sender
        assert recipients, "No recipients provided"
        msg['to'] = ','.join(recipients)
        if html:
            text_part = MIMEText(content, "html")
        else:
            text_part = MIMEText(content, "plain")
        msg.attach(text_part)
        attachment_size = 0
        for path in files:
            attachment_size += os.path.getsize(path)
            with open(path, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=os.path.basename(path)
                )
            part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(path)
            msg.attach(part)
        if attachment_size >= self._attachments_size_limit:
            raise AttachementSizeExceeded(current_size=attachment_size, limit=self._attachments_size_limit)
        email = msg.as_string()
        if len(email) >= self._body_size_limit:
            raise BodySizeExceeded(current_size=len(email), limit=self._body_size_limit)
        return email

    def send(self, subject, content, recipients, html=True, files=()):
        """
        :param subject: text
        :param content: text/html
        :param recipients: iterable, list of recipients
        :param html: True/False
        :param files: paths of the files that will be attached to the email
        :return:
        """
        email = self.prepare_email(subject, content, recipients, html, files)
        self.send_email(recipients, email)

    def send_email(self, recipients, email):
        self.conn.sendmail(self.sender, recipients, email)

    def __del__(self):
        self.conn.quit()


class BaseEmailReporter:
    COMMON_EMAIL_FIELDS = (
        "backend",
        "build_id",
        "job_url",
        "config_files",
        "end_time",
        "events_summary",
        "job_name",
        "last_events",
        "logs_links",
        "nodes",
        "number_of_db_nodes",
        "live_nodes_shards",
        "dead_nodes_shards",
        "region_name",
        "scylla_instance_type",
        "scylla_version",
        "relocatable_pkg",
        "kernel_version",
        "start_time",
        "subject",
        "test_id",
        "test_name",
        "test_status",
        "username",
        "shard_awareness_driver",
        "rack_aware_policy",
        "restore_monitor_job_base_link",
    )
    _fields = ()
    email_template_file = 'results_base.html'
    last_events_body_limit_per_severity = 35000
    last_events_body_limit_total = 70000
    last_events_limit = 10000000  # This limit won't be reached, to be relay only on body limits
    last_events_severities = ['CRITICAL', 'ERROR', 'WARNING']

    def __init__(self, email_recipients=(), email_template_fp=None, logger=None, logdir=None):
        self.email_recipients = email_recipients
        self.email_template_fp = email_template_fp if email_template_fp else self.email_template_file
        self.log = logger if logger else LOGGER
        self.logdir = logdir if logdir else tempfile.mkdtemp()

    @cached_property
    def fields(self) -> Tuple[str, ...]:
        return self.COMMON_EMAIL_FIELDS + self._fields

    def build_data_for_report(self, results):
        return {key: results.get(key, "N/A") for key in self.fields}

    def build_data_for_attachments(self, results):
        return {key: results.get(key, "N/A") for key in self.fields}

    def render_to_html(self, results, template_str=None, template_file=None):
        """
        Render analysis results to html template_init_es
        :param results: results dictionary
        :param template_str: template string
        :param template_file: template file (instead of default self.email_template_fp). If template_str is supplied,
                              template_file will be ignored
        :return: html string
        """
        if not template_str:
            current_template = self.email_template_fp if not template_file else template_file
        else:
            current_template = self.email_template_fp

        self.log.info("Rendering results to html using '%s' template...", current_template)
        loader = jinja2.FileSystemLoader(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report_templates'))
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'])
        env.filters["format_timestamp"] = format_timestamp
        if template_str is None:
            template = env.get_template(current_template)
        else:
            template = env.from_string(template_str)
        html = template.render(results)
        self.log.info("Results has been rendered to html")
        return html

    def save_html_to_file(self, results, html_file_path="", template_str=None, template_file=None):
        if html_file_path:
            html = self.render_to_html(results, template_str=template_str, template_file=template_file)
            with open(html_file_path, "wb") as html_file:
                html_file.write(html.encode('utf-8'))
            self.log.info("HTML report saved to '%s'.", html_file_path)
        else:
            self.log.error("File for HTML report is missing")

    def send_email(self, email):
        if not self.email_recipients:
            self.log.warning('Email recipient is not set, not sending report')
            return
        try:
            Email().send_email(recipients=self.email_recipients, email=email)
        except Exception as details:
            self.log.error("Error during sending email: %s", details, exc_info=True)
        finally:
            self.log.info('Send email with results to %s', self.email_recipients)

    def send_report(self, results):
        if not self.email_recipients:
            self.log.warning('Email recipient is not set, not sending report')
            return
        report_data = self.build_data_for_report(results)
        attachments_data = self.build_data_for_attachments(results)
        smtp = Email()
        # Generating email
        email = None
        for _ in range(4):
            try:
                report = self._generate_report(report_data)
                attachments = self._generate_report_attachments(attachments_data)
                email = smtp.prepare_email(
                    subject=report_data['subject'],
                    recipients=self.email_recipients,
                    content=report,
                    files=attachments)
                break
            except (AttachementSizeExceeded, BodySizeExceeded) as exc:
                report_data, attachments_data = self.cut_report_data(report_data, attachments_data, reason=exc)
        # Sending prepared email
        if email is None:
            self.log.error("Failed to prepare email", exc_info=True)
            return
        self.send_email(email)

    def _generate_report_attachments(self, attachments_data):
        if attachments_data is None:
            attachment_file = os.path.join(self.logdir, 'attachment_excluded.html')
            self.save_html_to_file(
                {},
                html_file_path=attachment_file,
                template_str='<html><body>Attachment was excluded due to the size limitation</body></html>')
            return (attachment_file,)
        elif attachments_data:
            return self.build_report_attachments(attachments_data)
        else:
            return tuple()

    def _generate_report(self, report_data):
        if report_data is None:
            return self.render_to_html(
                {},
                '<html><body>Report was not sent due to the size limitation</body></html>')
        return self.build_report(report_data)

    def build_report(self, report_data):
        self.log.info("Prepare result to send in email")
        report_data['last_events'] = self._get_last_events(
            report_data,
            self.last_events_body_limit_per_severity,
            self.last_events_body_limit_total,
            self.last_events_limit,
            self.last_events_severities
        )
        return self.render_to_html(report_data)

    @staticmethod
    def build_report_attachments(attachments_data, template_str=None):
        return ()

    @staticmethod
    def cut_report_data(report_data, attachments_data, reason):
        if attachments_data is not None:
            return report_data, None
        return None, None

    @staticmethod
    def _get_last_events(report_data, category_size_limit, total_size_limit, events_in_category_limit, severities):
        output = {}
        total_size = 0
        last_events = report_data.get('last_events')
        if last_events and isinstance(last_events, dict):
            for severity, events in last_events.items():
                if severity not in severities:
                    continue
                severity_events_length = 0
                if not events:
                    output[severity] = ['No events with this severity']
                    continue
                output[severity] = severity_events = []
                for event in reversed(events):
                    if len(event) >= category_size_limit - severity_events_length or \
                            len(severity_events) >= events_in_category_limit or \
                            total_size + len(event) > total_size_limit:
                        severity_events.append('There are more events. See log file.')
                        break
                    severity_events.insert(0, event)
                    severity_events_length += len(event)
                    total_size += len(event)
                if len(severity_events) == 0 and len(events) >= 1:
                    severity_events.append(events[-1][category_size_limit])
                    severity_events.append('There are more events. See log file.')
        return output

    @staticmethod
    def _check_if_last_events_over_the_limit(report_data, category_size_limit, total_size_limit, events_in_category_limit):
        total_size = 0
        last_events = report_data.get('last_events')
        if last_events and isinstance(last_events, dict):
            for events in last_events.values():
                severity_events_length = 0
                for event in reversed(events):
                    if len(event) >= category_size_limit - severity_events_length or \
                            severity_events_length >= events_in_category_limit or \
                            total_size + len(event) > total_size_limit:
                        return True
                    severity_events_length += len(event)
                    total_size += len(event)
        return False


class ManagerUpgradeEmailReporter(BaseEmailReporter):
    _fields = (
        "manager_server_repo",
        "manager_agent_repo",
        "target_manager_server_repo",
        "target_manager_agent_repo",
    )
    email_template_file = "results_manager_upgrade.html"


class ManagerEmailReporter(BaseEmailReporter):
    _fields = (
        "manager_server_repo",
        "manager_agent_repo",
        "agent_backup_config",
        "restore_parameters",
        "backup_time",
        "restore_time",
    )
    email_template_file = "results_manager.html"


class LongevityEmailReporter(BaseEmailReporter):
    _fields = (
        "grafana_screenshots",
        "nemesis_details",
        "nemesis_name",
        "scylla_ami_id",
        "node_benchmarks",
        "parallel_timelines_report",
    )
    email_template_file = "results_longevity.html"
    last_events_body_limit_per_severity_in_attachment = 3000000
    last_events_body_limit_total_in_attachment = 10000000
    last_events_limit_in_attachment = 10000000  # This limit won't be reached, to be relay only on body limits
    last_events_severities_in_attachment = BaseEmailReporter.last_events_severities + ['NORMAL']

    def cut_report_data(self, report_data, attachments_data, reason):
        if attachments_data is not None:
            if self._check_if_last_events_over_the_limit(
                    attachments_data,
                    self.last_events_body_limit_per_severity_in_attachment // 3,
                    self.last_events_body_limit_total_in_attachment // 3,
                    self.last_events_limit_in_attachment // 3):
                attachments_data['last_events'] = self._get_last_events(
                    attachments_data,
                    self.last_events_body_limit_per_severity_in_attachment // 3,
                    self.last_events_body_limit_total_in_attachment // 3,
                    self.last_events_limit_in_attachment // 3,
                    self.last_events_severities_in_attachment)
                return report_data, attachments_data
            return report_data, None
        if self._check_if_last_events_over_the_limit(
                report_data,
                self.last_events_body_limit_per_severity // 3,
                self.last_events_body_limit_total // 3,
                self.last_events_limit // 3):
            report_data['last_events'] = self._get_last_events(
                report_data,
                self.last_events_body_limit_per_severity // 3,
                self.last_events_body_limit_total // 3,
                self.last_events_limit // 3,
                self.last_events_severities)
            return report_data, None
        return None, None

    def build_report(self, report_data):
        report_data['short_report'] = True
        return super().build_report(report_data)

    def build_report_attachments(self, attachments_data, template_str=None):
        attachments = (self.build_email_report(attachments_data, template_str),
                       self.build_issue_template(attachments_data))
        return attachments

    def build_email_report(self, attachments_data, template_str=None):
        report_file = os.path.join(self.logdir, 'email_report.html')
        attachments_data['last_events'] = self._get_last_events(
            attachments_data,
            self.last_events_body_limit_per_severity_in_attachment,
            self.last_events_body_limit_total_in_attachment,
            self.last_events_limit_in_attachment,
            self.last_events_severities_in_attachment)
        self.save_html_to_file(attachments_data, report_file, template_str=template_str)
        return report_file

    def build_issue_template(self, attachments_data):
        report_file = os.path.join(self.logdir, 'issue_template.html')
        template_file = 'results_issue_template.html'
        attachments_data["config_files_link"] = self.get_config_file_link(attachments_data)
        self.save_html_to_file(attachments_data, report_file, template_file=template_file)
        return report_file

    @staticmethod
    def get_config_file_link(attachments_data):
        config_files = []

        if "N/A" in attachments_data["config_files"]:
            return config_files

        for config_file in attachments_data["config_files"]:
            if not config_file:
                continue

            if 'cloud' in config_file or 'cloud' in attachments_data["job_name"]:
                config_files.append({"file": f"{config_file.split('/')[-1]} ",
                                     "link": "Siren repo"})
                continue

            last_commit = subprocess.run(['git', 'rev-list', 'HEAD', '-1', config_file], capture_output=True,
                                         text=True, check=True)
            config_files.append({"file": f"[{config_file.split('/')[-1]}]",
                                 "link": f"https://github.com/scylladb/scylla-cluster-tests/blob"
                                 f"/{last_commit.stdout.strip()}/{config_file}"})

        return config_files


class GeminiEmailReporter(LongevityEmailReporter):
    _fields = (
        "gemini_cmd",
        "gemini_version",
        "nemesis_details",
        "nemesis_name",
        "number_of_oracle_nodes",
        "oracle_ami_id",
        "oracle_db_version",
        "oracle_instance_type",
        "results",
        "scylla_ami_id",
        "status",
        "grafana_screenshots",
    )
    email_template_file = "results_gemini.html"


class FunctionalEmailReporter(LongevityEmailReporter):
    _fields = (
        "test_statuses",
        "results",
        "status"
    )
    email_template_file = "results_functional.html"


class ScaleUpEmailReporter(LongevityEmailReporter):
    _fields = (
        "grafana_screenshots",
        "ingest_time",
        "rebuild_duration",
    )
    email_template_file = "results_scale_up.html"


class UpgradeEmailReporter(BaseEmailReporter):
    _fields = (
        "grafana_screenshots",
        "new_scylla_repo",
        "new_version",
        "scylla_ami_id",
    )
    email_template_file = "results_upgrade.html"


class ArtifactsEmailReporter(BaseEmailReporter):
    _fields = (
        "scylla_node_image",
        "scylla_packages_installed",
        "scylla_repo",
    )
    email_template_file = "results_artifacts.html"


class TestAbortedEmailReporter(LongevityEmailReporter):
    email_template_file = "results_aborted.html"


class CDCReplicationReporter(LongevityEmailReporter):
    _fields = (
        "grafana_screenshots",
        "nemesis_details",
        "nemesis_name",
        "scylla_ami_id",
        "oracle_ami_id",
        "oracle_db_version",
        "oracle_instance_type",
        "number_of_oracle_nodes",
        "consistency_status",
    )
    email_template_file = "results_cdcreplication.html"


class JepsenEmailReporter(BaseEmailReporter):
    _fields = (
        "grafana_screenshots",
        "jepsen_report",
        "jepsen_scylla_repo",
        "jepsen_test_cmd",
        "scylla_repo",
    )
    email_template_file = "results_jepsen.html"

    @staticmethod
    def build_report_attachments(attachments_data, template_str=None):
        return (attachments_data["jepsen_report"], )


class SlaPerUserEmailReporter(LongevityEmailReporter):
    _fields = (
        "grafana_screenshots",
        "scylla_ami_id",
        "parallel_timelines_report",
        "workload_comparison"
    )
    email_template_file = "results_sl_workloads.html"


class ClusterConfigurationTestsReporter(BaseEmailReporter):
    pass


class SnitchEmailReporter(BaseEmailReporter):
    pass


# Alternator's performance tests need only vanilla processing, hence empty class, which inherits functionality from base
class PerformanceRegressionAlternatorTestEmailReporter(BaseEmailReporter):
    pass


class PerfSimpleQueryReporter(BaseEmailReporter):
    _fields = (
        "subject",
        "testrun_id",
        "test_stats",
        "last_results_table",
        "scylla_date_results_table",
        "job_url",
        "test_version",
        "collect_last_results_count",
        "collect_last_scylla_date_count",
        "deviation_diff",
        "is_deviation_within_limits"
    )
    email_template_file = "results_perf_simple_query.html"


def build_reporter(name: str,  # noqa: PLR0911
                   email_recipients: Sequence[str] = (),
                   logdir: Optional[str] = None) -> Optional[BaseEmailReporter]:
    LOGGER.info("Building email reporter for class: %s", name)
    if "Gemini" in name:
        return GeminiEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Longevity" in name:
        return LongevityEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "SlaPerUser" in name:
        return SlaPerUserEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "ManagerUpgrade" in name:
        return ManagerUpgradeEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Upgrade" in name and "PerformanceRegression" not in name:
        return UpgradeEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Artifacts" in name:
        return ArtifactsEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Manager" in name:
        return ManagerEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "TestAborted" in name:
        return TestAbortedEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "CDCReplication" in name:
        return CDCReplicationReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Jepsen" in name:
        return JepsenEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Functional" in name:
        return FunctionalEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "ClusterConfigurationTests" in name:
        return ClusterConfigurationTestsReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Snitch" in name:
        return SnitchEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "PerfSimpleQuery" in name:
        return PerfSimpleQueryReporter(email_recipients=email_recipients, logdir=logdir)
    elif "ScaleUp" in name:
        return ScaleUpEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "PerformanceRegressionAlternatorTest" in name:
        return PerformanceRegressionAlternatorTestEmailReporter(email_recipients=email_recipients, logdir=logdir)
    else:
        return None


def get_running_instances_for_email_report(test_id: str, ip_filter: str = None):
    """Get running instances left after testrun

    Get all running instances leff after testrun is done.
    If ip_filter is provided, the instance with that ip will be
    filtered out of the results.
    :param test_id: testrun test id
    :type test_id: str
    :param ip_filter: the ip of the sct test runner to be excluded
    from the report, since it will be terminated on test end
    :type ip_filter: str
    :returns: list of instances left running after test run
    in format:
    [
        ["name", "public ip addrs", "state", "cloud", "region"]
    ]
    :rtype: {list}
    """
    nodes = []

    tags = {"TestId": test_id, }

    instances = list_instances_aws(tags_dict=tags, group_as_region=True, running=True)
    for region in instances:
        for instance in instances[region]:
            # NOTE: K8S nodes created by autoscaler never have 'Name' set.
            name = 'N/A'
            for tag in instance['Tags']:
                if tag['Key'] == 'Name':
                    name = tag['Value']
                    break
            public_ip_addr = instance.get('PublicIpAddress', 'N/A')
            if public_ip_addr != ip_filter:
                nodes.append([name,
                              instance.get('PublicIpAddress', 'N/A'),
                              instance['State']['Name'],
                              "aws",
                              region])
    instances = list_instances_gce(tags_dict=tags, running=True)
    for instance in instances:
        public_ips = gce_public_addresses(instance)
        if ip_filter not in public_ips:
            nodes.append([instance.name,
                          ", ".join(public_ips) if public_ips else "N/A",
                          instance.status.lower(),
                          "gce",
                          instance.zone.split('/')[-1]])
    resources = list_resources_docker(tags_dict=tags, running=True, group_as_builder=True)
    for builder_name, containers in resources.get("containers", {}).items():
        for container in containers:
            container.reload()
            nodes.append([container.name,
                          get_ip_address_of_container(container),
                          container.status,
                          "docker container",
                          builder_name])
    for builder_name, images in resources.get("images", {}).items():
        for image in images:
            nodes.append([", ".join(image.tags),
                          "N/A",
                          "N/A",
                          "docker image",
                          builder_name])
    return nodes


def send_perf_email(reporter, test_results, logs, email_recipients, testrun_dir, start_time):
    for subject, content in test_results.items():
        if 'email_body' not in content:
            content['email_body'] = {}
        if 'attachments' not in content:
            content['attachments'] = []
        if 'template' not in content:
            content['template'] = None
        email_content = copy.deepcopy(content)
        email_content['email_body']['logs_links'] = logs
        html = reporter.render_to_html(email_content['email_body'],
                                       template=email_content['template'])
        try:
            reporter.send_email(subject=subject, content=html, files=email_content['attachments'])
        except Exception:  # noqa: BLE001
            LOGGER.error("Failed to create email due to the following error:\n%s", traceback.format_exc())
            build_reporter("TestAborted", email_recipients, testrun_dir).send_report({
                "job_url": os.environ.get("BUILD_URL"),
                "subject": f"FAILED: {os.environ.get('JOB_NAME')}: {start_time}",
            })


def read_email_data_from_file(filename):
    """read email data from file

    During teardown ClusterTester wrote email data
    to file email_data.json
    :param filename: absolute path to email_data.json file
    :type filename: str
    :returns: dict read from json data
    :rtype: {dict}
    """
    email_data = None
    if os.path.exists(filename):
        try:
            with open(filename, encoding="utf-8") as file:
                data = file.read().strip()
                email_data = json.loads(data or '{}')
        except Exception as details:  # noqa: BLE001
            LOGGER.warning("Error during read email data file %s: %s", filename, details)
    return email_data


def save_email_data_to_file(email_data, filepath):
    """Save email data to file

    Collecte email data save to json file
    :param email_data: dict collected by ClusterTester.get_email_data
    :type email_data: dict
    :param filepath: absolute path to file where data will be written
    :type filepath: str
    """
    try:
        if email_data:
            with open(filepath, "w", encoding="utf-8") as json_file:
                json.dump(email_data, json_file)
    except Exception as details:  # noqa: BLE001
        LOGGER.warning("Error during collecting data for email %s", details)
