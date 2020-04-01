import smtplib
import os.path
import logging
import tempfile
import json
from typing import Optional, Sequence, Tuple
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import jinja2

from sdcm.keystore import KeyStore
from sdcm.utils.common import list_instances_gce, list_instances_aws, list_resources_docker
from sdcm.utils.decorators import cached_property


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
    #  pylint: disable=too-many-instance-attributes
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

    def prepare_email(self, subject, content, recipients, html=True, files=()):  # pylint: disable=too-many-arguments
        msg = MIMEMultipart()
        msg['subject'] = subject
        msg['from'] = self.sender
        if recipients:
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

    def send(self, subject, content, recipients, html=True, files=()):  # pylint: disable=too-many-arguments
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


class BaseEmailReporter():
    COMMON_EMAIL_FIELDS = ("build_url",
                           "end_time",
                           "events_summary",
                           "last_events",
                           "nodes",
                           "start_time",
                           "subject",
                           "test_id",
                           "test_name",
                           "test_status",
                           "username",)
    _fields = ()
    email_template_file = 'results_base.html'
    last_events_limit = 5
    last_events_body_limit = 400

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

    def render_to_html(self, results, template_str=None):
        """
        Render analysis results to html template
        :param results: results dictionary
        :param template_str: template string
        :return: html string
        """
        self.log.info("Rendering results to html using '%s' template...", self.email_template_fp)
        loader = jinja2.FileSystemLoader(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report_templates'))
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'])
        if template_str is None:
            template = env.get_template(self.email_template_fp)
        else:
            template = env.from_string(template_str)
        html = template.render(results)
        self.log.info("Results has been rendered to html")
        return html

    def save_html_to_file(self, results, html_file_path="", template_str=None):
        if html_file_path:
            html = self.render_to_html(results, template_str=template_str)
            with open(html_file_path, "wb") as html_file:
                html_file.write(html.encode('utf-8'))
            self.log.info("HTML report saved to '%s'.", html_file_path)
        else:
            self.log.error("File for HTML report is missing")

    def send_email(self, email):
        if not self.email_recipients:
            self.log.warning(f'Email recipient is not set, not sending report')
            return
        try:
            Email().send_email(recipients=self.email_recipients, email=email)
        except Exception as details:  # pylint: disable=broad-except
            self.log.error("Error during sending email: %s", details, exc_info=True)
        finally:
            self.log.info(f'Send email with results to {self.email_recipients}')

    def send_report(self, results):
        if not self.email_recipients:
            self.log.warning(f'Email recipient is not set, not sending report')
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
        report_data['last_events'] = self._get_last_events(report_data)
        return self.render_to_html(report_data)

    @staticmethod
    def build_report_attachments(attachments_data, template_str=None):  # pylint: disable=unused-argument
        return ()

    @staticmethod
    def cut_report_data(report_data, attachments_data, reason):  # pylint: disable=unused-argument
        if attachments_data is not None:
            return report_data, None
        return None, None

    def _get_last_events(self, report_data):
        output = {}
        last_events = report_data.get('last_events')
        if last_events and isinstance(last_events, dict):
            for severity, events in last_events.items():
                if not events:
                    output[severity] = ['No events in this category']
                    continue
                output[severity] = severity_events = []
                for event in reversed(events):
                    severity_events.insert(0, event[:self.last_events_body_limit])
                    if len(severity_events) >= self.last_events_limit:
                        severity_events.append('There are more events. See log file.')
                        break
        return output


class LongevityEmailReporter(BaseEmailReporter):
    _fields = ("grafana_screenshots",
               "grafana_snapshots",
               "nemesis_details",
               "nemesis_name",
               "number_of_db_nodes",
               "scylla_ami_id",
               "scylla_instance_type",
               "scylla_version",)
    email_template_file = "results_longevity.html"

    def cut_report_data(self, report_data, attachments_data, reason):
        if attachments_data is not None:
            return report_data, None
        return None, None

    def build_report(self, report_data):
        report_data['short_report'] = True
        return super().build_report(report_data)

    def build_report_attachments(self, attachments_data, template_str=None):
        report_file = os.path.join(self.logdir, 'email_report.html')
        attachments_data['last_events'] = self._get_last_events(attachments_data)
        self.save_html_to_file(attachments_data, report_file, template_str=template_str)
        attachments = (report_file, )
        return attachments


class GeminiEmailReporter(BaseEmailReporter):
    _fields = ("gemini_cmd",
               "gemini_version",
               "nemesis_details",
               "nemesis_name",
               "number_of_db_nodes",
               "number_of_oracle_nodes",
               "oracle_ami_id",
               "oracle_db_version",
               "oracle_instance_type",
               "results",
               "scylla_ami_id",
               "scylla_instance_type",
               "scylla_version",
               "status",)
    email_template_file = "results_gemini.html"


class UpgradeEmailReporter(BaseEmailReporter):
    _fields = ("number_of_db_nodes",
               "scylla_ami_id",
               "scylla_instance_type",
               "scylla_version",)
    email_template_file = "results_upgrade.html"


class ArtifactsEmailReporter(BaseEmailReporter):
    _fields = ("backend",
               "region_name",
               "scylla_instance_type",
               "scylla_node_image",
               "scylla_packages_installed",
               "scylla_repo",
               "scylla_version",)
    email_template_file = "results_artifacts.html"


class PrivateRepoEmailReporter(BaseEmailReporter):
    _fields = ("repo_ostype",
               "repo_uuid",
               "scylla_repo",)
    email_template_file = "results_private_repo.html"


def build_reporter(name: str,
                   email_recipients: Sequence[str] = (),
                   logdir: Optional[str] = None) -> Optional[BaseEmailReporter]:

    if "Gemini" in name:
        return GeminiEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Longevity" in name:
        return LongevityEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Upgrade" in name:
        return UpgradeEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Artifacts" in name:
        return ArtifactsEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "PrivateRepo" in name:
        return PrivateRepoEmailReporter(email_recipients=email_recipients, logdir=logdir)
    else:
        return None


def get_running_instances_for_email_report(test_id):
    """Get running instances left after testrun

    Get all running instances leff after testrun is done
    :param test_id: testrun test id
    :type test_id: str
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
            name = [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'Name']
            nodes.append([name[0],
                          instance.get('PublicIpAddress', 'N/A'),
                          instance['State']['Name'],
                          "aws",
                          region])
    instances = list_instances_gce(tags_dict=tags, running=True)
    for instance in instances:
        nodes.append([instance.name,
                      ", ".join(instance.public_ips) if None not in instance.public_ips else "N/A",
                      instance.state,
                      "gce",
                      instance.extra["zone"].name])
    resources = list_resources_docker(tags_dict=tags, running=True, group_as_builder=True)
    for builder_name, containers in resources.get("containers", {}).items():
        for container in containers:
            nodes.append([container.name,
                          container.attrs["NetworkSettings"]["IPAddress"],
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
            with open(filename, "r") as fp:  # pylint: disable=invalid-name
                email_data = json.load(fp)  # pylint: disable=invalid-name
        except Exception as details:  # pylint: disable=broad-except
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
            with open(filepath, "w") as json_file:
                json.dump(email_data, json_file)
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.warning("Error during collecting data for email %s", details)
