import smtplib
import os.path
import logging
import tempfile
import json
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import jinja2

from sdcm.keystore import KeyStore
from sdcm.utils.common import list_instances_gce, list_instances_aws

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
    #  pylint: disable=unused-argument, no-self-use
    fields = []
    email_template_file = 'results_base.html'

    def __init__(self, email_recipients=(), email_template_fp=None, logger=None, logdir=None):
        self.email_recipients = email_recipients
        self.email_template_fp = email_template_fp if email_template_fp else self.email_template_file
        self.log = logger if logger else LOGGER
        self.logdir = logdir if logdir else tempfile.mkdtemp()

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
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
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

    def send_email(self, subject, content, html=True, files=()):
        if self.email_recipients:
            self.log.debug('Send email to {}'.format(self.email_recipients))
            email = Email()
            email.send(subject, content, html=html, recipients=self.email_recipients, files=files)
        else:
            self.log.warning("Won't send email (send_email: %s, recipients: %s)",
                             self.send_email, self.email_recipients)

    def send_report(self, results):
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
        try:
            smtp.send_email(recipients=self.email_recipients, email=email)
        except Exception as details:  # pylint: disable=broad-except
            self.log.error("Error during sending email: %s", details, exc_info=True)
        self.log.info(f'Send email with results to {self.email_recipients}')

    def _generate_report_attachments(self, attachments_data):
        if attachments_data is None:
            self.save_html_to_file(
                {},
                'attachment_excluded.html',
                template_str='<html><body>Attachment was excluded due to the size limitation</body></html>')
            return ('attachment_excluded.html',)
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

    def build_report(self, report_data, template_str=None):
        return self.render_to_html(report_data, template_str=None)

    def build_report_attachments(self, attachments_data, template_str=None):
        return ()

    def cut_report_data(self, report_data, attachments_data, reason):
        if attachments_data is not None:
            return report_data, None
        return None, None


class LongevityEmailReporter(BaseEmailReporter):

    email_template_file = "results_longevity.html"
    fields = ['subject', 'grafana_screenshots', 'grafana_snapshots',
              'test_status', 'test_name', 'start_time', 'end_time',
              'build_url', 'scylla_version', 'scylla_ami_id',
              'scylla_instance_type', 'number_of_db_nodes',
              'nemesis_name', 'nemesis_details', 'test_id',
              'username', 'nodes']

    def cut_report_data(self, report_data, attachments_data, reason):
        if ['test_status'] in attachments_data and len(attachments_data['test_status']) > 2 and len(
                attachments_data['test_status'][1]) > 101:
            #  Reduce number of records in test_status[1] to 100
            attachments_data['test_status'][1] = attachments_data['test_status'][1][:100]
            attachments_data['test_status'][1].append('List of events has been cut due to the email size limit')
            return report_data, attachments_data
        if attachments_data is not None:
            return report_data, None
        return None, None

    def build_report(self, report_data, template_str=None):
        report_data['short_report'] = True
        return self.render_to_html(report_data, template_str)

    def build_report_attachments(self, attachments_data, template_str=None):
        report_file = os.path.join(self.logdir, 'email_report.html')
        self.save_html_to_file(attachments_data, report_file, template_str=template_str)
        attachments = (report_file, )
        return attachments


class GeminiEmailReporter(BaseEmailReporter):

    email_template_file = "results_gemini.html"
    fields = ['subject', 'gemini_cmd', 'gemini_version',
              'scylla_version', 'scylla_ami_id', 'scylla_instance_type',
              'number_of_db_nodes', 'number_of_oracle_nodes',
              'oracle_db_version', 'oracle_ami_id', 'oracle_instance_type',
              "results", "status", 'test_name', 'test_id', 'test_status',
              'start_time', 'end_time', 'username',
              'build_url', 'nemesis_name', 'nemesis_details',
              'test_id', 'nodes']

    def build_report(self, report_data, template_str=None):
        self.log.info('Prepare result to send in email')
        html = self.render_to_html(report_data, template_str)
        return html


def build_reporter(tester):
    """Build reporter

    [description]

    Arguments:
        tester {ClusterTester} -- instance of ClusterTester for currrent test
    """
    email_recipients = tester.params.get('email_recipients', default=None)
    logdir = tester.logdir
    if "Gemini" in tester.__class__.__name__:
        return GeminiEmailReporter(email_recipients=email_recipients, logdir=logdir)
    elif "Longevity" in tester.__class__.__name__:
        return LongevityEmailReporter(email_recipients=email_recipients, logdir=logdir)
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

    instances = list_instances_aws(tags_dict={'TestId': test_id}, group_as_region=True, running=True)
    for region in instances:
        for instance in instances[region]:
            name = [tag['Value'] for tag in instance['Tags'] if tag['Key'] == 'Name']
            nodes.append([name[0],
                          instance.get('PublicIpAddress', 'N/A'),
                          instance['State']['Name'],
                          "aws",
                          region])
    instances = list_instances_gce(tags_dict={"TestId": test_id}, running=True)
    for instance in instances:
        nodes.append([instance.name,
                      ", ".join(instance.public_ips) if None not in instance.public_ips else "N/A",
                      instance.state,
                      "gce",
                      instance.extra["zone"].name])
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
