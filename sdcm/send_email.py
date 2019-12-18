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


class Email():
    """
    Responsible for sending emails
    """

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

    def send(self, subject, content, recipients, html=True, files=()):  # pylint: disable=too-many-arguments
        """
        :param subject: text
        :param content: text/html
        :param recipients: iterable, list of recipients
        :param html: True/False
        :param files: paths of the files that will be attached to the email
        :return:
        """
        msg = MIMEMultipart()
        msg['subject'] = subject
        msg['from'] = self.sender
        msg['to'] = ','.join(recipients)
        if html:
            text_part = MIMEText(content, "html")
        else:
            text_part = MIMEText(content, "plain")
        msg.attach(text_part)
        for path in files:
            with open(path, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=os.path.basename(path)
                )
            part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(path)
            msg.attach(part)

        self.conn.sendmail(self.sender, recipients, msg.as_string())

    def __del__(self):
        self.conn.quit()


class BaseEmailReporter():

    fields = []
    email_template_file = 'results_base.html'

    def __init__(self, email_recipients=(), email_template_fp=None, logger=None, logdir=None):
        self.email_recipients = email_recipients
        self.email_template_fp = email_template_fp if email_template_fp else self.email_template_file
        self.log = logger if logger else LOGGER
        self.logdir = logdir if logdir else tempfile.mkdtemp()

    def build_data_for_render(self, results):
        return {key: results.get(key, "N/A") for key in self.fields}

    def render_to_html(self, results):
        """
        Render analysis results to html template
        :param results: results dictionary
        :return: html string
        """
        self.log.info("Rendering results to html using '%s' template...", self.email_template_fp)
        loader = jinja2.FileSystemLoader(os.path.dirname(os.path.abspath(__file__)))
        env = jinja2.Environment(loader=loader, autoescape=True, extensions=['jinja2.ext.loopcontrols'])
        template = env.get_template(self.email_template_fp)
        html = template.render(results)
        self.log.info("Results has been rendered to html")
        return html

    def save_html_to_file(self, results, html_file_path=""):
        if html_file_path:
            html = self.render_to_html(results)
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
        try:
            email_data = self.build_data_for_render(results)
            self.log.info('Send email with results to {}'.format(self.email_recipients))
            html, attached_files = self.build_report(email_data)
            self.send_email(subject=email_data['subject'], content=html, files=attached_files)
        except Exception as details:  # pylint: disable=broad-except
            self.log.error("Error during sending email: %s", details, exc_info=True)

    def build_report(self, email_data):
        return self.render_to_html(email_data), ()


class LongevityEmailReporter(BaseEmailReporter):

    email_template_file = "results_longevity.html"
    fields = ['subject', 'grafana_screenshots', 'grafana_snapshots',
              'test_status', 'test_name', 'start_time', 'end_time',
              'build_url', 'scylla_version', 'scylla_ami_id',
              'scylla_instance_type', 'number_of_db_nodes',
              'nemesis_name', 'nemesis_details', 'test_id',
              'username', 'nodes']

    def build_report(self, email_data):
        report_file = os.path.join(self.logdir, 'email_report.html')
        self.save_html_to_file(email_data, report_file)
        email_data['short_report'] = True
        html = self.render_to_html(email_data)
        return html, (report_file, )


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

    def build_report(self, email_data):
        self.log.info('Prepare result to send in email')
        html = self.render_to_html(email_data)
        return html, ()


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
