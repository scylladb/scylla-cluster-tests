import os.path
import smtplib
import sys
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging import getLogger

from sdcm.keystore import KeyStore
from sdcm.utils.cloud_monitor.resources.capacity_reservations import get_active_capacity_reservations
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances
from sdcm.utils.cloud_monitor.report import (
    BaseReport,
    GeneralReport,
    DetailedReport,
    QAonlyInstancesTimeDistributionReport,
    NonQaInstancesTimeDistributionReport,
)
from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs
from sdcm.utils.cloud_monitor.resources.xcloud import XCloudResources

LOGGER = getLogger(__name__)


class Email:
    """Responsible for sending emails via SMTP."""

    _attachments_size_limit = 10485760  # 10Mb
    _body_size_limit = 26214400  # 25Mb

    def __init__(self):
        self.sender = "qa@scylladb.com"
        self._password = ""
        self._user = ""
        self._server_host = "smtp.gmail.com"
        self._server_port = "587"
        self.conn = None
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

    def send(self, subject, content, recipients, html=True, files=()):
        msg = MIMEMultipart()
        msg["subject"] = subject
        msg["from"] = self.sender
        assert recipients, "No recipients provided"
        msg["to"] = ",".join(recipients)
        if html:
            text_part = MIMEText(content, "html")
        else:
            text_part = MIMEText(content, "plain")
        msg.attach(text_part)
        attachment_size = 0
        for path in files:
            attachment_size += os.path.getsize(path)
            if attachment_size >= self._attachments_size_limit:
                raise RuntimeError(f"Attachment size {attachment_size} exceeds limit {self._attachments_size_limit}")
            with open(path, "rb") as fil:
                part = MIMEApplication(fil.read(), Name=os.path.basename(path))
            part["Content-Disposition"] = 'attachment; filename="%s"' % os.path.basename(path)
            msg.attach(part)
        email = msg.as_string()
        if len(email) >= self._body_size_limit:
            raise RuntimeError(f"Email body size {len(email)} exceeds limit {self._body_size_limit}")
        self.conn.sendmail(self.sender, recipients, email)

    def __del__(self):
        if self.conn:
            self.conn.quit()


def notify_by_email(
    general_report: BaseReport, detailed_report: DetailedReport = None, recipients: list = None, group_str=""
):
    email_client = Email()
    LOGGER.info("Sending email to '%s'", recipients)
    email_client.send(
        subject=f"Cloud resources: {group_str} usage report - {datetime.now()}",
        content=general_report.to_html(),
        recipients=recipients,
        html=True,
        files=[detailed_report.to_file()] if detailed_report else [],
    )


def cloud_report(mail_to):
    cloud_instances = CloudInstances()
    static_ips = StaticIPs(cloud_instances)
    crs = get_active_capacity_reservations()

    LOGGER.info("Starting xcloud resources collection...")
    try:
        xcloud_resources = XCloudResources()
        cluster_count = len(xcloud_resources.clusters)
        LOGGER.info(
            f"Collected {cluster_count} xcloud cluster(s) for reporting"
            if cluster_count
            else "No xcloud clusters found"
        )
    except Exception:  # noqa: BLE001
        LOGGER.error("Failed to collect xcloud resources", exc_info=True)
        xcloud_resources = None

    notify_by_email(
        general_report=GeneralReport(
            cloud_instances=cloud_instances, static_ips=static_ips, crs=crs, xcloud_resources=xcloud_resources
        ),
        detailed_report=DetailedReport(
            cloud_instances=cloud_instances, static_ips=static_ips, xcloud_resources=xcloud_resources
        ),
        recipients=mail_to,
    )


def cloud_qa_report(mail_to, user=None):
    cloud_instances = CloudInstances()
    notify_by_email(
        general_report=QAonlyInstancesTimeDistributionReport(cloud_instances=cloud_instances, user=user),
        recipients=mail_to,
        group_str="QA only",
    )


def cloud_non_qa_report(mail_to, user=None):
    cloud_instances = CloudInstances()
    notify_by_email(
        general_report=NonQaInstancesTimeDistributionReport(cloud_instances=cloud_instances, user=user),
        recipients=mail_to,
        group_str="NON QA",
    )


if __name__ == "__main__":
    cloud_report(mail_to=sys.argv[1].split(","))
