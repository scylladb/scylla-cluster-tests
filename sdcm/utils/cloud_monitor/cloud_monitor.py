import sys
import random
from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances
from sdcm.utils.cloud_monitor.report import (
    BaseReport,
    GeneralReport,
    DetailedReport,
    QAonlyInstancesTimeDistributionReport,
    NonQaInstancesTimeDistributionReport,
)
from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs

LOGGER = getLogger(__name__)


def notify_by_email(
    general_report: BaseReport, detailed_report: DetailedReport = None, recipients: list = None, group_str=""
):
    def rand(string_to_manipulate):
        letters = [
            ["o", "0", "\u022f", "\u1ecd", "\u1ecf", "\u01a1", "\u00f6", "\u00f3", "\u00f2"],
            ["s", "\u0282"],
            ["r", "\u0393"],
            ["e", "\u1eb9", "\u0117", "\u0117", "\u00e9", "\u00e8"],
            ["g", "\u0121"],
            ["u", "\u057d", "\u00fc", "\u00fa", "\u00f9"],
        ]

        manipulated_string = string_to_manipulate
        for charlist in letters:
            manipulated_string = manipulated_string.replace(charlist[0], random.choice(charlist))
        return manipulated_string

    email_client = Email()
    LOGGER.info("Sending email to '%s'", recipients)
    subject = f"Cloud resources: {group_str} usage report - {datetime.now()}"
    random_subject = rand(string_to_manipulate=subject)
    email_client.send(
        subject=random_subject,
        content=general_report.to_html(),
        recipients=recipients,
        html=True,
        files=[detailed_report.to_file()] if detailed_report else [],
    )


def cloud_report(mail_to):
    cloud_instances = CloudInstances()
    static_ips = StaticIPs(cloud_instances)
    notify_by_email(
        general_report=GeneralReport(cloud_instances=cloud_instances, static_ips=static_ips),
        detailed_report=DetailedReport(cloud_instances=cloud_instances, static_ips=static_ips),
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
