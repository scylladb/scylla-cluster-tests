import sys
from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances
from sdcm.utils.cloud_monitor.report import GeneralReport, DetailedReport, QAonlyTimeDistributionReport
from sdcm.utils.cloud_monitor.resources.static_ips import StaticIPs

LOGGER = getLogger(__name__)


def notify_by_email(general_report: GeneralReport, detailed_report: DetailedReport, recipients: list):
    email_client = Email()
    LOGGER.info("Sending email to '%s'", recipients)
    email_client.send(subject="Cloud resources: usage report - {}".format(datetime.now()),
                      content=general_report.to_html(),
                      recipients=recipients,
                      html=True,
                      files=[detailed_report.to_file()]
                      )


def notify_qa_by_email(general_report: GeneralReport, detailed_report: DetailedReport = None, recipients: list = None):
    email_client = Email()
    if not recipients:
        recipients = ["qa@scylladb.com"]
    attaching_files = [detailed_report.to_file()] if detailed_report else []
    LOGGER.info("Sending email to '%s'", recipients)
    email_client.send(subject="Cloud resources: QA only usage report - {}".format(datetime.now()),
                      content=general_report.to_html(),
                      recipients=recipients,
                      html=True,
                      files=attaching_files
                      )


def cloud_report(mail_to):
    cloud_instances = CloudInstances()
    static_ips = StaticIPs(cloud_instances)
    notify_by_email(general_report=GeneralReport(cloud_instances=cloud_instances, static_ips=static_ips),
                    detailed_report=DetailedReport(cloud_instances=cloud_instances, static_ips=static_ips),
                    recipients=mail_to)


def cloud_qa_report(mail_to, user=None):
    cloud_instances = CloudInstances()
    notify_qa_by_email(general_report=QAonlyTimeDistributionReport(cloud_instances=cloud_instances, static_ips=None, user=user),
                       recipients=mail_to)


if __name__ == "__main__":
    cloud_report(mail_to=sys.argv[1].split(","))
