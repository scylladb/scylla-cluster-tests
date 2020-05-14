import sys
from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
from sdcm.utils.cloud_monitor.resources.instances import CloudInstances
from sdcm.utils.cloud_monitor.report import GeneralReport, DetailedReport
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


def cloud_report(mail_to):
    cloud_instances = CloudInstances()
    static_ips = StaticIPs(cloud_instances)
    notify_by_email(general_report=GeneralReport(cloud_instances=cloud_instances, static_ips=static_ips),
                    detailed_report=DetailedReport(cloud_instances=cloud_instances, static_ips=static_ips),
                    recipients=mail_to)


if __name__ == "__main__":
    cloud_report(mail_to=sys.argv[1].split(","))
