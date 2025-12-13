import sys
from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
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
