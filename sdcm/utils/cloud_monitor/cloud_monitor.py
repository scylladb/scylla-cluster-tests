from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
from sdcm.utils.common import list_instances_aws, list_instances_gce, aws_tags_to_dict, gce_meta_to_dict
from sdcm.utils.cloud_monitor.report import GeneralReport, DetailedReport


LOGGER = getLogger(__name__)
CLOUD_PROVIDERS = ("aws", "gce")


class CloudInstance(object):
    def __init__(self, cloud, name, region_az, state, lifecycle, instance_type, owner, create_time):
        self.cloud = cloud
        self.name = name
        self.region_az = region_az
        self.state = state
        self.lifecycle = lifecycle
        self.instance_type = instance_type
        self.owner = owner.lower()
        self.create_time = create_time


class CloudInstances(object):

    def __init__(self):
        self.instances = {prov: [] for prov in CLOUD_PROVIDERS}
        self.all_instances = []
        self.get_all()

    def __getitem__(self, item):
        return self.instances[item]

    def get_aws_instances(self):
        aws_instances = list_instances_aws()
        for instance in aws_instances:
            tags = aws_tags_to_dict(instance.get('Tags'))
            cloud_instance = CloudInstance(
                cloud="aws",
                name=tags.get("Name", "N/A"),
                region_az=instance["Placement"]["AvailabilityZone"],
                state=instance["State"]["Name"],
                lifecycle="spot" if instance.get("SpotInstanceRequestId") else "on-demand",
                instance_type=instance["InstanceType"],
                owner=tags.get("RunByUser", tags.get("Owner", "N/A")),
                create_time=instance['LaunchTime'].ctime(),
            )
            self.instances["aws"].append(cloud_instance)
        self.all_instances += self.instances["aws"]

    def get_gce_instances(self):
        gce_instances = list_instances_gce()
        for instance in gce_instances:
            tags = gce_meta_to_dict(instance.extra['metadata'])
            cloud_instance = CloudInstance(
                cloud="gce",
                name=instance.name,
                region_az=instance.extra["zone"].name,
                state=instance.state,
                lifecycle="spot" if instance.extra["scheduling"]["preemptible"] else "on-demand",
                instance_type=instance.size,
                owner=tags.get('RunByUser', 'N/A') if tags else "N/A",
                create_time=instance.extra['creationTimestamp'],
            )
            self.instances["gce"].append(cloud_instance)
        self.all_instances += self.instances["gce"]

    def get_all(self):
        LOGGER.info("Getting all cloud instances...")
        self.get_aws_instances()
        self.get_gce_instances()


def notify_by_email(general_report, detailed_report, recipients):
    email_client = Email()
    LOGGER.info("Sending email to '%s'", recipients)
    email_client.send(subject="Cloud resources report - {}".format(datetime.now()),
                      content=general_report.to_html(),
                      recipients=recipients,
                      html=True,
                      files=[detailed_report.to_file()]
                      )


def cloud_report(mail_to):
    cloud_instances = CloudInstances()
    notify_by_email(general_report=GeneralReport(cloud_instances),
                    detailed_report=DetailedReport(cloud_instances),
                    recipients=mail_to)


if __name__ == "__main__":
    cloud_report()
