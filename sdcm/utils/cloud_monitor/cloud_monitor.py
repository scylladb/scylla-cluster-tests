from datetime import datetime
from logging import getLogger
from sdcm.send_email import Email
from sdcm.utils.cloud_monitor.common import InstanceLifecycle
from sdcm.utils.common import list_instances_aws, list_instances_gce, aws_tags_to_dict, gce_meta_to_dict
from sdcm.utils.cloud_monitor.report import GeneralReport, DetailedReport, NA
from sdcm.utils.fromisoformat import datetime_from_isoformat
from sdcm.utils.pricing import AWSPricing, GCEPricing

LOGGER = getLogger(__name__)


class CloudInstance:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    pricing = None  # need to be set in the child class

    def __init__(self, cloud, name, instance_id, region_az, state, lifecycle, instance_type, owner, create_time, keep):  # pylint: disable=too-many-arguments
        self.cloud = cloud
        self.name = name
        self.instance_id = instance_id
        self.region_az = region_az
        self.state = state
        self.lifecycle = lifecycle
        self.instance_type = instance_type
        self.owner = owner.lower()
        self.create_time = create_time
        self.keep = keep  # keep alive
        self.price = self.pricing.get_instance_price(region=self.region, instance_type=self.instance_type,
                                                     state=self.state, lifecycle=self.lifecycle)

    @property
    def region(self):
        raise NotImplementedError

    def hours_running(self):
        if self.state == "running":
            dt_since_created = datetime.now(self.create_time.tzinfo) - self.create_time
            return int(dt_since_created.total_seconds()/3600)
        return 0

    @property
    def total_cost(self):
        return round(self.hours_running() * self.price, 1)

    @property
    def projected_daily_cost(self):
        return round(24 * self.price, 1)


class AWSInstance(CloudInstance):  # pylint: disable=too-few-public-methods,
    pricing = AWSPricing()

    def __init__(self, instance):
        tags = aws_tags_to_dict(instance.get('Tags'))
        super(AWSInstance, self).__init__(
            cloud="aws",
            name=tags.get("Name", NA),
            instance_id=instance['InstanceId'],
            region_az=instance["Placement"]["AvailabilityZone"],
            state=instance["State"]["Name"],
            lifecycle=InstanceLifecycle.SPOT if instance.get("SpotInstanceRequestId") else InstanceLifecycle.ON_DEMAND,
            instance_type=instance["InstanceType"],
            owner=tags.get("RunByUser", tags.get("Owner", NA)),
            create_time=instance['LaunchTime'],
            keep=tags.get("keep", ""),
        )

    @property
    def region(self):
        return self.region_az[:-1]


class GCEInstance(CloudInstance):
    pricing = GCEPricing()

    def __init__(self, instance):
        tags = gce_meta_to_dict(instance.extra['metadata'])
        is_preemptible = instance.extra["scheduling"]["preemptible"]
        super(GCEInstance, self).__init__(
            cloud="gce",
            name=instance.name,
            instance_id=instance.id,
            region_az=instance.extra["zone"].name,
            state=instance.state,
            lifecycle=InstanceLifecycle.SPOT if is_preemptible else InstanceLifecycle.ON_DEMAND,
            instance_type=instance.size,
            owner=tags.get("RunByUser", NA) if tags else NA,
            create_time=datetime_from_isoformat(instance.extra['creationTimestamp']),
            keep=self.get_keep_alive_gce_instance(instance),
        )

    @property
    def region(self):
        return self.region_az[:-2]

    @staticmethod
    def get_keep_alive_gce_instance(instance):
        # same logic as in cloud instance stopper
        # checking labels
        labels = instance.extra["labels"]
        if labels:
            return labels.get("keep", labels.get("keep-alive", ""))
        # checking tags
        tags = instance.extra["tags"]
        if tags:
            return "alive" if 'alive' in tags or 'keep-alive' in tags or 'keep' in tags else ""
        return ""


class CloudInstances:
    CLOUD_PROVIDERS = ("aws", "gce")

    def __init__(self):
        self.instances = {prov: [] for prov in self.CLOUD_PROVIDERS}
        self.all_instances = []
        self.get_all()

    def __getitem__(self, item):
        return self.instances[item]

    def get_aws_instances(self):
        aws_instances = list_instances_aws(verbose=True)
        self.instances["aws"] = [AWSInstance(instance) for instance in aws_instances]
        self.all_instances.extend(self.instances["aws"])

    def get_gce_instances(self):
        gce_instances = list_instances_gce(verbose=True)
        self.instances["gce"] = [GCEInstance(instance) for instance in gce_instances]
        self.all_instances.extend(self.instances["gce"])

    def get_all(self):
        LOGGER.info("Getting all cloud instances...")
        self.get_aws_instances()
        self.get_gce_instances()


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
    notify_by_email(general_report=GeneralReport(cloud_instances),
                    detailed_report=DetailedReport(cloud_instances),
                    recipients=mail_to)
