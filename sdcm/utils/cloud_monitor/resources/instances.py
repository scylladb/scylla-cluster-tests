from logging import getLogger
from datetime import datetime

from sdcm.utils.cloud_monitor.common import InstanceLifecycle, NA
from sdcm.utils.cloud_monitor.resources import CloudInstance, CloudResources
from sdcm.utils.common import aws_tags_to_dict, gce_meta_to_dict, list_instances_aws, list_instances_gce
from sdcm.utils.pricing import AWSPricing, GCEPricing


LOGGER = getLogger(__name__)


class AWSInstance(CloudInstance):  # pylint: disable=too-few-public-methods,
    pricing = AWSPricing()

    def __init__(self, instance):
        tags = aws_tags_to_dict(instance.get('Tags'))
        super().__init__(
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
        super().__init__(
            cloud="gce",
            name=instance.name,
            instance_id=instance.id,
            region_az=instance.extra["zone"].name,
            state=instance.state,
            lifecycle=InstanceLifecycle.SPOT if is_preemptible else InstanceLifecycle.ON_DEMAND,
            instance_type=instance.size,
            owner=tags.get("RunByUser", NA) if tags else NA,
            create_time=datetime.fromisoformat(instance.extra['creationTimestamp']),
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


class CloudInstances(CloudResources):

    def get_aws_instances(self):
        aws_instances = list_instances_aws(verbose=True)
        self["aws"] = [AWSInstance(instance) for instance in aws_instances]
        self.all.extend(self["aws"])

    def get_gce_instances(self):
        gce_instances = list_instances_gce(verbose=True)
        self["gce"] = [GCEInstance(instance) for instance in gce_instances]
        self.all.extend(self["gce"])

    def get_all(self):
        LOGGER.info("Getting all cloud instances...")
        self.get_aws_instances()
        self.get_gce_instances()
