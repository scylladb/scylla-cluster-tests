from logging import getLogger
from datetime import datetime, timezone

from boto3 import client as boto3_client
from azure.mgmt.compute.models import VirtualMachine
from google.cloud.compute_v1.types import Instance as GceInstance

from sdcm.utils.azure_utils import AzureService
from sdcm.utils.cloud_monitor.common import InstanceLifecycle, NA
from sdcm.utils.cloud_monitor.resources import CloudInstance, CloudResources
from sdcm.utils.common import aws_tags_to_dict, gce_meta_to_dict, list_instances_aws, list_instances_gce
from sdcm.utils.pricing import AWSPricing, GCEPricing, AzurePricing
from sdcm.utils.gce_utils import SUPPORTED_PROJECTS
from sdcm.utils.context_managers import environment

LOGGER = getLogger(__name__)


class AWSInstance(CloudInstance):
    pricing = AWSPricing()

    def __init__(self, instance):
        self._instance = instance
        self._tags = aws_tags_to_dict(instance.get('Tags'))
        super().__init__(
            cloud="aws",
            name=self._tags.get("Name", NA),
            instance_id=instance['InstanceId'],
            region_az=instance["Placement"]["AvailabilityZone"],
            state=instance["State"]["Name"],
            lifecycle=InstanceLifecycle.SPOT if instance.get("SpotInstanceRequestId") else InstanceLifecycle.ON_DEMAND,
            instance_type=instance["InstanceType"],
            owner=self.get_owner(),
            create_time=instance['LaunchTime'],
            keep=self._tags.get("keep", ""),
        )

    @property
    def region(self):
        return self.region_az[:-1]

    def get_owner_from_cloud_trail(self):
        try:
            client = boto3_client('cloudtrail', region_name=self._instance["Placement"]["AvailabilityZone"][:-1])
            result = client.lookup_events(LookupAttributes=[{'AttributeKey': 'ResourceName',
                                                             'AttributeValue': self._instance['InstanceId']}])
            for event in result["Events"]:
                if event['EventName'] == 'RunInstances':
                    return event["Username"]
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Error occurred when trying to find an owner for '%s' in CloudTrail: %s",
                           self._instance['InstanceId'], exc)
        return None

    def get_owner(self):
        # try to get the owner using tags
        if owner := self._tags.get("RunByUser", self._tags.get("Owner")):
            return owner
        # get the owner from the Cloud Trail
        if owner := self.get_owner_from_cloud_trail():
            return owner
        return NA


class GCEInstance(CloudInstance):
    pricing = GCEPricing()

    def __init__(self, instance: GceInstance):
        tags = gce_meta_to_dict(instance.metadata)
        is_preemptible = instance.scheduling.preemptible
        super().__init__(
            cloud="gce",
            name=instance.name,
            instance_id=instance.id,
            region_az=instance.zone.split('/')[-1],
            state=str(instance.status.lower()),
            lifecycle=InstanceLifecycle.SPOT if is_preemptible else InstanceLifecycle.ON_DEMAND,
            instance_type=instance.machine_type.split('/')[-1],
            owner=tags.get("RunByUser", NA) if tags else NA,
            create_time=datetime.fromisoformat(instance.creation_timestamp),
            keep=self.get_keep_alive_gce_instance(instance),
            project=instance.self_link.split('/')[6]
        )

    @property
    def region(self):
        return self.region_az[:-2]

    @staticmethod
    def get_keep_alive_gce_instance(instance: GceInstance):
        # same logic as in cloud instance stopper
        # checking labels
        labels = instance.labels
        if labels:
            return labels.get("keep", labels.get("keep-alive", ""))
        # checking tags
        tags = instance.tags.items
        if tags:
            return "alive" if 'alive' in tags or 'keep-alive' in tags or 'keep' in tags else ""
        return ""


class AzureInstance(CloudInstance):
    pricing = AzurePricing()

    def __init__(self, instance: VirtualMachine, resource_group: str):
        tags = instance.tags or {}
        creation_time = tags.get("creation_time", None)
        if creation_time:
            creation_time = datetime.fromisoformat(creation_time).replace(tzinfo=timezone.utc)
        super().__init__(
            cloud="azure",
            name=instance.name,
            instance_id=resource_group,
            region_az=instance.location,
            state=self._get_vm_status(instance),
            lifecycle=InstanceLifecycle.SPOT if instance.priority == "Spot" else InstanceLifecycle.ON_DEMAND,
            instance_type=instance.hardware_profile.vm_size,
            owner=tags.get("RunByUser", NA),
            create_time=creation_time,
            keep=tags.get("keep", ""),
            project=resource_group
        )

    @staticmethod
    def _get_vm_status(instance) -> str:
        statuses = {s.code: s.display_status for s in instance.instance_view.statuses}
        return 'running' if statuses.get("PowerState/running") else 'stopped'

    @property
    def region(self):
        return self.region_az


class CloudInstances(CloudResources):

    def get_aws_instances(self):
        aws_instances = list_instances_aws(verbose=True)
        self["aws"] = [AWSInstance(instance) for instance in aws_instances]
        self.all.extend(self["aws"])

    def get_gce_instances(self):
        self["gce"] = []
        for project in SUPPORTED_PROJECTS:
            with environment(SCT_GCE_PROJECT=project):
                gce_instances = list_instances_gce(verbose=True)
                self["gce"] += [GCEInstance(instance) for instance in gce_instances]
        self.all.extend(self["gce"])

    def get_azure_instances(self):
        query_bits = ["Resources", "where type =~ 'Microsoft.Compute/virtualMachines'",
                      "project id, resourceGroup, name"]
        res = AzureService().resource_graph_query(query=' | '.join(query_bits))
        get_virtual_machine = AzureService().compute.virtual_machines.get
        instances = [(get_virtual_machine(resource_group_name=vm["resourceGroup"],
                      vm_name=vm["name"], expand='instanceView'), vm["resourceGroup"]) for vm in res]
        self["azure"] = [AzureInstance(instance, resource_group) for instance, resource_group in instances]
        self.all.extend(self["azure"])

    def get_all(self):
        LOGGER.info("Getting all cloud instances...")
        self.get_aws_instances()
        self.get_gce_instances()
        self.get_azure_instances()
