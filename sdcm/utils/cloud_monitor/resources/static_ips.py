from logging import getLogger
from google.cloud import compute_v1

from sdcm.utils.azure_utils import AzureService
from sdcm.utils.cloud_monitor.common import NA
from sdcm.utils.cloud_monitor.resources import CloudResources
from sdcm.utils.common import list_elastic_ips_aws, aws_tags_to_dict, list_static_ips_gce


LOGGER = getLogger(__name__)


class StaticIP:
    def __init__(self, cloud, name, address, region, used_by, owner):
        self.cloud = cloud
        self.name = name
        self.address = address
        self.region = region
        self.used_by = used_by  # instance to which the static IP is associated
        self.owner = owner

    @property
    def is_used(self):
        if self.used_by != NA:
            return True
        return False


class AwsElasticIP(StaticIP):
    def __init__(self, eip, region):
        tags = eip.get('Tags')
        tags_dict = {}
        if tags:
            tags_dict = aws_tags_to_dict(tags)
        super().__init__(
            cloud="aws",
            name=tags_dict.get("Name", NA) if tags else NA,
            address=eip['PublicIp'],
            region=region,
            used_by=eip.get('InstanceId', NA),
            owner=tags_dict.get("RunByUser", NA) if tags else NA,
        )


class GceStaticIP(StaticIP):
    def __init__(self, static_ip: compute_v1.Address):
        used_by = static_ip.users
        super().__init__(
            cloud="gce",
            name=static_ip.name,
            address=static_ip.address,
            region=static_ip.region,
            used_by=used_by[0].rsplit("/", maxsplit=1)[-1] if used_by else NA,
            owner=NA  # currently unsupported, maybe we can store it in description in future
        )


class AzureStaticIP(StaticIP):
    def __init__(self, static_ip, resource_group):
        super().__init__(
            cloud="azure",
            name=static_ip["name"],
            address=static_ip["properties"]["ipAddress"],
            region=static_ip["location"],
            used_by=resource_group,
            owner=NA  # currently unsupported
        )


class StaticIPs(CloudResources):
    """Allocated static IPs"""

    def __init__(self, cloud_instances):
        self.cloud_instances = cloud_instances  # needed to identify use when attached to an instance
        super().__init__()

    def get_aws_elastic_ips(self):
        LOGGER.info("Getting AWS Elastic IPs...")
        eips_grouped_by_region = list_elastic_ips_aws(group_as_region=True, verbose=True)
        self["aws"] = [AwsElasticIP(eip, region) for region, eips in eips_grouped_by_region.items() for eip in eips]
        # identify user by the owner of the resource
        cloud_instances_by_id = {instance.instance_id: instance for instance in self.cloud_instances["aws"]}
        for eip in self["aws"]:
            if eip.used_by != NA and cloud_instances_by_id.get(eip.used_by):
                if eip.owner == NA:
                    eip.owner = cloud_instances_by_id[eip.used_by].owner
                if eip.name == NA:
                    # display the used instance name if ip's name is empty
                    eip.name = f"{NA} ({cloud_instances_by_id[eip.used_by].name})"
        self.all.extend(self["aws"])

    def get_gce_static_ips(self):
        static_ips = list_static_ips_gce(verbose=True)
        self["gce"] = [GceStaticIP(ip) for ip in static_ips]
        cloud_instances_by_name = {instance.name: instance for instance in self.cloud_instances["gce"]}
        for eip in self["gce"]:
            if eip.owner == NA and eip.used_by != NA and cloud_instances_by_name.get(eip.used_by):
                eip.owner = cloud_instances_by_name[eip.used_by].owner
        self.all.extend(self["gce"])

    def get_azure_static_ips(self):
        try:
            # all azure public ip's are external resources and we pay for it
            query_bits = ["Resources", "where type =~ 'Microsoft.Network/publicIPAddresses'",
                          "project id, resourceGroup, name, location, properties"]
            res = AzureService().resource_graph_query(query=' | '.join(query_bits))
            self["azure"] = [AzureStaticIP(ip, ip["resourceGroup"]) for ip in res if "ipAddress" in ip["properties"]]
            self.all.extend(self["azure"])
            LOGGER.info("Successfully retrieved Azure static IPs")
        except Exception as e:  # noqa: BLE001
            LOGGER.error("Failed to retrieve Azure static IPs: %s. Continuing with other cloud providers.", e)
            self["azure"] = []  # Set empty list to indicate partial failure

    def get_all(self):
        self.get_aws_elastic_ips()
        self.get_gce_static_ips()
        self.get_azure_static_ips()
