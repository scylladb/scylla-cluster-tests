import logging
from typing import List, Union

from azure.mgmt.compute.v2021_07_01.models import VirtualMachine

from sdcm.provision.azure.utils import AzureMixing
from sdcm.provision.common.provisioner import InstanceProvisionerBase, ProvisionParameters, TagsType


LOGGER = logging.getLogger(__name__)


class AzureInstanceProvisioner(InstanceProvisionerBase, AzureMixing):  # pylint: disable=too-few-public-methods
    # TODO: Make them configurable
    _wait_interval = 5
    _iam_fleet_role = 'arn:aws:iam::797456418907:role/aws-ec2-spot-fleet-role'

    def provision(  # pylint: disable=too-many-arguments
            self,
            provision_parameters: ProvisionParameters,
            instance_parameters: VirtualMachine,
            count: int,
            tags: Union[List[TagsType], TagsType] = None,
            names: List[str] = None,
    ) -> List[VirtualMachine]:

        if tags is None:
            tags = {}
        if isinstance(tags, dict):
            tags = [tags] * count
        elif isinstance(tags, list):
            tags = tags.copy()

        if names:
            assert len(names) == count, "Names length should be equal to count"
        assert len(tags) == count, "Tags length should be equal to count"

        for node_id, name in enumerate(names):
            tag = tags[node_id]
            tag['Name'] = name

        pricing_params = {
            'priority': "Regular",  # possible values are "Regular", "Low", or "Spot"
        }

        if provision_parameters.spot:
            pricing_params.update({
                'priority': 'Spot',
                'eviction_policy': 'Deallocate',  # can be "Deallocate" or "Delete"
                'billing_profile': {
                    "max_price": -1.0,  # -1 indicates the VM shouldn't be evicted for price reasons
                },
            })

        if provision_parameters.duration:
            LOGGER.info("Azure does not support duration on instance provision. Let's change priority to Low")
            pricing_params['priority'] = "Low"
        if provision_parameters.price:
            pricing_params['billing_profile']['max_price'] = provision_parameters.price
        output = []
        for idx in range(count):
            output.append(self._compute.virtual_machines.begin_create_or_update(
                parameters={
                    **self._extract_parameters_from_instance_parameters(instance_parameters),
                    **pricing_params,
                    'location': provision_parameters.region_name,
                    'tags': tags[idx],
                },
                vm_name=names[idx],
                resource_group_name='SCT-' + provision_parameters.region_name,
            ))
        return output

    def _extract_parameters_from_instance_parameters(self, instance_parameters: VirtualMachine) -> dict:
        return {name: value for name, value in instance_parameters.__dict__.items() if
                name[0] != '_' and value is not None}
