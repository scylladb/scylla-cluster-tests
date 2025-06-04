# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2022 ScyllaDB

import logging
from typing import List, Any


from sdcm.provision import provisioner_factory
from sdcm.provision.helpers.cloud_init import wait_cloud_init_completes
from sdcm.provision.provisioner import PricingModel, VmInstance, ProvisionError, Provisioner, InstanceDefinition, OperationPreemptedError
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


@retrying(n=3, sleep_time=5, allowed_exceptions=(ProvisionError,))
def provision_with_retry(provisioner: Provisioner, definitions: List[InstanceDefinition], pricing_model: PricingModel
                         ) -> List[VmInstance]:
    return provisioner.get_or_create_instances(definitions=definitions, pricing_model=pricing_model)


def provision_instances_with_fallback(provisioner: Provisioner, definitions: List[InstanceDefinition], pricing_model: PricingModel,
                                      fallback_on_demand: bool
                                      ) -> List[VmInstance]:
    try:
        provision_with_retry(provisioner, definitions=definitions, pricing_model=pricing_model)
    except OperationPreemptedError:
        if pricing_model.is_spot() and fallback_on_demand:
            provision_with_retry(provisioner, definitions=definitions, pricing_model=PricingModel.ON_DEMAND)
        else:
            raise

    provisioned_instances = provisioner.get_or_create_instances(definitions=definitions)
    for v_m in provisioned_instances:
        ssh_login_info = {'hostname': v_m.public_ip_address,
                          'user': v_m.user_name,
                          'key_file': f"~/.ssh/{v_m.ssh_key_name}"}
        remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)
        wait_cloud_init_completes(remoter=remoter, instance=v_m)
        # todo: wait for scylla-machine-image service to complete if instance is scylla-db?
        # todo: download cloud-init logs
    return provisioned_instances


def provision_sct_resources(params: SCTConfiguration, test_config: TestConfig, **provisioner_config: Any):
    """Provisions instances according to SCT Configuration."""
    builder = region_definition_builder.get_builder(params=params, test_config=test_config)
    definitions_per_region = builder.build_all_region_definitions()
    pricing_model = PricingModel(params.get("instance_provision"))
    provision_fallback_on_demand = params.get("instance_provision_fallback_on_demand")
    for request in definitions_per_region:
        provisioner = provisioner_factory.create_provisioner(backend=request.backend,
                                                             test_id=request.test_id,
                                                             region=request.region,
                                                             availability_zone=request.availability_zone,
                                                             **provisioner_config)
        provision_instances_with_fallback(provisioner=provisioner,
                                          definitions=request.definitions,
                                          pricing_model=pricing_model,
                                          fallback_on_demand=provision_fallback_on_demand)
