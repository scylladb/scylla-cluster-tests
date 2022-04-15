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

from tenacity import retry, stop_after_attempt

from sdcm.provision import provisioner_factory
from sdcm.provision.provisioner import PricingModel, VmInstance, ProvisionError, Provisioner, InstanceDefinition
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import instances_request_builder

LOGGER = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), reraise=True)
def provision_with_retry(provisioner: Provisioner, definitions: List[InstanceDefinition], pricing_model: PricingModel
                         ) -> List[VmInstance]:
    return provisioner.get_or_create_instances(definitions=definitions, pricing_model=pricing_model)


def provision_sct_resources(sct_config: SCTConfiguration, **provisioner_config: Any):
    """Provisions instances according to SCT Configuration."""
    definitions_per_region = instances_request_builder.build(sct_config=sct_config)
    pricing_model = PricingModel(sct_config.get("instance_provision"))
    provision_fallback_on_demand = sct_config.get("instance_provision_fallback_on_demand")
    for request in definitions_per_region:
        provisioner = provisioner_factory.create_provisioner(backend=request.backend,
                                                             test_id=request.test_id,
                                                             region=request.region,
                                                             **provisioner_config)
        try:
            provision_with_retry(provisioner, definitions=request.definitions, pricing_model=pricing_model)
        except ProvisionError:
            if pricing_model.is_spot() and provision_fallback_on_demand:
                provision_with_retry(provisioner, definitions=request.definitions, pricing_model=PricingModel.ON_DEMAND)
            else:
                raise
