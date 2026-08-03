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
from sdcm.provision.common.fallback import is_region_fallback_enabled
from sdcm.provision.gce import region_fallback as gce_region_fallback
from sdcm.provision.gce.zone_resolver import GceAZResolver
from sdcm.provision.helpers.cloud_init import wait_cloud_init_completes
from sdcm.provision.provisioner import (
    PricingModel,
    VmInstance,
    ProvisionError,
    ProvisionUnrecoverableError,
    Provisioner,
    InstanceDefinition,
    OperationPreemptedError,
)
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


@retrying(n=3, sleep_time=5, allowed_exceptions=(ProvisionError,))
def provision_with_retry(
    provisioner: Provisioner, definitions: List[InstanceDefinition], pricing_model: PricingModel
) -> List[VmInstance]:
    return provisioner.get_or_create_instances(definitions=definitions, pricing_model=pricing_model)


def provision_instances_with_fallback(
    provisioner: Provisioner,
    definitions: List[InstanceDefinition],
    pricing_model: PricingModel,
    fallback_on_demand: bool,
) -> List[VmInstance]:
    try:
        provision_with_retry(provisioner, definitions=definitions, pricing_model=pricing_model)
    except OperationPreemptedError:
        if pricing_model.is_spot() and fallback_on_demand:
            provision_with_retry(provisioner, definitions=definitions, pricing_model=PricingModel.ON_DEMAND)
        else:
            raise

    provisioned_instances = provisioner.get_or_create_instances(definitions=definitions)
    for definition, v_m in zip(definitions, provisioned_instances):
        if definition.use_public_ip:
            hostname = v_m.public_ip_address if v_m.public_ip_address else v_m.private_ip_address
        else:
            hostname = v_m.private_ip_address
        ssh_login_info = {
            "hostname": hostname,
            "user": v_m.user_name,
            "key_file": f"~/.ssh/{v_m.ssh_key_name}",
        }
        remoter = RemoteCmdRunnerBase.create_remoter(**ssh_login_info)
        wait_cloud_init_completes(remoter=remoter, instance=v_m)
        # todo: wait for scylla-machine-image service to complete if instance is scylla-db?
    return provisioned_instances


def provision_sct_resources(params: SCTConfiguration, test_config: TestConfig, **provisioner_config: Any) -> None:
    """Provision instances per SCT config, with optional backend region fallback and placement handoff.

    Backends that support whole-cluster region fallback (currently GCE) get a pre-provision zone/AZ
    resolve, drive provisioning through the shared region-fallback loop for a single-region config,
    and persist the resolved placement so a later Run Test step picks up any relocated region or zone. Other
    backends (AWS uses its own layout; Azure/OCI have no fallback yet) provision once, unchanged - the
    shared shape is ready for them to opt in.
    """

    def provision_once() -> None:
        _provision_sct_resources_once(params=params, test_config=test_config, **provisioner_config)

    if params.get("cluster_backend") == "gce":
        _provision_gce_resources(params, test_config, provision_once)
    else:
        provision_once()


def _provision_sct_resources_once(params: SCTConfiguration, test_config: TestConfig, **provisioner_config: Any) -> None:
    """Provision every region's instances once in the currently configured region/AZ."""
    builder = region_definition_builder.get_builder(params=params, test_config=test_config)
    definitions_per_region = builder.build_all_region_definitions()
    pricing_model = PricingModel(params.get("instance_provision"))
    provision_fallback_on_demand = params.get("instance_provision_fallback_on_demand")
    for request in definitions_per_region:
        # Merge backend-specific configuration with provided provisioner config
        backend_config = dict(provisioner_config)
        if request.provisioner_config:
            backend_config.update(request.provisioner_config)

        provisioner = provisioner_factory.create_provisioner(
            backend=request.backend,
            test_id=request.test_id,
            region=request.region,
            availability_zone=request.availability_zone,
            **backend_config,
        )
        provision_instances_with_fallback(
            provisioner=provisioner,
            definitions=request.definitions,
            pricing_model=pricing_model,
            fallback_on_demand=provision_fallback_on_demand,
        )


def _provision_gce_resources(params: SCTConfiguration, test_config: TestConfig, provision_once: Any) -> None:
    """GCE provisioning: upfront zone filter, optional zone/region fallback, placement handoff.

    Capacity exhaustion escalates in two steps, each independently gated by its own config flag:
    ``fallback_to_next_availability_zone`` retries the remaining zones of the configured region, and
    only once those are gone does ``fallback_to_next_region`` relocate - the whole cluster for a
    single-region config, or just the exhausted DC for a multi-region one.

    Any final placement that differs from the configured one - a relocated region, but also a
    zone-only change from AZ fallback or from the resolver picking a zone when none is configured -
    is persisted to the resolved-placement handoff. Unlike AWS, which finds instances region-wide by
    ``test_id`` tag, GCE instance discovery is zone-scoped, so the later Run Test step (a separate
    hydra command) must be pointed at the exact zone or it provisions a duplicate cluster.
    """
    # Both baselines must be captured before the resolver runs: with no availability_zone configured
    # it picks a random valid zone, and that pick has to be detected as a change so the Run Test step
    # reuses it instead of rolling its own.
    original_region = " ".join(params.gce_datacenters) if params.gce_datacenters else None
    original_az = params.get("availability_zone")
    GceAZResolver(params).resolve()
    test_id = str(test_config.test_id())
    network_name = params.get("gce_network")

    # Zone fallback wraps the single provisioning attempt, so it is exhausted before the region loop
    # below ever sees a capacity error. The legacy cluster path has its own per-node zone retry in
    # sdcm.cluster_gce; this is the modern path's equivalent.
    provision_attempt = gce_region_fallback.provision_with_az_fallback(
        params=params,
        test_id=test_id,
        network_name=network_name,
        provision_once=provision_once,
    )

    if is_region_fallback_enabled(params):
        gce_region_fallback.provision_with_fallback(
            params=params,
            test_id=test_id,
            network_name=network_name,
            provision_once=provision_attempt,
            error_factory=ProvisionUnrecoverableError,
        )
    else:
        provision_attempt()

    region_name = " ".join(params.gce_datacenters) if params.gce_datacenters else None
    availability_zone = params.get("availability_zone")
    if region_name and (region_name != original_region or availability_zone != original_az):
        test_config.write_resolved_placement(
            params.get("reuse_cluster") or str(test_config.test_id()),
            region_name=region_name,
            availability_zone=availability_zone,
        )
