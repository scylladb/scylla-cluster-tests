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
from invoke import Result

from sdcm.provision.provisioner import PricingModel, provisioner_factory
from sdcm.sct_provision.instances_provider import provision_sct_resources
from sdcm.test_config import TestConfig


def test_can_provision_instances_according_to_sct_configuration(params, test_config, azure_service, fake_remoter):
    """Integration test for provisioning sct resources according to SCT configuration."""
    fake_remoter.result_map = {r"sudo cloud-init status --wait": Result(stdout="..... \n status: done", stderr="nic", exited=0),
                               r"cloud-init --version 2>&1": Result(stdout="/bin/ 19.2-amzn", stderr="", exited=0),
                               r"ls /var/lib/sct/cloud-init": Result(stdout="done", exited=0)}
    tags = TestConfig.common_tags()
    provision_sct_resources(params=params, test_config=test_config, azure_service=azure_service)
    provisioner_eastus = provisioner_factory.create_provisioner(
        backend="azure", test_id=params.get("test_id"), region="eastus", availability_zone="1", azure_service=azure_service)
    eastus_instances = provisioner_eastus.list_instances()
    db_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "scylla-db"]
    loader_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "loader"]
    monitor_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "monitor"]

    assert len(db_nodes) == 3
    assert len(loader_nodes) == 2
    assert len(monitor_nodes) == 1
    db_node = db_nodes[0]
    assert db_node.region == "eastus"
    assert list(db_node.tags.keys()) == list(tags.keys()) + ["NodeType", "keep_action", "NodeIndex"]
    assert db_node.pricing_model == PricingModel.SPOT

    provisioner_easteu = provisioner_factory.create_provisioner(backend="azure", test_id=params.get("test_id"),
                                                                region="easteu", availability_zone="1", azure_service=azure_service)
    easteu_instances = provisioner_easteu.list_instances()
    db_nodes = [node for node in easteu_instances if node.tags['NodeType'] == "scylla-db"]
    loader_nodes = [node for node in easteu_instances if node.tags['NodeType'] == "loader"]
    monitor_nodes = [node for node in easteu_instances if node.tags['NodeType'] == "monitor"]

    assert len(db_nodes) == 1
    assert len(loader_nodes) == 0
    assert len(monitor_nodes) == 0
    db_node = db_nodes[0]
    assert db_node.region == "easteu"
    assert list(db_node.tags.keys()) == list(tags.keys()) + ["NodeType", "keep_action", "NodeIndex"]
    assert db_node.pricing_model == PricingModel.SPOT


def test_fallback_on_demand_when_spot_fails(fallback_on_demand, params, test_config, azure_service, fake_remoter):

    fake_remoter.result_map = {r"sudo cloud-init status --wait": Result(stdout="..... \n status: done", stderr="nic", exited=0),
                               r"cloud-init --version 2>&1": Result(stdout="/bin/ 19.2-amzn", stderr="", exited=0),
                               r"ls /var/lib/sct/cloud-init": Result(stdout="done", exited=0)}
    provision_sct_resources(params=params, test_config=test_config, azure_service=azure_service)
    provisioner_eastus = provisioner_factory.create_provisioner(
        backend="azure", test_id=params.get("test_id"), region="eastus", availability_zone="1", azure_service=azure_service)
    eastus_instances = provisioner_eastus.list_instances()
    db_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "scylla-db"]
    loader_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "loader"]
    monitor_nodes = [node for node in eastus_instances if node.tags['NodeType'] == "monitor"]

    assert len(db_nodes) == 3
    assert len(loader_nodes) == 2
    assert len(monitor_nodes) == 1
    for node in db_nodes:
        assert node.pricing_model == PricingModel.ON_DEMAND
    for node in loader_nodes:
        assert node.pricing_model == PricingModel.SPOT
    for node in monitor_nodes:
        assert node.pricing_model == PricingModel.SPOT
