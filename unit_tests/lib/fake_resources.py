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
from sdcm.provision import provisioner_factory
from sdcm.provision.provisioner import PricingModel
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig


def prepare_fake_region(test_id: str, region: str, n_db_nodes: int = 3, n_loaders: int = 2, n_monitor_nodes: int = 1):
    params = {
        "test_id": test_id,
        "fake_image_db": "test:image:db:2",
        "fake_instance_type_db": "test-inst-type",
        "fake_image_user_name": "scyllaadm",
        "root_disk_size_db": 15,
        "fake_image_loader": "test:image:loader:2",
        "fake_instance_type_loader": "test-inst-type",
        "ami_loader_user": "centos",
        "fake_rood_tisk_size_loader": 15,
        "fake_image_monitor": "test:image:monitor:2",
        "fake_instance_type_monitor": "test-inst-type",
        "ami_monitor_user": "centos",
        "root_disk_size_monitor": 15,
        "fake_region_name": ["eastus"],
        "cluster_backend": "fake",
    }
    test_config = TestConfig()
    test_config.set_test_id_only(test_id)
    builder = region_definition_builder.get_builder(params=params, test_config=test_config)
    region_definition = builder.build_region_definition(region, "a", n_db_nodes, n_loaders, n_monitor_nodes)
    provisioner = provisioner_factory.create_provisioner(
        backend=region_definition.backend,
        test_id=region_definition.test_id,
        region=region_definition.region,
        availability_zone="a",
    )
    provisioner.get_or_create_instances(region_definition.definitions, PricingModel.SPOT)
