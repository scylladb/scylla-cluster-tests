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
# Copyright (c) 2026 ScyllaDB

"""The cloud-init wait in `provision_instances_with_fallback()` runs on all nodes concurrently.

This is the path GCE, Azure and OCI provision through. Waiting node by node made provisioning time
grow with the cluster size, so the waits must overlap, and a node whose cloud-init failed must still
fail provisioning.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.helpers.cloud_init import CloudInitError
from sdcm.provision.provisioner import InstanceDefinition, PricingModel
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.utils.parallel_object import ParallelObjectException


def _definition(index: int, use_public_ip: bool = False) -> InstanceDefinition:
    return InstanceDefinition(
        name=f"node-{index}",
        image_id="image-1",
        type="n2-highmem-16",
        user_name="scyllaadm",
        ssh_key=MagicMock(),
        use_public_ip=use_public_ip,
    )


def _instance(index: int) -> MagicMock:
    instance = MagicMock(
        public_ip_address=f"1.2.3.{index}",
        private_ip_address=f"10.0.0.{index}",
        user_name="scyllaadm",
        ssh_key_name="test-key",
    )
    instance.name = f"node-{index}"
    return instance


def _provision(definitions, instances, wait_side_effect):
    """Drive the shared path with a stubbed provisioner; return the patched remoter class."""
    provisioner = MagicMock()
    provisioner.get_or_create_instances.return_value = instances
    with (
        patch("sdcm.sct_provision.instances_provider.record_spot_provision_outcome"),
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes", side_effect=wait_side_effect),
        patch("sdcm.sct_provision.instances_provider.RemoteCmdRunnerBase") as remoter_class,
    ):
        result = provision_instances_with_fallback(
            provisioner=provisioner,
            definitions=definitions,
            pricing_model=PricingModel.ON_DEMAND,
            fallback_on_demand=False,
        )
    return result, remoter_class


def test_cloud_init_waits_overlap():
    count = 4
    barrier = threading.Barrier(count, timeout=5)

    def wait(remoter, instance):
        barrier.wait()  # raises BrokenBarrierError unless all the nodes are waited on at the same time

    instances = [_instance(index) for index in range(1, count + 1)]

    result, _ = _provision([_definition(index) for index in range(1, count + 1)], instances, wait)

    assert result == instances


def test_each_node_is_waited_on_once_at_its_own_address():
    waited = []
    definitions = [_definition(1, use_public_ip=True), _definition(2, use_public_ip=False)]
    instances = [_instance(1), _instance(2)]

    _, remoter_class = _provision(definitions, instances, lambda remoter, instance: waited.append(instance.name))

    assert sorted(waited) == ["node-1", "node-2"]
    hostnames = sorted(call.kwargs["hostname"] for call in remoter_class.create_remoter.call_args_list)
    # node-1 asked for its public address, node-2 for its private one
    assert hostnames == ["1.2.3.1", "10.0.0.2"]


def test_cloud_init_failure_on_one_node_fails_provisioning():
    waited = []

    def wait(remoter, instance):
        waited.append(instance.name)
        if instance.name == "node-2":
            raise CloudInitError("cloud-init failed on node-2")

    definitions = [_definition(index) for index in range(1, 4)]
    instances = [_instance(index) for index in range(1, 4)]

    with pytest.raises(ParallelObjectException) as error:
        _provision(definitions, instances, wait)

    # the failure names the node and keeps the underlying error, and no node was skipped
    assert "cloud-init failed on node-2" in str(error.value)
    assert sorted(waited) == ["node-1", "node-2", "node-3"]


def test_no_instances_skips_the_wait():
    result, remoter_class = _provision([], [], MagicMock(side_effect=AssertionError("must not be called")))

    assert result == []
    remoter_class.create_remoter.assert_not_called()
