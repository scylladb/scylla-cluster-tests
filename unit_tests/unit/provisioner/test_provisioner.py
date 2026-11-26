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

import uuid
from datetime import datetime, timezone

import pytest

from sdcm.keystore import KeyStore
from sdcm.provision.provisioner import (
    InstanceDefinition,
    ProvisionerError,
    provisioner_factory,
)
from sdcm.provision.user_data import UserDataObject


class PrintingTestUserDataObject(UserDataObject):
    """User data that runs a script echoing two lines."""

    @property
    def script_to_run(self) -> str:
        """Shell script printing OK and another command."""
        return """echo OK
        echo another command"""


# fixture defines which provisioners to test
@pytest.fixture(scope="module", params=["azure", "fake"])
def backend(request):
    """Provisioner backend under test: azure or fake."""
    return request.param


@pytest.fixture(scope="module")
def image_id(backend):
    """Image id valid for the backend under test."""
    if backend == "azure":
        return "OpenLogic:CentOS:7_9:latest"
    return "some-image-id"


@pytest.fixture(scope="module")
def image_type(backend):
    """Instance type valid for the backend under test."""
    if backend == "azure":
        return "Standard_D2s_v3"
    return "test-image-type"


@pytest.fixture(scope="module")
def test_id():
    """Random test id shared by the provisioners of one module run."""
    return f"{uuid.uuid4()!s}"


@pytest.fixture(scope="module")
def region(backend):
    """Region valid for the backend under test."""
    if backend == "azure":
        return "eastus"
    return "some-region"


@pytest.fixture(scope="module")
def definition(image_id, image_type):
    """InstanceDefinition for test-vm-1 with a public IP and user data."""
    return InstanceDefinition(
        name="test-vm-1",
        image_id=image_id,
        type=image_type,
        user_name="tester",
        ssh_key=KeyStore().get_ssh_key_pair(name="scylla_test_id_ed25519"),
        tags={"test-tag": "test_value"},
        user_data=[PrintingTestUserDataObject()],
        use_public_ip=True,
    )


@pytest.fixture(scope="module")
def provisioner_params(test_id, region, azure_service):
    """Keyword arguments for provisioner_factory.create_provisioner."""
    return {"test_id": test_id, "region": region, "availability_zone": "a", "azure_service": azure_service}


@pytest.fixture(scope="function")
def provisioner(backend, provisioner_params):
    """Provisioner of the backend under test, built from provisioner_params."""
    return provisioner_factory.create_provisioner(backend, **provisioner_params)


def test_can_provision_scylla_vm(region, definition, provisioner, backend, provisioner_params):
    """A VM is created once, reports its fields, and is rediscovered by a new provisioner."""
    creation_time = datetime.now(timezone.utc).replace(microsecond=0)
    v_m = provisioner.get_or_create_instances(definitions=[definition])[0]
    assert v_m.name == definition.name
    assert v_m.region == region
    assert v_m.user_name == definition.user_name
    assert v_m.public_ip_address
    assert v_m.private_ip_address
    assert v_m.tags == definition.tags
    assert v_m.creation_time >= creation_time

    assert v_m == provisioner.list_instances()[0]

    v_m_2 = provisioner.get_or_create_instance(definition)
    assert v_m is v_m_2, "provisioner should not not recreate vm with the same name"

    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    v_m._provisioner = provisioner
    assert v_m == provisioner.list_instances()[0], (
        "provisioner with the same params should rediscover created resources"
    )


def test_can_discover_regions(test_id, region, backend, provisioner_params):
    """discover_provisioners returns a provisioner with the test region and test id."""
    provisioner = provisioner_factory.discover_provisioners(backend=backend, **provisioner_params)[0]
    assert provisioner.region == region
    assert provisioner.test_id == test_id


def test_discover_provisioners_wrong_backend(provisioner_params):
    """discover_provisioners raises ProvisionerError for a backend that is not registered."""
    wrong_backend = "absent-name-in-backend-mapping"
    match_err_msg = "Provisioner class was not registered for the '%s' backend" % wrong_backend
    with pytest.raises(ProvisionerError, match=match_err_msg):
        provisioner_factory.discover_provisioners(backend=wrong_backend, **provisioner_params)


def test_can_add_tags(provisioner, definition, backend, provisioner_params):
    """Tags added to an instance are kept after a new provisioner rediscovers it."""
    provisioner.add_instance_tags(definition.name, {"tag_key": "tag_value"})
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"

    # validate real tags change
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"


def test_can_run_command(provisioner, definition, backend, provisioner_params):
    """run_command on a created instance returns the command output."""
    assert (
        provisioner.get_or_create_instance(definition).run_command('echo "hello instance"').stdout == "hello instance\n"
    )

    # validate real tags change
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"


def test_null_tag_value_is_replaced_with_empty_string(provisioner, definition, backend, provisioner_params):
    """Azure stores a null tag value as an empty string, also after rediscovery."""
    if backend != "azure":
        pytest.skip("Only Azure does not support null tags")
    provisioner.add_instance_tags(definition.name, {"tag_key": "null"})
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == ""

    # validate real tags change
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == ""


def test_can_terminate_vm_instance(provisioner, definition, backend, provisioner_params):
    """Should read from cache instead creating anything - so should be fast (after provisioner initialized)"""
    provisioner.terminate_instance(definition.name, wait=True)

    # validate cache has been cleaned up
    assert not provisioner.list_instances()

    # validate real termination
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert not provisioner.list_instances()


def test_can_trigger_cleanup(definition, provisioner, backend, provisioner_params):
    """cleanup(wait=True) terminates all instances, also as seen by a new provisioner."""
    provisioner.get_or_create_instance(definition)
    assert len(provisioner.list_instances()) == 1
    provisioner.cleanup(wait=True)

    # validate cache has been cleaned up
    assert not provisioner.list_instances()

    # validate real termination
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert not provisioner.list_instances(), "failed cleaning up resources"
