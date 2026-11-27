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
    """User data object whose script echoes two lines, used as the VM's boot payload."""

    @property
    def script_to_run(self) -> str:
        """Return a two-line echo script."""
        return """echo OK
        echo another command"""


# fixture defines which provisioners to test
@pytest.fixture(scope="module", params=["azure", "fake"])
def backend(request):
    """Parametrize the module over the azure and fake provisioner backends."""
    return request.param


@pytest.fixture(scope="module")
def image_id(backend):
    """Return a valid image id for the current backend."""
    if backend == "azure":
        return "OpenLogic:CentOS:7_9:latest"
    return "some-image-id"


@pytest.fixture(scope="module")
def image_type(backend):
    """Return a valid instance type for the current backend."""
    if backend == "azure":
        return "Standard_D2s_v3"
    return "test-image-type"


@pytest.fixture(scope="module")
def test_id():
    """Return a random test id shared by the module."""
    return f"{uuid.uuid4()!s}"


@pytest.fixture(scope="module")
def region(backend):
    """Return a valid region for the current backend."""
    if backend == "azure":
        return "eastus"
    return "some-region"


@pytest.fixture(scope="module")
def definition(image_id, image_type):
    """Return the definition of a tagged test VM with a public IP and the printing user data."""
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
    """Return the kwargs shared by every provisioner created in the module."""
    return {"test_id": test_id, "region": region, "availability_zone": "a", "azure_service": azure_service}


@pytest.fixture(scope="function")
def provisioner(backend, provisioner_params):
    """Return a fresh provisioner for the current backend."""
    return provisioner_factory.create_provisioner(backend, **provisioner_params)


def test_can_provision_scylla_vm(region, definition, provisioner, backend, provisioner_params):
    """A created VM matches its definition, is reused on a repeated call and is rediscovered by a new provisioner."""
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
    """discover_provisioners() finds a provisioner for the test's region and test id."""
    provisioner = provisioner_factory.discover_provisioners(backend=backend, **provisioner_params)[0]
    assert provisioner.region == region
    assert provisioner.test_id == test_id


def test_discover_provisioners_wrong_backend(provisioner_params):
    """discover_provisioners() raises ProvisionerError for an unregistered backend."""
    wrong_backend = "absent-name-in-backend-mapping"
    match_err_msg = "Provisioner class was not registered for the '%s' backend" % wrong_backend
    with pytest.raises(ProvisionerError, match=match_err_msg):
        provisioner_factory.discover_provisioners(backend=wrong_backend, **provisioner_params)


def test_can_add_tags(provisioner, definition, backend, provisioner_params):
    """Added instance tags are visible both in the cache and to a newly created provisioner."""
    provisioner.add_instance_tags(definition.name, {"tag_key": "tag_value"})
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"

    # validate real tags change
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"


def test_can_run_command(provisioner, definition, backend, provisioner_params):
    """run_command() on a provisioned VM returns the command's stdout."""
    assert (
        provisioner.get_or_create_instance(definition).run_command('echo "hello instance"').stdout == "hello instance\n"
    )

    # validate real tags change
    assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"


def test_null_tag_value_is_replaced_with_empty_string(provisioner, definition, backend, provisioner_params):
    """On Azure, a 'null' tag value is stored as an empty string."""
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
    """cleanup() terminates all instances, emptying both the cache and a new provisioner's listing."""
    provisioner.get_or_create_instance(definition)
    assert len(provisioner.list_instances()) == 1
    provisioner.cleanup(wait=True)

    # validate cache has been cleaned up
    assert not provisioner.list_instances()

    # validate real termination
    provisioner = provisioner_factory.create_provisioner(backend=backend, **provisioner_params)
    assert not provisioner.list_instances(), "failed cleaning up resources"
