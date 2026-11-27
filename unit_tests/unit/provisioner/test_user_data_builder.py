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

import gzip
import json
from email import message_from_string

import pytest
import yaml

from sdcm.provision.common.utils import minify_shell_script
from sdcm.provision.user_data import UserDataBuilder, UserDataObject, gzipped_mime_part
from sdcm.sct_provision.user_data_objects.walinuxagent import EnableWaLinuxAgent


class ExampleUserDataObject(UserDataObject):
    """User data object with one package and a multi-line script."""

    @property
    def packages_to_install(self) -> set[str]:
        """Return a single example package."""
        return {"some-pkg-to-install"}

    @property
    def script_to_run(self) -> str:
        """Return a multi-line example script."""
        return """
        Some script
        that spans multiple lines
        """


class AnotherExampleUserDataObject(UserDataObject):
    """Second user data object with its own package and script, for merge tests."""

    @property
    def packages_to_install(self) -> set[str]:
        """Return a package distinct from ExampleUserDataObject's."""
        return {"another-pkg-to-install"}

    @property
    def script_to_run(self) -> str:
        """Return a one-line example script."""
        return """
        Just another script
        """


class EmptyScriptUserDataObject(UserDataObject):
    """User data object with packages but an empty script."""

    @property
    def packages_to_install(self) -> set[str]:
        """Return one new package and one shared with AnotherExampleUserDataObject."""
        return {"pkg-from-empty", "another-pkg-to-install"}


class ScyllaImageUserDataObject(UserDataObject):
    """User data object that carries a scylla-machine-image JSON payload."""

    @property
    def scylla_machine_image_json(self) -> str:
        """Return a minimal scylla-machine-image JSON payload."""
        return json.dumps({"start_scylla_on_first_boot": False})


class NotApplicableUserDataObject(UserDataObject):
    """User data object that reports itself as not applicable."""

    @property
    def is_applicable(self) -> bool:
        """Return False so the builder skips this object."""
        return False


def test_user_data_builder_generates_valid_yaml_from_single_user_data_object():
    """One object yields #cloud-config with its packages, a minified script file, its runcmd and the done marker."""
    user_data_object_1 = ExampleUserDataObject()

    builder = UserDataBuilder(user_data_objects=[user_data_object_1])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert user_data_yaml.startswith("#cloud-config\n"), "user-data yaml must start with #cloud-config"
    assert loaded_yaml["packages"] == ["some-pkg-to-install"]
    assert (
        loaded_yaml["runcmd"][0]
        == "cd /var/lib/sct/cloud-init; bash -eux /var/lib/sct/cloud-init/0_ExampleUserDataObject.sh; test  $? = 0 "
        "|| touch /var/lib/sct/cloud-init/0_ExampleUserDataObject.sh.failed"
    )
    script_file = loaded_yaml["write_files"][0]
    assert script_file["path"] == "/var/lib/sct/cloud-init/0_ExampleUserDataObject.sh"
    assert script_file["content"] == minify_shell_script(user_data_object_1.script_to_run)
    assert script_file["permissions"] == "0644"
    assert loaded_yaml["runcmd"][1] == "mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"


def test_user_data_can_merge_user_data_objects_yaml():
    """Several objects merge their packages, and only non-empty scripts become write_files entries."""
    user_data_object_1 = ExampleUserDataObject()
    user_data_object_2 = AnotherExampleUserDataObject()
    user_data_object_3 = EmptyScriptUserDataObject()

    builder = UserDataBuilder(user_data_objects=[user_data_object_1, user_data_object_2, user_data_object_3])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert sorted(loaded_yaml["packages"]) == sorted(
        ["some-pkg-to-install", "another-pkg-to-install", "pkg-from-empty"]
    )
    script_files = loaded_yaml["write_files"]
    assert len(script_files) == 2, "empty script user data object should not be added"
    assert script_files[0]["content"] == minify_shell_script(user_data_object_1.script_to_run)
    assert script_files[1]["content"] == minify_shell_script(user_data_object_2.script_to_run)


def test_only_done_runcmd_in_yaml_when_no_user_data_objects():
    """With no objects, the YAML has no packages or files and only the done-marker runcmd."""
    builder = UserDataBuilder(user_data_objects=[])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert not loaded_yaml["packages"]
    assert not loaded_yaml["write_files"]
    assert loaded_yaml["runcmd"] == ["mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"]


def test_apt_configuration_bounds_retries():
    """With 60 retries, a slow mirror kept `apt-get update` running past the cloud-init wait."""
    loaded_yaml = yaml.safe_load(UserDataBuilder(user_data_objects=[]).build_user_data_yaml())

    assert loaded_yaml["apt"]["conf"].splitlines() == [
        'Acquire::Retries "3";',
        'Acquire::http::Timeout "60";',
        'DPkg::Lock::Timeout "300";',
    ]


def test_only_done_runcmd_in_yaml_when_no_applicable_user_data_objects():
    """Non-applicable objects are skipped, leaving only the done-marker runcmd."""
    builder = UserDataBuilder(user_data_objects=[NotApplicableUserDataObject()])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert not loaded_yaml["packages"]
    assert not loaded_yaml["write_files"]
    assert loaded_yaml["runcmd"] == ["mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"]


@pytest.mark.parametrize(
    "node_type,backend,expected",
    [
        ("scylla-db", "azure", True),
        ("oracle-db", "azure", True),  # azure oracle nodes need the agent enabled too
        ("loader", "azure", False),
        ("oracle-db", "aws", False),
    ],
)
def test_walinuxagent_applicability(node_type, backend, expected):
    """EnableWaLinuxAgent applies to Azure DB and oracle nodes only."""
    user_data_object = EnableWaLinuxAgent(
        test_config=None,
        params={"cluster_backend": backend},
        instance_name="unit-node-1",
        node_type=node_type,
    )

    assert user_data_object.is_applicable is expected


@pytest.mark.parametrize("with_scylla_image_json", [True, False], ids=["scylla-db", "loader"])
def test_mime_user_data_carries_the_cloud_config_compressed(with_scylla_image_json):
    """MIME user data gzips the cloud-config and keeps the scylla-machine-image JSON part as plain text."""
    user_data_objects = [ExampleUserDataObject()]
    if with_scylla_image_json:
        user_data_objects.append(ScyllaImageUserDataObject())
    builder = UserDataBuilder(user_data_objects=user_data_objects)

    parts = {
        part.get_content_type(): part
        for part in message_from_string(builder.build_mime_multipart_user_data()).walk()
        if not part.is_multipart()
    }

    # compressed for the OCI metadata limit; cloud-init unpacks it back into a cloud-config part
    cloud_config = gzip.decompress(parts.pop("application/x-gzip").get_payload(decode=True)).decode()
    assert cloud_config == builder.build_user_data_yaml()
    if with_scylla_image_json:
        # scylla-machine-image reads this part as plain text, so it must stay uncompressed
        assert json.loads(parts.pop("x-scylla/json").get_payload()) == {"start_scylla_on_first_boot": False}
    assert not parts


@pytest.mark.parametrize("content", ["echo no shebang\n", "packages: []\n", "\n#!/bin/bash\n"])
def test_gzipped_part_refuses_content_cloud_init_would_skip(content):
    """gzipped_mime_part() raises ValueError for content whose first line cloud-init cannot identify."""
    # unpacked, the part has no MIME type left: cloud-init only runs it by its first line
    with pytest.raises(ValueError, match="first line"):
        gzipped_mime_part(content, filename="user-script.txt")
