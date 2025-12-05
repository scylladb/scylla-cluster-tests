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

import yaml

from sdcm.provision.user_data import UserDataObject, UserDataBuilder


class ExampleUserDataObject(UserDataObject):
    @property
    def packages_to_install(self) -> set[str]:
        return {"some-pkg-to-install"}

    @property
    def script_to_run(self) -> str:
        return """
        Some script
        that spans multiple lines
        """


class AnotherExampleUserDataObject(UserDataObject):
    @property
    def packages_to_install(self) -> set[str]:
        return {"another-pkg-to-install"}

    @property
    def script_to_run(self) -> str:
        return """
        Just another script
        """


class EmptyScriptUserDataObject(UserDataObject):
    @property
    def packages_to_install(self) -> set[str]:
        return {"pkg-from-empty", "another-pkg-to-install"}


class NotApplicableUserDataObject(UserDataObject):
    @property
    def is_applicable(self) -> bool:
        return False


def test_user_data_builder_generates_valid_yaml_from_single_user_data_object():
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
    assert user_data_object_1.script_to_run in script_file["content"]
    assert script_file["permissions"] == "0644"
    assert loaded_yaml["runcmd"][1] == "mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"


def test_user_data_can_merge_user_data_objects_yaml():
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
    assert user_data_object_1.script_to_run in script_files[0]["content"]
    assert user_data_object_2.script_to_run in script_files[1]["content"]


def test_only_done_runcmd_in_yaml_when_no_user_data_objects():
    builder = UserDataBuilder(user_data_objects=[])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert not loaded_yaml["packages"]
    assert not loaded_yaml["write_files"]
    assert loaded_yaml["runcmd"] == ["mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"]


def test_only_done_runcmd_in_yaml_when_no_applicable_user_data_objects():
    builder = UserDataBuilder(user_data_objects=[NotApplicableUserDataObject()])
    user_data_yaml = builder.build_user_data_yaml()
    loaded_yaml = yaml.safe_load(user_data_yaml)

    assert not loaded_yaml["packages"]
    assert not loaded_yaml["write_files"]
    assert loaded_yaml["runcmd"] == ["mkdir -p /var/lib/sct/cloud-init && touch /var/lib/sct/cloud-init/done"]
