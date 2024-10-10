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

import abc
from dataclasses import dataclass, field
from textwrap import dedent
from typing import List, Dict

import yaml

CLOUD_INIT_SCRIPTS_PATH = "/var/lib/sct/cloud-init"


@dataclass
class UserDataObject(abc.ABC):
    """
    UserDataObject represents installed packages and script that will be executed on the first boot of new VM instance.
    User data concept comes from 'cloud-init' library. For more info refer cloud-init documentation.
    """

    @property
    def name(self):
        return self.__class__.__name__

    @property
    def is_applicable(self) -> bool:
        """Defines if given user data is applicable in given context.

        E.g. workaround for ipv6 only when is AWS and ipv6 configured"""
        return True

    @property
    def packages_to_install(self) -> set[str]:
        """Specifies packages to be installed."""
        return set()

    @property
    def script_to_run(self) -> str:
        """Specifies script that is going to be executed after first boot of VM instance"""
        return ""

    @property
    def scylla_machine_image_json(self) -> str:
        """Specifies configuration file accepted by scylla-machine-image service"""
        return ""


@dataclass
class UserDataBuilder:
    """Generates content for cloud-init"""
    user_data_objects: List[UserDataObject] = field(default_factory=list)

    @property
    def yum_repos(self) -> Dict:
        return {
            "yum_repos":
                {
                    "epel-release": {
                        "baseurl": "https://dl.fedoraproject.org/pub/epel/9/Everything/$basearch",
                        "enabled": True,
                        "failovermethod": "priority",
                        "gpgcheck": True,
                        "gpgkey": "https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-9",
                        "name": "Extra Packages for Enterprise Linux 9 - Everything"
                    }
                }
        }

    @property
    def apt_configuration(self) -> Dict:
        return yaml.safe_load(dedent("""
                                        apt:
                                          conf: |
                                            Acquire::Retries "60";
                                            DPkg::Lock::Timeout "60";
                                     """))

    def build_user_data_yaml(self) -> str:
        """
        Function creating cloud-init applicable file in yaml format from UserDataObjects.

        For each user data object (with script defined) will generate script file on VM Instance and add it's invocation to runcmd.
        In case of script execution failure it will create .failed file for each failed script.
        """
        packages = set()
        scripts = []
        runcmds = []
        for idx, user_data_object in enumerate(self.user_data_objects):
            script_path = f"{CLOUD_INIT_SCRIPTS_PATH}/{idx}_{user_data_object.name}.sh"
            packages.update(user_data_object.packages_to_install)
            if user_data_object.script_to_run:
                scripts.append({"content": user_data_object.script_to_run,
                                "path": script_path,
                                "permissions": "0644"
                                })
                runcmds.append(
                    f"cd {CLOUD_INIT_SCRIPTS_PATH}; bash -eux {script_path}; test  $? = 0 || touch {script_path}.failed")
        # in case of problems with creating scripts, cloud-init won't run anything and will not report any error
        # to fix it create 'done' file as last step to enable further verification if executed at all
        runcmds.append(f"mkdir -p {CLOUD_INIT_SCRIPTS_PATH} && touch {CLOUD_INIT_SCRIPTS_PATH}/done")
        user_data_yaml = yaml.dump(data={
            "packages": list(packages),
            "write_files": scripts,
            "runcmd": runcmds
        } | self.yum_repos | self.apt_configuration)
        return "#cloud-config\n" + user_data_yaml

    def get_scylla_machine_image_json(self):
        """Returns json applicable for scylla-machine-image service."""
        for user_data_object in self.user_data_objects:
            if smi_json := user_data_object.scylla_machine_image_json:
                return smi_json
        return ""
