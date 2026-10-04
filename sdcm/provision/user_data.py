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
import gzip
from dataclasses import dataclass, field
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from textwrap import dedent
from typing import List, Dict

import yaml

from sdcm.provision.common.utils import minify_shell_script

CLOUD_INIT_SCRIPTS_PATH = "/var/lib/sct/cloud-init"
# what cloud-init recognizes a gzipped part by, once unpacked
CLOUD_INIT_HEADERS = ("#!", "#cloud-config")


def gzipped_mime_part(content: str, filename: str) -> MIMEApplication:
    """A user data part as gzip, which cloud-init unpacks and handles like the plain part.

    Cloud user data is small: EC2 allows 16 KB, OCI 32,000 bytes of base64 for all metadata
    (SCT-1147); compressed, a boot script takes about a third of the room. Never compress the
    whole user data: scylla-machine-image reads it as text to find its x-scylla/json part, and
    falls back to its defaults (starting scylla itself) when it cannot decode it.

    Once unpacked, the part has no MIME type of its own: cloud-init picks the handler from its
    first line, and silently skips content which starts with neither `#!` nor `#cloud-config`.

    Args:
        content: the part, a shell script or a cloud-config, starting with its `#!` / `#cloud-config` line.
        filename: the name of the uncompressed part.

    Returns:
        The MIME part to attach.

    Raises:
        ValueError: when cloud-init could not tell what the unpacked content is.
    """
    if not content.startswith(CLOUD_INIT_HEADERS):
        raise ValueError(f"{filename}: cloud-init needs a {' or '.join(CLOUD_INIT_HEADERS)} first line to run it")
    # mtime=0 keeps the payload identical between runs of the same config
    part = MIMEApplication(gzip.compress(content.encode("utf-8"), mtime=0), "x-gzip")
    part.add_header("Content-Disposition", f'attachment; filename="{filename}.gz"')
    return part


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
            "yum_repos": {
                "epel-release": {
                    "baseurl": "https://dl.fedoraproject.org/pub/epel/9/Everything/$basearch",
                    "enabled": True,
                    "failovermethod": "priority",
                    "gpgcheck": True,
                    "gpgkey": "https://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-9",
                    "name": "Extra Packages for Enterprise Linux 9 - Everything",
                }
            }
        }

    @property
    def apt_configuration(self) -> Dict:
        return yaml.safe_load(
            dedent("""
                                        apt:
                                          conf: |
                                            Acquire::Retries "60";
                                            DPkg::Lock::Timeout "300";
                                     """)
        )

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
                scripts.append(
                    {
                        "content": minify_shell_script(user_data_object.script_to_run),
                        "path": script_path,
                        "permissions": "0644",
                    }
                )
                runcmds.append(
                    f"cd {CLOUD_INIT_SCRIPTS_PATH}; bash -eux {script_path}; test  $? = 0 || touch {script_path}.failed"
                )
        # in case of problems with creating scripts, cloud-init won't run anything and will not report any error
        # to fix it create 'done' file as last step to enable further verification if executed at all
        runcmds.append(f"mkdir -p {CLOUD_INIT_SCRIPTS_PATH} && touch {CLOUD_INIT_SCRIPTS_PATH}/done")
        user_data_yaml = yaml.dump(
            data={"packages": list(packages), "write_files": scripts, "runcmd": runcmds}
            | self.yum_repos
            | self.apt_configuration,
            width=float("inf"),  # prevent YAML from breaking long lines with newlines
            default_flow_style=False,  # Do not use JSON-like brackets anywhere, classic YAML format
        )
        return "#cloud-config\n" + user_data_yaml

    def get_scylla_machine_image_json(self):
        """Returns json applicable for scylla-machine-image service."""
        for user_data_object in self.user_data_objects:
            if smi_json := user_data_object.scylla_machine_image_json:
                return smi_json
        return ""

    def build_mime_multipart_user_data(self) -> str:
        smi_json = self.get_scylla_machine_image_json()
        yaml_content = self.build_user_data_yaml()

        msg = MIMEMultipart()

        # Scylla JSON
        if smi_json:
            part = MIMEBase("x-scylla", "json")
            part.set_payload(smi_json)
            part.add_header("Content-Disposition", 'attachment; filename="scylla_machine_image.json"')
            msg.attach(part)

        # Cloud config, compressed even without a JSON part: loaders and monitors carry the most scripts
        msg.attach(gzipped_mime_part(yaml_content, filename="cloud-config.txt"))

        return msg.as_string()
