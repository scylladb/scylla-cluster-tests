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
# Copyright (c) 2020 ScyllaDB

import os
import json
import random
from functools import cached_property

from libcloud.compute.providers import Provider, get_driver

from sdcm.keystore import KeyStore
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container


GOOGLE_CLOUD_SDK_IMAGE = "google/cloud-sdk:311.0.0-alpine"

GceDriver = get_driver(Provider.GCE)  # pylint: disable=invalid-name


def append_zone(region: str) -> str:
    assert region.startswith("us-east1"), "only `us-east1' region is supported"
    if region.count("-") == 1:
        # us-east1 zones: b, c, and d. Details: https://cloud.google.com/compute/docs/regions-zones#locations
        return f"{region}-{random.choice('bccdd')}"  # choose zones c and d twice as often as zone b
    return region


def _get_gce_service(credentials: dict, datacenter: str) -> GceDriver:
    return GceDriver(user_id=credentials["project_id"] + "@appspot.gserviceaccount.com",
                     key=credentials["private_key"],
                     datacenter=datacenter,
                     project=credentials["project_id"])


def get_gce_services(regions: list) -> dict:
    credentials = KeyStore().get_gcp_credentials()
    return {region_az: _get_gce_service(credentials, region_az) for region_az in map(append_zone, regions)}


class GcloudContainerMixin:
    """Run gcloud command using official Google Cloud SDK Docker image.

    See more details here: https://hub.docker.com/r/google/cloud-sdk
    """

    def gcloud_container_run_args(self) -> dict:
        volumes = {os.path.expanduser("~/.kube"): {"bind": "/.kube", "mode": "rw"},
                   os.path.expanduser("~/.config/gcloud"): {"bind": "/.config/gcloud", "mode": "rw"}, }
        return dict(image=GOOGLE_CLOUD_SDK_IMAGE,
                    command="cat",
                    tty=True,
                    name=f"{self.name}-gcloud",
                    volumes=volumes,
                    user=f"{os.getuid()}:{os.getgid()}")

    @cached_property
    def _gcloud_container(self) -> Container:
        """Create Google Cloud SDK container.

        Cloud SDK requires to enable some authorization method first.  Because of that we start a container which
        runs forever using `cat' command (like Jenkins do), put a service account credentials and activate them.

        All consequent gcloud commands run using container.exec_run() method.
        """
        container = ContainerManager.run_container(self, "gcloud")
        credentials = KeyStore().get_gcp_credentials()
        credentials["client_email"] = f"{credentials['project_id']}@appspot.gserviceaccount.com"
        shell_command = f"umask 077 && echo '{json.dumps(credentials)}' > /tmp/gcloud_svc_account.json"
        res = container.exec_run(["sh", "-c", shell_command])
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")
        res = container.exec_run(["gcloud", "auth", "activate-service-account", credentials["client_email"],
                                  "--key-file", "/tmp/gcloud_svc_account.json",
                                  "--project", credentials["project_id"]])
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")
        return container

    def gcloud(self, command: str) -> str:
        res = self._gcloud_container.exec_run(["sh", "-c", f"gcloud {command}"])
        if res.exit_code:
            raise DockerException(f"{self._gcloud_container}: {res.output.decode('utf-8')}")
        return res.output.decode("utf-8")
