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
import logging
import threading

from libcloud.compute.providers import Provider, get_driver

from sdcm.keystore import KeyStore
from sdcm.utils.docker_utils import ContainerManager, DockerException, Container


GOOGLE_CLOUD_SDK_IMAGE = "google/cloud-sdk:311.0.0-alpine"

LOGGER = logging.getLogger(__name__)

GceDriver = get_driver(Provider.GCE)  # pylint: disable=invalid-name


def append_zone(region: str) -> str:
    assert region.startswith("us-east1"), "only `us-east1' region is supported"
    if region.count("-") == 1:
        # us-east1 zones: b, c, and d. Details: https://cloud.google.com/compute/docs/regions-zones#locations
        # Currently choose only zones c and d as zone b frequently fails allocating resources.
        return f"{region}-{random.choice('cd')}"
    return region


def _get_gce_service(credentials: dict, datacenter: str) -> GceDriver:
    return GceDriver(user_id=credentials["project_id"] + "@appspot.gserviceaccount.com",
                     key=credentials["private_key"],
                     datacenter=datacenter,
                     project=credentials["project_id"])


def get_gce_services(regions: list) -> dict:
    credentials = KeyStore().get_gcp_credentials()
    return {region_az: _get_gce_service(credentials, region_az) for region_az in map(append_zone, regions)}


class GcloudContextManager:
    def __init__(self, instance: 'GcloudContainerMixin', name: str):
        self._instance = instance
        self._name = name
        self._container = None

    def _span_container(self):
        try:
            self._container = self._instance._get_gcloud_container()  # pylint: disable=protected-access
        except Exception as exc:
            try:
                ContainerManager.destroy_container(self._instance, self._name)
            except Exception:  # pylint: disable=broad-except
                pass
            raise exc from None

    def _destroy_container(self):
        try:
            ContainerManager.destroy_container(self._instance, self._name)
        except Exception:  # pylint: disable=broad-except
            pass
        self._container = None

    def __enter__(self):
        self._span_container()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._destroy_container()

    def run(self, command) -> str:
        one_time = self._container is None
        if one_time:
            self._span_container()
        try:
            LOGGER.debug("Execute `gcloud %s'", command)
            res = self._container.exec_run(["sh", "-c", f"gcloud {command}"])
            if res.exit_code:
                raise DockerException(f"{self._container}: {res.output.decode('utf-8')}")
            return res.output.decode("utf-8")
        finally:
            if one_time:
                self._destroy_container()


class GcloudContainerMixin:
    """Run gcloud command using official Google Cloud SDK Docker image.

    See more details here: https://hub.docker.com/r/google/cloud-sdk
    """
    _gcloud_container_instance = None

    def gcloud_container_run_args(self) -> dict:
        kube_config_path = os.path.expanduser(os.environ.get('KUBECONFIG', '~/.kube/config'))
        volumes = {
            os.path.dirname(kube_config_path): {"bind": "/.kube", "mode": "rw"}
        }
        return dict(image=GOOGLE_CLOUD_SDK_IMAGE,
                    command="cat",
                    tty=True,
                    name=f"{self.name}-gcloud",
                    volumes=volumes,
                    user=f"{os.getuid()}:{os.getgid()}",
                    tmpfs={'/.config': f'size=50M,uid={os.getuid()}'}
                    )

    def _get_gcloud_container(self) -> Container:
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
            raise DockerException(f"{container}[]: {res.output.decode('utf-8')}")
        return container

    @property
    def gcloud(self) -> GcloudContextManager:
        return GcloudContextManager(self, 'gcloud')


class GcloudTokenUpdateThread(threading.Thread):
    update_period = 1800

    def __init__(self, gcloud: GcloudContextManager, config_path: str, token_min_duration: int = 60):
        self._gcloud = gcloud
        self._config_path = config_path
        self._token_min_duration = token_min_duration
        self._termination_event = threading.Event()
        super().__init__(daemon=True)

    def run(self):
        wait_time = 0.01
        while not self._termination_event.wait(wait_time):
            try:
                gcloud_config = self._gcloud.run(
                    f'config config-helper --min-expiry={self._token_min_duration * 60} --format=json')
                with open(self._config_path, 'w') as gcloud_config_file:
                    gcloud_config_file.write(gcloud_config)
                    gcloud_config_file.flush()
                LOGGER.debug('Gcloud token has been updated and stored at %s', self._config_path)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.debug('Failed to read gcloud config: %s', exc)
                wait_time = 5
            else:
                wait_time = self.update_period

    def stop(self, timeout=None):
        self._termination_event.set()
        self.join(timeout)
