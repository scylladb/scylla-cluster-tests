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

import time
import logging
import os
from typing import Optional
from functools import cached_property

import paramiko
from docker import DockerClient  # pylint: disable=wrong-import-order
from selenium.webdriver import Remote, ChromeOptions

from sdcm.utils.docker_utils import ContainerManager, DOCKER_API_CALL_TIMEOUT
from sdcm.utils.common import get_free_port, wait_for_port, normalize_ipv6_url
from sdcm.utils.ssh_agent import SSHAgent


WEB_DRIVER_IMAGE = "selenium/standalone-chrome:3.141.59-20210713"
WEB_DRIVER_REMOTE_PORT = 4444
WEB_DRIVER_CONTAINER_START_DELAY = 30  # seconds

LOGGER = logging.getLogger(__name__)


class WebDriverContainerMixin:
    def web_driver_container_run_args(self) -> dict:
        return dict(image=WEB_DRIVER_IMAGE,
                    name=f"{self.name}-webdriver",
                    ports={f"{WEB_DRIVER_REMOTE_PORT}/tcp": None, },
                    privileged=True,
                    volumes={"/dev/shm": {"bind": "/dev/shm"}, })

    @property
    def web_driver_docker_client(self) -> Optional[DockerClient]:
        if not self.ssh_login_info or self.ssh_login_info["key_file"] is None:
            # running with docker backend, no real monitor node, fallback to use local docker
            return None
        SSHAgent.add_keys((self.ssh_login_info["key_file"], ))
        # since a bug in docker package https://github.com/docker-library/python/issues/517 that need to explicitly
        # pass down the port for supporting ipv6
        user = self.ssh_login_info['user']
        hostname = normalize_ipv6_url(self.ssh_login_info['hostname'])
        try:
            return DockerClient(base_url=f"ssh://{user}@{hostname}:22", timeout=DOCKER_API_CALL_TIMEOUT)
        except paramiko.ssh_exception.BadHostKeyException as exc:
            system_host_keys_path = os.path.expanduser("~/.ssh/known_hosts")
            system_host_keys = paramiko.hostkeys.HostKeys(system_host_keys_path)
            if system_host_keys.pop(exc.hostname, None):
                system_host_keys.save(system_host_keys_path)
            return DockerClient(base_url=f"ssh://{user}@{hostname}:22", timeout=DOCKER_API_CALL_TIMEOUT)


class RemoteBrowser:
    def __init__(self, node, use_tunnel=True):
        self.node = node
        backend = self.node.parent_cluster.params.get(
            "cluster_backend") if hasattr(self.node, 'parent_cluster') else None
        self.use_tunnel = bool(self.node.ssh_login_info and use_tunnel and backend not in ('docker',))

    @cached_property
    def browser(self):
        ContainerManager.run_container(self.node, "web_driver")

        LOGGER.debug("Waiting for WebDriver container is up")
        ContainerManager.wait_for_status(self.node, "web_driver", "running")

        port = ContainerManager.get_container_port(self.node, "web_driver", WEB_DRIVER_REMOTE_PORT)
        LOGGER.debug("WebDriver port is %s", port)

        if self.use_tunnel:
            LOGGER.debug("Start auto_ssh for Selenium remote WebDriver")
            ContainerManager.run_container(self.node, "auto_ssh:web_driver",
                                           local_port=port,
                                           remote_port=get_free_port(),
                                           ssh_mode="-L")

            LOGGER.debug("Waiting for SSH tunnel container is up")
            ContainerManager.wait_for_status(self.node, "auto_ssh:web_driver", status="running")

            host = "127.0.0.1"
            port = int(ContainerManager.get_environ(self.node, "auto_ssh:web_driver")["SSH_TUNNEL_REMOTE"])
        else:
            host = self.node.external_address

        LOGGER.debug("Waiting for port %s:%s is accepting connections", host, port)
        wait_for_port(host, port)

        time.sleep(WEB_DRIVER_CONTAINER_START_DELAY)

        return Remote(command_executor=f"http://{host}:{port}/wd/hub", options=ChromeOptions())

    def open(self, url, resolution="1920px*1280px"):
        LOGGER.info("Set resolution %s", resolution)
        self.browser.set_window_size(*resolution.replace("px", "").split("*", 1))
        LOGGER.info("Get url %s", url)
        self.browser.get(url)

    def get_screenshot(self, url, screenshot_path):
        LOGGER.info("Save a screenshot of %s to %s", url, screenshot_path)

        self.browser.get_screenshot_as_file(screenshot_path)

    def quit(self):
        self.browser.quit()

    def destroy_containers(self):
        names = ["web_driver"]
        if self.use_tunnel:
            names.append("auto_ssh:web_driver")

        for container_name in names:
            self.destroy_container(container_name)

    def destroy_container(self, name):
        try:
            if ContainerManager.get_container(self.node, name, raise_not_found_exc=False) is not None:
                LOGGER.debug("Destroy %s (%s) container", name, self)
                ContainerManager.destroy_container(self.node, name, ignore_keepalive=True)
                LOGGER.info("%s (%s) destroyed", name, self)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error("%s: some exception raised during container '%s' destroying", self, name, exc_info=exc)
