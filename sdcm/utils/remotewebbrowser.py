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
from typing import Optional

from docker import DockerClient  # pylint: disable=wrong-import-order
from selenium.webdriver import Remote, ChromeOptions

from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.common import get_free_port, wait_for_port
from sdcm.utils.ssh_agent import SSHAgent
from sdcm.utils.decorators import cached_property


WEB_DRIVER_IMAGE = "selenium/standalone-chrome:latest"
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
        if not self.ssh_login_info:
            return None
        SSHAgent.add_keys((self.ssh_login_info["key_file"], ))
        return DockerClient(base_url=f"ssh://{self.ssh_login_info['user']}@{self.ssh_login_info['hostname']}")


class RemoteBrowser:
    def __init__(self, node, use_tunnel=True):
        self.node = node
        self.use_tunnel = bool(self.node.ssh_login_info and use_tunnel)

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

    def get_screenshot(self, url, screenshot_path, resolution="1920px*1280px", load_page_screenshot_delay=30):
        LOGGER.info("Get a screenshot of %s", url)

        self.browser.set_window_size(*resolution.replace("px", "").split("*", 1))

        LOGGER.debug("Goto %s", url)
        self.browser.get(url)

        LOGGER.debug("Wait for %s seconds and write a screenshot of %s to %s",
                     load_page_screenshot_delay, url, screenshot_path)
        time.sleep(load_page_screenshot_delay)
        self.browser.get_screenshot_as_file(screenshot_path)
