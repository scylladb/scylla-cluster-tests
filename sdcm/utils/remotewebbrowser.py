import time
import logging

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

from sdcm.auto_ssh import start_auto_ssh, is_auto_ssh_running
from sdcm.utils.common import get_free_port


LOGGER = logging.getLogger(__name__)


class RemoteWebDriverContainer():
    name = "awebdriver"
    remote_port = 4444

    def __init__(self, node):
        """Create instance of RemoteWebDriver
        Arguments:
            node {BaseNode} -- Node where container should run
        """
        self.node = node

    def run(self):
        """
        Start docker container with Selenium Chrome driver

        """
        LOGGER.debug("Start container with RemoteWebDriver")
        self.kill()
        self.node.remoter.run("docker run --privileged \
                                  -d -p {0.remote_port}:{0.remote_port} \
                                  -v /dev/shm:/dev/shm \
                                  --name {0.name} \
                                  selenium/standalone-chrome".format(self))
        time.sleep(5)

    def kill(self):
        """
        Kill docker container with Selenium Chrome driver
        """
        LOGGER.debug("Kill container with RemoteWebDriver")
        self.node.remoter.run("docker rm -f {0.name}".format(self),
                              ignore_status=True,
                              verbose=False)

    def is_running(self):
        """
        Check if container is running

        :returns: if container with Selenium chrome driver runnin on node
        :rtype: {boolean}
        """
        result = self.node.remoter.run("docker ps", ignore_status=True, verbose=False)
        return self.name in result.stdout


class RemoteBrowser():
    local_port = get_free_port()
    load_page_timeout = 30

    def __init__(self, remote_web_driver, use_tunnel=True):
        """Instance of remote browser

        Create instance of remote browser for docker
        container remote_web_driver
        :param remote_web_driver: Instance of RemoteWebDriverContainer
        :type remote_web_driver: RemoteWebDriverContainer
        :param use_tunnel: use auto_ssh container to access, defaults to True
        :type use_tunnel: bool, optional
        """
        self.remote_web_driver = remote_web_driver
        self._use_tunnel = use_tunnel
        self._remote_browser = self._get_remote_browser()

    @property
    def browser(self):
        return self._remote_browser

    def _get_remote_browser(self):
        """Get instance of remote browser

        Get remote browser as instance of Selenium
        remote webdriver running in containter described by RemoteWebDriverContainer

        if self._use_tunnel true, start container with auto_ssh to up tunnel to
        node were docker with remote selenium driver is running

        :returns: Opened session to remote selenium browser
        :rtype: {selenium.webdriver.Remote}
        """
        chrome_options = webdriver.ChromeOptions()

        if not is_auto_ssh_running(self.remote_web_driver.name, self.remote_web_driver.node) and self._use_tunnel:
            LOGGER.debug("Start autossh for Selenium remote webdriver ")
            start_auto_ssh(self.remote_web_driver.name,
                           self.remote_web_driver.node,
                           self.remote_web_driver.remote_port,
                           self.local_port,
                           ssh_mode="-L")
            LOGGER.debug("Waiting tunnel is up")
            time.sleep(5)
        if self._use_tunnel:
            remote_browser_url = "http://127.0.0.1:{0.local_port}/wd/hub".format(self)
        else:
            remote_browser_url = "http://{0.external_address}:{0.remote_port}/wd/hub".format(self.remote_web_driver)

        driver = webdriver.Remote(
            command_executor=remote_browser_url,
            options=chrome_options
        )

        return driver

    def get_screenshot(self, url, screenshot_path, resolution="1920px*1280px"):
        """get screenshot of url and save to file screenshot_path

        Use remote selenium webdriver containter, open url
        wait page loading for time and create screenshot

        Arguments:
            node {BaseNode} -- monitor node
            url {str} -- url to load
            screenshot_path {str} -- path to save screenshot in format 1920px*1280px

        Keyword Arguments:
            resolution {str} -- [description] (default: {"1920x1280"})
        """

        def wait_page_loading(_):
            time.sleep(5)
            if time.time() - start_time < self.load_page_timeout:
                return False
            else:
                return True

        resolution = resolution.split("*")
        self._remote_browser.set_window_size(int(resolution[0][:-2]), int(resolution[1][:-2]))
        LOGGER.info("Get screenshot of page %s", url)
        self._remote_browser.get(url)
        start_time = time.time()
        try:
            LOGGER.debug('Wait page loading ...')
            WebDriverWait(self._remote_browser, self.load_page_timeout * 2).until(wait_page_loading)
        finally:
            LOGGER.debug('Create snapshot')
            self._remote_browser.get_screenshot_as_file(screenshot_path)
