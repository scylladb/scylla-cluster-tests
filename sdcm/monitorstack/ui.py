import logging

from typing import Tuple, List

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.common import exceptions

from sdcm.utils.ci_tools import get_test_name


LOGGER = logging.getLogger(__name__)
UI_ELEMENT_LOAD_TIMEOUT = 180
GRAFANA_USERNAME = "admin"
GRAFANA_PASSWORD = "admin"


class Login:
    path = "http://{ip}:{port}/login"
    username_locator = (By.XPATH, "//input[@name='user']")
    password_locator = (By.XPATH, "//input[@name='password']")
    login_button = (By.XPATH, "//button/span[contains(text(), 'Log in')]")
    skip_button = (By.XPATH, "//button/span[contains(text(), 'Skip')]")

    def __init__(self, remote_browser, ip, port):
        self.browser = remote_browser
        LOGGER.info("open url: %s", self.path.format(ip=ip, port=port))
        self.browser.get(self.path.format(ip=ip, port=port))

    def use_default_creds(self):
        LOGGER.info("Login to grafana with default credentials")
        try:
            WebDriverWait(self.browser, UI_ELEMENT_LOAD_TIMEOUT).until(
                EC.visibility_of_element_located(self.username_locator))
            username_element: WebElement = self.browser.find_element(*self.username_locator)
            password_element: WebElement = self.browser.find_element(*self.password_locator)
            username_element.clear()
            username_element.send_keys(GRAFANA_USERNAME)
            password_element.clear()
            password_element.send_keys(GRAFANA_PASSWORD)
            login_button: WebElement = self.browser.find_element(*self.login_button)
            login_button.click()
            self.skip_set_new_password()
            LOGGER.info("Logged in succesful")
        except Exception as details:  # pylint: disable=broad-except
            LOGGER.error("Authentication failed: %s", details)

    def skip_set_new_password(self):
        WebDriverWait(self.browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(self.skip_button))
        skip_element: WebElement = self.browser.find_element(*self.skip_button)
        skip_element.click()


class Panel:
    xpath_tmpl = """//h6[contains(@title,'{name}') and contains(@class, 'panel-title')]"""

    def __init__(self, name):
        self.name = name

    @property
    def xpath_locator(self):
        return (By.XPATH, self.xpath_tmpl.format(name=self.name))

    def wait_loading(self, remote_browser):
        LOGGER.info("Waiting panel %s loading", self.name)
        WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(self.xpath_locator))
        panel_title: WebElement = remote_browser.find_element(*self.xpath_locator)
        panel_elem: WebElement = panel_title.find_element_by_xpath("parent::*//parent::*")
        try:
            loading = panel_elem.find_element_by_xpath("div[contains(@class, 'panel-loading')]")
            WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(EC.invisibility_of_element(loading))
            LOGGER.debug("Panel %s could be without data", self.name)
        except exceptions.NoSuchElementException:
            LOGGER.debug("Panel %s loaded", self.name)
        except exceptions.TimeoutException:
            LOGGER.warning("Panel %s is still loading. Data on panel could displayed with delay", self.name)
            LOGGER.debug("Panel %s was not fully loaded", self.name)
        LOGGER.info("Work with panel %s done", self.name)


class Snapshot:  # pylint: disable=too-few-public-methods
    locators_sequence = [
        (By.XPATH, """//button[contains(@aria-label, "Share dashboard or panel")]"""),
        (By.XPATH, """//ul/li/a[contains(@aria-label, "Tab Snapshot") and contains(text(), "Snapshot")]"""),
        (By.XPATH, """//button//span[contains(text(), "Publish to snapshot.raintank.io")]"""),
        (By.XPATH, """//a[contains(@href, "https://snapshot.raintank.io")]""")
    ]

    def get_shared_link(self, remote_browser):
        """Get link from page to remote snapshot on https://snapshot.raintank.io

        using selenium remote web driver remote_browser find sequentially web_element by locators
        in self.snapshot_locators_sequence run actiom WebElement.click() and
        get value from result link found by latest element in snapshot_locators_sequence

        :param remote_browser: remote webdirver instance
        :type remote_browser: selenium.webdriver.Remote
        :returns: return value of link to remote snapshot on https://snapshot.raintank.io
        :rtype: {str}
        """
        for element in self.locators_sequence[:-1]:
            LOGGER.debug("Search element '%s'", element)
            WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(EC.visibility_of_element_located(element))
            found_element = remote_browser.find_element(*element)
            found_element.click()
        snapshot_link_locator = self.locators_sequence[-1]
        WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(snapshot_link_locator))
        snapshot_link_element = remote_browser.find_element(*snapshot_link_locator)

        LOGGER.debug(snapshot_link_element.text)
        return snapshot_link_element.text


class Dashboard:
    name: str
    path: str
    resolution: str
    scroll_ready_locator: Tuple[By, str] = (By.XPATH, "//div[@class='scrollbar-view']")
    panels: List[Panel]
    scroll_step: int = 1000
    title: str

    def scroll_to_bottom(self, remote_browser):
        WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(self.scroll_ready_locator))
        scroll_element = remote_browser.find_element(*self.scroll_ready_locator)
        scroll_height = remote_browser.execute_script("return arguments[0].scrollHeight", scroll_element)

        for scroll_size in range(0, scroll_height, self.scroll_step):
            LOGGER.debug("Scrolling next %s", self.scroll_step)
            remote_browser.execute_script(f'arguments[0].scrollTop = {scroll_size}', scroll_element)

    def wait_panels_loading(self, remote_browser):
        LOGGER.info("Waiting panels load data")
        for panel in self.panels:
            panel.wait_loading(remote_browser)
        LOGGER.info("All panels have loaded data")

    @staticmethod
    def get_snapshot(remote_browser):
        return Snapshot().get_shared_link(remote_browser)


class OverviewDashboard(Dashboard):
    name = 'overview'
    path = 'd/overview-{version}/scylla-{dashboard_name}'
    title = 'Overview'
    resolution = '1920px*4000px'
    panels = [Panel("Disk Size by DC"),
              Panel("Running Compactions"),
              Panel("Writes"),
              Panel("Write Latencies"),
              Panel("Read Timeouts by DC"),
              Panel("Write Timeouts by DC")]


class ServerMetricsNemesisDashboard(Dashboard):
    if test_name := get_test_name():
        test_name = f"{test_name.lower()}-"

    name = f'{test_name}scylla-per-server-metrics-nemesis'
    title = 'Scylla Per Server Metrics Nemesis'
    path = 'dashboard/db/{dashboard_name}-{version}'
    resolution = '1920px*15000px'
    panels = [Panel("Total Requests"),
              Panel("Load per Instance"),
              Panel("Requests Served per Instance"),
              Panel("Reads with no misses"),
              Panel("Total Bytes"),
              Panel("Running Compactions"),
              Panel("Gemini metrics")
              ]


class AlternatorDashboard(Dashboard):
    name = 'alternator'
    title = 'Alternator'
    path = 'd/alternator-{version}/{dashboard_name}'
    resolution = '1920px*4000px'
    panels = [Panel("Total Actions"),
              Panel("Scan by Instance"),
              Panel("Completed GetItem"),
              Panel("95th percentile DeleteItem latency by Instance"),
              Panel("Cache Hits"),
              Panel("Your Graph here")
              ]
