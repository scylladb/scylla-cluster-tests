import logging

from typing import Tuple, List

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.common import exceptions

from sdcm.utils.common import get_test_name


LOGGER = logging.getLogger(__name__)
UI_ELEMENT_LOAD_TIMEOUT = 180


class Panel:
    xpath_tmpl = """//div[contains(@aria-label,'{name}') and contains(@class, 'panel-title-container')]"""

    def __init__(self, name):
        self.name = name

    @property
    def xpath_locator(self):
        return (By.XPATH, self.xpath_tmpl.format(name=self.name))

    def wait_loading(self, remote_browser):
        LOGGER.info(f"Waiting panel {self.name} loading")
        WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(self.xpath_locator))
        el: WebElement = remote_browser.find_element(*self.xpath_locator)
        parent_el: WebElement = el.find_element_by_xpath("parent::*//parent::*")
        remote_browser.execute_script("arguments[0].style.border='10px solid red'", parent_el)
        try:
            loading = parent_el.find_element_by_xpath("div[contains(@class, 'panel-loading')]")
            WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(EC.invisibility_of_element(loading))
        except exceptions.NoSuchElementException:
            LOGGER.debug(f"Check panel is loaded")
        list_of_childs = parent_el.find_elements_by_xpath("child::*")
        assert len(list_of_childs) == 2, f"num of elements {len(list_of_childs)}"
        LOGGER.info(f"Panel {self.name} loaded")


class Snapshot:
    locators_sequence = [
        (By.XPATH, """//div[contains(@class, "navbar-buttons--actions")]"""),
        (By.XPATH, """//ul/li[contains(text(), "Snapshot")]"""),
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
    scroll_ready_locator: Tuple[By, str]
    panels: List[Panel]
    scroll_step: int = 1000

    def scroll_to_bottom(self, remote_browser):
        WebDriverWait(remote_browser, UI_ELEMENT_LOAD_TIMEOUT).until(
            EC.visibility_of_element_located(self.scroll_ready_locator))
        scroll_element = remote_browser.find_element(*self.scroll_ready_locator)
        scroll_height = remote_browser.execute_script("return arguments[0].scrollHeight", scroll_element)

        for scroll_size in range(0, scroll_height, self.scroll_step):
            LOGGER.debug(f"Scrolling next {self.scroll_step}")
            remote_browser.execute_script(f'arguments[0].scrollTop = {scroll_size}', scroll_element)

    def wait_panels_loading(self, remote_browser):
        LOGGER.info("Waiting panels load data")
        for panel in self.panels:
            panel.wait_loading(remote_browser)
        LOGGER.info("All panels have loaded data")

    def get_snapshot(self, remote_browser):
        return Snapshot().get_shared_link(remote_browser)


class OverviewDashboard(Dashboard):
    name = 'overview'
    path = 'd/overview-{version}/scylla-{dashboard_name}'
    resolution = '1920px*4000px'
    scroll_ready_locator = (By.XPATH, "//div[@class='view']")
    panels = [Panel("Writes"),
              Panel("Write Latencies"),
              Panel("Read/Write Timeouts by DC")]


class ServerMetricsNemesisDashboard(Dashboard):
    if test_name := get_test_name():
        test_name = f"{test_name.lower()}-"

    name = f'{test_name}scylla-per-server-metrics-nemesis'
    path = 'dashboard/db/{dashboard_name}-{version}'
    resolution = '1920px*7000px'
    scroll_ready_locator = (By.XPATH, "//div[@class='view']")
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
    path = 'd/alternator-{version}/{dashboard_name}'
    resolution = '1920px*4000px'
    scroll_ready_locator = (By.XPATH, "//div[@class='view']")
    panels = [Panel("Total Actions"),
              Panel("Scan by Instance"),
              Panel("Completed GetItem"),
              Panel("95th percentile DeleteItem latency by Instance"),
              Panel("Cache Hits"),
              Panel("Your Graph here")
              ]
