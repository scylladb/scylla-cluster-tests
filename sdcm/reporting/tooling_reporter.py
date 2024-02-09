import logging

from cassandra import __version__ as PYTHON_DRIVER_VERSION
from argus.client.sct.client import ArgusSCTClient
from argus.client.sct.types import Package

from sdcm.remote.base import CommandRunner

LOGGER = logging.getLogger(__name__)


class ToolReporterBase():

    TOOL_NAME = None

    def __init__(self, runner: CommandRunner, command_prefix: str = None, argus_client: ArgusSCTClient = None) -> None:
        self.runner = runner
        self.command_prefix = command_prefix
        self.argus_client = argus_client
        self.version: str | None = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    def _report_to_log(self) -> None:
        LOGGER.info("%s: %s version is %s", self, self.TOOL_NAME, self.version)

    def _report_to_argus(self) -> None:
        if not self.argus_client:
            LOGGER.warning("%s: Skipping reporting to argus, client not initialized.", self)
            return

        package = Package(name=f"{self.TOOL_NAME}", version=self.version,
                          date=None, revision_id=None, build_id=None)
        self.argus_client.submit_packages([package])

    def _collect_version_info(self) -> None:
        raise NotImplementedError()

    def report(self) -> None:
        self._collect_version_info()
        if not self.version:
            LOGGER.warning("%s: Version not collected, skipping report...", self)
            return
        self._report_to_log()
        self._report_to_argus()


class PythonDriverReporter(ToolReporterBase):
    # pylint: disable=too-few-public-methods
    """
        Reports python-driver version used for SCT operations.
    """

    TOOL_NAME = "scylla-cluster-tests/python-driver"

    def __init__(self, argus_client: ArgusSCTClient = None) -> None:
        super().__init__(None, "", argus_client)

    def _collect_version_info(self):
        self.version = PYTHON_DRIVER_VERSION


class CassandraStressVersionReporter(ToolReporterBase):
    # pylint: disable=too-few-public-methods
    TOOL_NAME = "cassandra-stress"

    def _collect_version_info(self) -> None:
        output = self.runner.run(f"{self.command_prefix} {self.TOOL_NAME} version")
        LOGGER.info("%s: Collected cassandra-stress output:\n%s", self, output.stdout)
        field_map = {
            "Version": "cassandra-stress",
            "scylla-java-driver": "scylla-java-driver",
        }
        result = {}
        for line in output.stdout.splitlines():
            try:
                key, value = line.split(":", 2)
                if not (field_name := field_map.get(key)):
                    continue
                result[field_name] = value.strip()
            except ValueError:
                continue
        LOGGER.info("Result:\n%s", result)
        self.version = f"{result.get('cassandra-stress', '#FAILED_CHECK_LOGS')}"
        if driver_version := result.get("scylla-java-driver"):
            self.version += f" (with java-driver {driver_version})"
