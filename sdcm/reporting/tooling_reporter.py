import json
import logging
from functools import lru_cache

from cassandra import __version__ as PYTHON_DRIVER_VERSION
from argus.client.sct.client import ArgusSCTClient
from argus.client.sct.types import Package

from sdcm.remote.base import CommandRunner

LOGGER = logging.getLogger(__name__)


@lru_cache
def report_package_to_argus(client: ArgusSCTClient, tool_name: str, package_version: str, additional_data: str = None,
                            date: str = None, revision_id: str = None) -> None:
    package = Package(name=f"{tool_name}", version=package_version,
                      date=date, revision_id=revision_id, build_id=additional_data)
    client.submit_packages([package])


class ToolReporterBase():
    TOOL_NAME = None

    def __init__(self, runner: CommandRunner, command_prefix: str = None, argus_client: ArgusSCTClient = None) -> None:
        self.runner = runner
        self.command_prefix = command_prefix
        self.argus_client = argus_client
        self.additional_data = None
        self.version: str | None = None
        self.date: str | None = None
        self.revision_id: str | None = None

    def __str__(self) -> str:
        return f"{self.__class__.__name__}()"

    def _report_to_log(self) -> None:
        LOGGER.info("%s: %s version is %s", self, self.TOOL_NAME, self.version)

    def _report_to_argus(self) -> None:
        if not self.argus_client:
            LOGGER.warning("%s: Skipping reporting to argus, client not initialized.", self)
            return
        try:
            report_package_to_argus(client=self.argus_client, tool_name=self.TOOL_NAME, package_version=self.version,
                                    additional_data=self.additional_data, date=self.date, revision_id=self.revision_id)
        except Exception:  # pylint: disable=broad-except # noqa: BLE001
            LOGGER.warning("Failed reporting tool version to Argus", exc_info=True)

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
            self.additional_data = f"java-driver: {driver_version}"
            CassandraStressJavaDriverVersionReporter(
                driver_version=driver_version, command_prefix=None, runner=None, argus_client=self.argus_client).report()


class CqlStressCassandraStressVersionReporter(ToolReporterBase):
    # pylint: disable=too-few-public-methods
    TOOL_NAME = "cql-stress-cassandra-stress"

    def _collect_version_info(self) -> None:
        output = self.runner.run(f"{self.command_prefix} {self.TOOL_NAME} version_json")
        LOGGER.debug("%s: Collected cql-stress-cassandra-stress output:\n%s", self, output.stdout)
        result = json.loads(output.stdout)

        LOGGER.debug("Result:\n%s", result)
        cql_stress_details = result.get('cql-stress', {})
        self.version = f"{cql_stress_details.get('version', '#FAILED_CHECK_LOGS')}"
        self.date = cql_stress_details.get('commit_date')
        self.revision_id = cql_stress_details.get('commit_sha')

        if driver_details := result.get("scylla-driver", {}):
            driver_version = driver_details.get('version')
            driver_date = driver_details.get("commit_date")
            driver_revision_id = driver_details.get("commit_sha")
            CqlStressRustDriverVersionReporter(
                driver_version=driver_version, date=driver_date, revision_id=driver_revision_id, command_prefix=None, runner=None,
                argus_client=self.argus_client).report()


class CassandraStressJavaDriverVersionReporter(ToolReporterBase):
    # pylint: disable=too-few-public-methods
    TOOL_NAME = "java-driver"

    def __init__(self, driver_version: str, runner: CommandRunner, command_prefix: str = None, argus_client: ArgusSCTClient = None) -> None:
        super().__init__(runner, command_prefix, argus_client)
        self.version = driver_version

    def _collect_version_info(self) -> None:
        pass


class CqlStressRustDriverVersionReporter(ToolReporterBase):
    # pylint: disable=too-few-public-methods
    TOOL_NAME = "cql-stress-rust-driver"

    def __init__(self, driver_version: str, date: str, revision_id: str, runner: CommandRunner, command_prefix: str = None,
                 argus_client: ArgusSCTClient = None) -> None:
        super().__init__(runner, command_prefix, argus_client)
        self.version = driver_version
        self.date = date
        self.revision_id = revision_id

    def _collect_version_info(self) -> None:
        pass
