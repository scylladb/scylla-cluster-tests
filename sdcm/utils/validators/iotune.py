import logging
from typing import TypedDict
import yaml
from argus.client.generic_result import ColumnMetadata, GenericResultTable, ResultType, Status, ValidationRule
from argus.client.sct.client import ArgusSCTClient
from sdcm.argus_results import submit_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.remote.remote_file import remote_file
from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


class IOTuneDiskParams(TypedDict):
    mountpoint: str
    read_iops: int
    read_bandwidth: int
    write_iops: int
    write_bandwidth: int


class IOProperties(TypedDict):
    disks: list[IOTuneDiskParams]


class IOTuneValidator:
    def __init__(self, node: BaseNode, test_params: SCTConfiguration, argus_client: ArgusSCTClient = None):
        self.node = node
        self.params = test_params
        self.node_io_properties = {}
        self.argus_client = argus_client

    def validate(self):
        self._run_io_tune()
        self._format_results_to_console()
        self._submit_results_to_argus()

    def _read_io_properties(self, io_props_path="/etc/scylla.d/io_properties.yaml") -> IOProperties:
        with remote_file(self.node.remoter, io_props_path) as f:
            return yaml.safe_load(f)

    def _run_io_tune(self, temp_props_path="/tmp/io_properties.yaml") -> IOProperties:
        self.node.remoter.sudo(f"iotune  --evaluation-directory /var/lib/scylla --properties-file {temp_props_path}")
        self.node_io_properties = self._read_io_properties(temp_props_path)

        return self.node_io_properties

    def _format_results_to_console(self):
        preset_properties = self._read_io_properties()
        preset_disk = next(iter(preset_properties.get("disks")))

        for disk in self.node_io_properties.get("disks"):
            disk_copy = {**disk}
            mountpoint = disk_copy.pop("mountpoint")
            LOGGER.info("Disk performance values validation - testing %s", mountpoint)
            for key, val in disk_copy.items():
                preset_val = preset_disk.get(key)
                diff = (val / preset_val - 1) * 100
                LOGGER.info("[%s] %s: %s (%.0f)", mountpoint, key, val, diff)

    @staticmethod
    def _bottom_limit(metric_val, threshold=0.15):
        """
            Determine disk metric deviation threshold and
            use that as fixed limit
        """
        return metric_val * threshold

    def _submit_results_to_argus(self):

        if not self.argus_client:
            LOGGER.warning("Will not submit to argus - no client initialized")
            return
        preset_io_props = self._read_io_properties()
        preset_disk: IOTuneDiskParams = next(iter(preset_io_props.get("disks", [])), None)
        if not preset_disk:
            LOGGER.warning("Unable to continue - node should have io_properties.yaml, but it doesn't.")
            return

        class IOPropertiesResultsTable(GenericResultTable):
            class Meta:
                name = f"{self.params.get('cluster_backend')} - {self.node.db_node_instance_type} Disk Performance"
                description = "io_properties.yaml comparison with live data"
                Columns = [
                    ColumnMetadata(name="read_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
                    ColumnMetadata(name="read_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
                    ColumnMetadata(name="write_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
                    ColumnMetadata(name="write_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
                ]

                ValidationRules = {

                }

        class IOPropertiesDeviationResultsTable(GenericResultTable):
            class Meta:
                name = f"{self.params.get('cluster_backend')} - {self.node.db_node_instance_type} Disk Performance Absolute deviation"
                description = "io_properties.yaml absolute deviation from preset disk"
                Columns = [
                    ColumnMetadata(name="read_iops_abs_deviation", unit="iops",
                                   type=ResultType.INTEGER, higher_is_better=False),
                    ColumnMetadata(name="read_bandwidth_abs_deviation", unit="bps",
                                   type=ResultType.INTEGER, higher_is_better=False),
                    ColumnMetadata(name="write_iops_abs_deviation", unit="iops",
                                   type=ResultType.INTEGER, higher_is_better=False),
                    ColumnMetadata(name="write_bandwidth_abs_deviation", unit="bps",
                                   type=ResultType.INTEGER, higher_is_better=False),
                ]

                ValidationRules = {
                    "read_iops_abs_deviation": ValidationRule(fixed_limit=self._bottom_limit(preset_disk.get("read_iops"))),
                    "read_bandwidth_abs_deviation": ValidationRule(fixed_limit=self._bottom_limit(preset_disk.get("read_bandwidth"))),
                    "write_iops_abs_deviation": ValidationRule(fixed_limit=self._bottom_limit(preset_disk.get("write_iops"))),
                    "write_bandwidth_abs_deviation": ValidationRule(fixed_limit=self._bottom_limit(preset_disk.get("write_bandwidth"))),
                }

        tested_disk: IOTuneDiskParams = next(iter(self.node_io_properties.get("disks", [])))
        tested_mountpoint = tested_disk.pop("mountpoint")
        if tested_mountpoint != preset_disk["mountpoint"]:
            LOGGER.warning("Disks differ - probably a mistake: %s vs %s, will not submit iotune results",
                           tested_mountpoint, preset_disk["mountpoint"])
            return
        table = IOPropertiesResultsTable()
        for key, value in tested_disk.items():
            table.add_result(column=key, row="#1", value=value, status=Status.PASS)
        submit_results_to_argus(self.argus_client, table)

        table = IOPropertiesDeviationResultsTable()
        for key, value in tested_disk.items():
            deviation_val = abs(preset_disk.get(key) - value)
            table.add_result(column=f"{key}_abs_deviation", row="#1", value=deviation_val, status=Status.UNSET)

        submit_results_to_argus(self.argus_client, table)
