from copy import deepcopy
import logging
from typing import TypedDict
import yaml
from sdcm.cluster import BaseNode
from sdcm.remote.remote_file import remote_file
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent

LOGGER = logging.getLogger(__name__)


class IOTuneDiskParams(TypedDict):
    mountpoint: str
    read_iops: int
    read_bandwidth: int
    write_iops: int
    write_bandwidth: int


class IOProperties(TypedDict):
    disks: list[IOTuneDiskParams]


class IOTuneException(Exception):
    pass


class IOTuneValidator:
    def __init__(self, node: BaseNode):
        self.node = node
        self.node_io_properties: IOProperties = {}
        self.preset_io_properties: IOProperties = {}
        self.results = {
            "mountpoint": None,
            "deviation": {},
            "deviation_pct": {},
            "active": {},
            "preset": {},
            "limits":  {},
        }

    def validate(self):
        self._run_io_tune()
        self._prepare_results_table()
        self._format_results_to_console()

    def _read_io_properties(self, io_props_path="/etc/scylla.d/io_properties.yaml") -> IOProperties:
        with remote_file(self.node.remoter, io_props_path, sudo=True) as f:
            return yaml.safe_load(f)

    def _run_io_tune(self, temp_props_path="/tmp/io_properties.yaml") -> IOProperties:
        self.node.remoter.sudo(f"iotune  --evaluation-directory /var/lib/scylla --properties-file {temp_props_path}")
        self.node_io_properties = self._read_io_properties(temp_props_path)
        self.preset_io_properties = self._read_io_properties()

        preset_disk = next(iter(self.preset_io_properties.get("disks", [])), None)
        if not preset_disk:
            LOGGER.error("Unable to continue - node should have io_properties.yaml, but it doesn't.")
            TestFrameworkEvent(source="send_iotune_results_to_argus",
                               message="Unable to continue - node should have io_properties.yaml, but it doesn't.",
                               severity=Severity.ERROR).publish()
            raise IOTuneException("Unable to continue - node should have io_properties.yaml, but it doesn't.")

        return self.node_io_properties

    @staticmethod
    def _bottom_limit(metric_val, threshold=0.15):
        """
            Determine disk metric deviation threshold and
            use that as fixed limit
        """
        return metric_val * threshold

    def _prepare_results_table(self):
        preset_disk = next(iter(self.preset_io_properties.get("disks", [])), None)
        tested_disk = deepcopy(next(iter(self.node_io_properties.get("disks", []))))
        tested_mountpoint = tested_disk.pop("mountpoint")
        if tested_mountpoint != preset_disk["mountpoint"]:
            LOGGER.error("Disks differ - probably a mistake: %s vs %s",
                         tested_mountpoint, preset_disk["mountpoint"])
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Disks differ - probably a mistake: {tested_mountpoint} vs {preset_disk['mountpoint']}",
                               severity=Severity.ERROR).publish()
            raise IOTuneException(
                f"Disks differ - probably a mistake: {tested_mountpoint} vs {preset_disk['mountpoint']}")

        self.results["mountpoint"] = tested_mountpoint
        for key, value in tested_disk.items():
            preset_val = preset_disk.get(key)
            deviation_val = abs(preset_val - value)
            self.results["deviation"][key] = deviation_val
            self.results["deviation_pct"][key] = (deviation_val / preset_val) * 100
            self.results["active"][key] = value
            self.results["preset"][key] = preset_disk.get(key)
            self.results["limits"][key] = self._bottom_limit(preset_disk[key])

    def _format_results_to_console(self):
        LOGGER.info("Disk performance values validation - testing %s", self.results["mountpoint"])
        for key, val in self.results["active"].items():
            LOGGER.info("[%s] %s: %s (%.0f%%)", self.results["mountpoint"],
                        key, val, self.results["deviation_pct"][key])
