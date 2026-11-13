from copy import deepcopy
import logging
from typing import TypedDict
import yaml
from sdcm.cluster import BaseNode
from sdcm.remote.remote_file import remote_file
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent

LOGGER = logging.getLogger(__name__)

# Deviation is the absolute percent difference between the installed io_properties.yaml value and the
# measured value: |installed - measured| / installed * 100.
#
# An installed value above the measured value lets the I/O scheduler send more I/O than the disk can serve.
# An installed value below the measured value keeps Scylla from using the full disk performance.
# Argus fails the run when the deviation is at or above the limit of the backend. The limits come from the Argus
# "Disk Performance" tables of release image artifacts runs, 2025-07 to 2026-10 (SCT-1274). The "healthy max"
# columns do not include the instance types with a known preset defect.
#
#   backend  limit  healthy max,              healthy max,             smallest defect
#                   installed above           installed below
#   aws      25%    25% (i4i.4xlarge),        24% (i4g)                31% above (i4i.8xlarge),
#                   38% on some i3en hosts                             117% below (is4gen.2xlarge)
#   gce      20%    15.4%                     18.9% (n2-highmem-128)   25% above (fixed values on small types),
#                                                                      52% below (2025.1)
#   oci      30%    23.5%                     18.4%                    none seen
#   azure    35%    35% (Lsv3), 12% (v4)      32% (Lsv3), 11% (v4)     98% above (Lsv3 low-IOPS hosts),
#                                                                      360% below (2026.1 L8aos_v4)
IO_DEVIATION_LIMIT_PCT = {"aws": 25, "gce": 20, "oci": 30, "azure": 35}


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


def io_deviation_pct(installed: int, measured: int) -> float:
    return abs(installed - measured) / installed * 100


class IOTuneValidator:
    def __init__(self, node: BaseNode, backend: str):
        self.node = node
        self.node_io_properties: IOProperties = {}
        self.preset_io_properties: IOProperties = {}
        self.results = {
            "mountpoint": None,
            "limit_pct": IO_DEVIATION_LIMIT_PCT[backend],
            "deviation_pct": {},
            "active": {},
            "preset": {},
        }

    def validate(self):
        self._run_io_tune()
        self._prepare_results_table()
        self._format_results_to_console()

    def _read_io_properties(self, io_props_path="/etc/scylla.d/io_properties.yaml") -> IOProperties:
        with remote_file(self.node.remoter, io_props_path) as f:
            return yaml.safe_load(f)

    def _run_io_tune(self) -> IOProperties:
        self.preset_io_properties = self._read_io_properties()
        self.node.remoter.sudo("scylla_io_setup", timeout=600)
        self.node_io_properties = self._read_io_properties()

        preset_disk = next(iter(self.preset_io_properties.get("disks", [])), None)
        if not preset_disk:
            LOGGER.error("Unable to continue - node should have io_properties.yaml, but it doesn't.")
            TestFrameworkEvent(
                source="send_iotune_results_to_argus",
                message="Unable to continue - node should have io_properties.yaml, but it doesn't.",
                severity=Severity.ERROR,
            ).publish()
            raise IOTuneException("Unable to continue - node should have io_properties.yaml, but it doesn't.")

        return self.node_io_properties

    def _prepare_results_table(self):
        preset_disk = next(iter(self.preset_io_properties.get("disks", [])), None)
        tested_disk = deepcopy(next(iter(self.node_io_properties.get("disks", []))))
        tested_mountpoint = tested_disk.pop("mountpoint")
        if tested_mountpoint != preset_disk["mountpoint"]:
            LOGGER.error("Disks differ - probably a mistake: %s vs %s", tested_mountpoint, preset_disk["mountpoint"])
            TestFrameworkEvent(
                source=self.__class__.__name__,
                message=f"Disks differ - probably a mistake: {tested_mountpoint} vs {preset_disk['mountpoint']}",
                severity=Severity.ERROR,
            ).publish()
            raise IOTuneException(
                f"Disks differ - probably a mistake: {tested_mountpoint} vs {preset_disk['mountpoint']}"
            )

        self.results["mountpoint"] = tested_mountpoint
        for key, value in tested_disk.items():
            self.results["deviation_pct"][key] = io_deviation_pct(preset_disk[key], value)
            self.results["active"][key] = value
            self.results["preset"][key] = preset_disk[key]

    def _format_results_to_console(self):
        LOGGER.info("Disk performance values validation - testing %s", self.results["mountpoint"])
        for key, val in self.results["active"].items():
            LOGGER.info(
                "[%s] %s: %s (%.0f%%)", self.results["mountpoint"], key, val, self.results["deviation_pct"][key]
            )
