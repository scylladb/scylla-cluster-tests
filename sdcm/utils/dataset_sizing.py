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
# Copyright (c) 2026 ScyllaDB

"""Size the gradual test's dataset from the DB nodes' memory and disk.

The stress commands of the test-case define the dataset for a reference node (128 GiB RAM, 3750 GB data disk:
i4i.4xlarge and i8g.4xlarge). On another node, their row counts and key ranges are scaled by the node's RAM and
disk relative to the reference, so the read workloads keep reading from cache or from disk as their names say.
The scale is rounded to a step, so nodes close to the reference keep the test-case's dataset exactly.
"""

import logging
import math
import re
from dataclasses import dataclass
from fractions import Fraction
from typing import Self

LOGGER = logging.getLogger(__name__)

GIB = 1024**3
GB = 1000**3

# Which resource each stress command's dataset follows:
# - the hot range read from cache follows RAM;
# - the preloaded dataset and the disk reads over it follow RAM too, so reads miss the cache, but never outgrow
#   the disk (the smaller of the two scales);
# - the write dataset follows the disk.
SCALE_BY_PARAM = {
    "stress_cmd_r": "ram",
    "stress_cmd_m": "ram",
    "stress_cmd_cache_warmup": "ram",
    "prepare_write_cmd": "preload",
    "stress_cmd_read_disk": "preload",
    "stress_cmd_w": "disk",
}

SEQ_RANGE = re.compile(r"\bseq=(\d+)\.\.(\d+)")
# 'n=<rows>' only: not 'n=FIXED(1)' of a column spec, nor the end of another option such as 'conn=8'.
ROW_COUNT = re.compile(r"(?<![\w-])n=(\d+)(?![\w(])")


@dataclass(frozen=True)
class DatasetSizingConfig:
    reference_ram_gib: float = 128
    reference_disk_gb: float = 3750
    scale_step: float = 0.25

    @classmethod
    def from_params(cls, sizing_params: dict | None) -> Self | None:
        """Build the config from `perf_dataset_sizing`; None keeps the test-case's dataset as is."""
        if sizing_params is None:
            return None
        return cls(**sizing_params)


@dataclass(frozen=True)
class DatasetScale:
    ram: Fraction
    disk: Fraction
    preload: Fraction

    def for_param(self, param: str) -> Fraction:
        return getattr(self, SCALE_BY_PARAM[param])


def _round_to_step(value: float, step: float) -> Fraction:
    """Round to the nearest multiple of `step`, at least `step` itself: even a tiny node runs some dataset."""
    step = Fraction(str(step))
    return max(step, round(Fraction(value) / step) * step)


def dataset_scale(ram_bytes: int, disk_bytes: int, config: DatasetSizingConfig) -> DatasetScale:
    """Scale of the node's RAM and disk relative to the reference node, each rounded to `scale_step`."""
    ram = _round_to_step(ram_bytes / (config.reference_ram_gib * GIB), config.scale_step)
    disk = _round_to_step(disk_bytes / (config.reference_disk_gb * GB), config.scale_step)
    return DatasetScale(ram=ram, disk=disk, preload=min(ram, disk))


def scale_stress_command(stress_cmd: str, scale: Fraction) -> str:
    """Scale a stress command's key range and row count.

    A range `seq=a..b` becomes `seq=floor((a-1)*scale)+1..floor(b*scale)`, so ranges that were contiguous across
    commands stay contiguous. A row count `n=` that goes with a range becomes the scaled range's length; without
    a range it is scaled on its own. Commands with neither, e.g. time-bound latte commands, are returned as is.
    """
    if scale == 1:
        return stress_cmd
    scaled_range = None
    if seq_range := SEQ_RANGE.search(stress_cmd):
        start, end = int(seq_range.group(1)), int(seq_range.group(2))
        scaled_start, scaled_end = math.floor((start - 1) * scale) + 1, max(1, math.floor(end * scale))
        scaled_end = max(scaled_end, scaled_start)
        scaled_range = scaled_end - scaled_start + 1
        stress_cmd = SEQ_RANGE.sub(f"seq={scaled_start}..{scaled_end}", stress_cmd, count=1)
    if row_count := ROW_COUNT.search(stress_cmd):
        rows = scaled_range if scaled_range is not None else max(1, math.floor(int(row_count.group(1)) * scale))
        stress_cmd = ROW_COUNT.sub(f"n={rows}", stress_cmd, count=1)
    return stress_cmd


def scale_dataset_params(params: dict, scale: DatasetScale) -> dict[str, list[str] | str]:
    """Return the scaled stress commands of every sized parameter that `params` sets."""
    scaled = {}
    for param in SCALE_BY_PARAM:
        if not (commands := params.get(param)):
            continue
        param_scale = scale.for_param(param)
        if isinstance(commands, str):
            scaled[param] = scale_stress_command(commands, param_scale)
        else:
            scaled[param] = [scale_stress_command(cmd, param_scale) for cmd in commands]
    return scaled
