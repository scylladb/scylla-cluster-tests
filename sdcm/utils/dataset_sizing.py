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

The test-case's commands also split their key range for 4 loaders, one command each. On another number of loaders
the same range can be split into one command per loader, so every loader generates load.
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

# The preload writes at a fixed rate; its rate and threads scale with its rows, so it takes as long as on the
# reference node and fits its prepare_stress_duration and the test's setup allowance on any node size.
RATE_SCALED_PARAMS = ("prepare_write_cmd",)

SEQ_RANGE = re.compile(r"\bseq=(\d+)\.\.(\d+)")
# 'n=<rows>' only: not 'n=FIXED(1)' of a column spec, nor the end of another option such as 'conn=8'.
ROW_COUNT = re.compile(r"(?<![\w-])n=(\d+)(?![\w(])")
# Literal '-rate' values only: not the '$threads' and '$throttle' placeholders of the measured steps.
THROTTLE = re.compile(r"(?<![\w$])throttle=(\d+)/s")
THREADS = re.compile(r"(?<![\w$])threads=(\d+)\b")

# latte: the cycle range of a command, the rune script's key space (row_count) and its start (offset), and a
# --duration given as a cycle count rather than a time. Literal values only, not the '$...' placeholders.
LATTE_START = re.compile(r"--start-cycle[ =](\d+)\b")
LATTE_END = re.compile(r"--end-cycle[ =](\d+)\b")
LATTE_ROW_COUNT = re.compile(r"-P row_count=(\d+)\b")
LATTE_OFFSET = re.compile(r"-P offset=(\d+)\b")
LATTE_CYCLES = re.compile(r"--duration[ =](\d+)(?![\w.])")
LATTE_RATE = re.compile(r"--rate[ =](\d+)\b")
LATTE_THREADS = re.compile(r"--threads[ =](\d+)\b")


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


def _scaled_range(start: int, end: int, scale: Fraction) -> tuple[int, int]:
    """Scale a range so that ranges contiguous across commands stay contiguous."""
    scaled_start, scaled_end = math.floor((start - 1) * scale) + 1, max(1, math.floor(end * scale))
    return scaled_start, max(scaled_end, scaled_start)


def _sub_value(pattern: re.Pattern, value: int, stress_cmd: str) -> str:
    """Replace the number of the pattern's first match, keeping the rest of the match as is."""
    return pattern.sub(lambda match: match.group(0).replace(match.group(1), str(value)), stress_cmd, count=1)


def scale_latte_command(stress_cmd: str, scale: Fraction) -> str:
    """Scale a latte command's cycle range, its row_count and offset, and a --duration given in cycles.

    The cycle range scales like a cassandra-stress range. row_count and offset scale by the same factor, so the
    keys the rune script derives from them keep their place in the scaled dataset. A cycle-count duration that
    covers the whole range follows the scaled range; another one scales on its own.
    """
    start, end = LATTE_START.search(stress_cmd), LATTE_END.search(stress_cmd)
    if start and end:
        old_length = int(end.group(1)) - int(start.group(1)) + 1
        scaled_start, scaled_end = _scaled_range(int(start.group(1)), int(end.group(1)), scale)
        stress_cmd = _sub_value(LATTE_END, scaled_end, _sub_value(LATTE_START, scaled_start, stress_cmd))
        if cycles := LATTE_CYCLES.search(stress_cmd):
            new_cycles = (
                scaled_end - scaled_start + 1
                if int(cycles.group(1)) == old_length
                else max(1, math.floor(int(cycles.group(1)) * scale))
            )
            stress_cmd = _sub_value(LATTE_CYCLES, new_cycles, stress_cmd)
    if row_count := LATTE_ROW_COUNT.search(stress_cmd):
        stress_cmd = _sub_value(LATTE_ROW_COUNT, max(1, math.floor(int(row_count.group(1)) * scale)), stress_cmd)
    if offset := LATTE_OFFSET.search(stress_cmd):
        stress_cmd = _sub_value(LATTE_OFFSET, math.floor(int(offset.group(1)) * scale), stress_cmd)
    return stress_cmd


def is_latte(stress_cmd: str) -> bool:
    return stress_cmd.lstrip().startswith("latte ")


def scale_stress_command(stress_cmd: str, scale: Fraction) -> str:
    """Scale a stress command's key range and row count.

    A range `seq=a..b` becomes `seq=floor((a-1)*scale)+1..floor(b*scale)`, so ranges that were contiguous across
    commands stay contiguous. A row count `n=` that goes with a range becomes the scaled range's length; without
    a range it is scaled on its own. Commands with neither, e.g. time-bound latte commands, are returned as is.
    """
    if scale == 1:
        return stress_cmd
    if is_latte(stress_cmd):
        return scale_latte_command(stress_cmd, scale)
    scaled_range = None
    if seq_range := SEQ_RANGE.search(stress_cmd):
        scaled_start, scaled_end = _scaled_range(int(seq_range.group(1)), int(seq_range.group(2)), scale)
        scaled_range = scaled_end - scaled_start + 1
        stress_cmd = SEQ_RANGE.sub(f"seq={scaled_start}..{scaled_end}", stress_cmd, count=1)
    if row_count := ROW_COUNT.search(stress_cmd):
        rows = scaled_range if scaled_range is not None else max(1, math.floor(int(row_count.group(1)) * scale))
        stress_cmd = ROW_COUNT.sub(f"n={rows}", stress_cmd, count=1)
    return stress_cmd


def scale_stress_rate(stress_cmd: str, scale: Fraction) -> str:
    """Scale a stress command's literal throttle and thread count, so it writes its scaled rows in the same time."""
    if scale == 1:
        return stress_cmd
    if is_latte(stress_cmd):
        stress_cmd = LATTE_RATE.sub(
            lambda rate: rate.group(0).replace(rate.group(1), str(max(1, round(int(rate.group(1)) * scale)))),
            stress_cmd,
        )
        return LATTE_THREADS.sub(
            lambda threads: threads.group(0).replace(
                threads.group(1), str(max(1, math.ceil(int(threads.group(1)) * scale)))
            ),
            stress_cmd,
        )
    stress_cmd = THROTTLE.sub(lambda rate: f"throttle={max(1, round(int(rate.group(1)) * scale))}/s", stress_cmd)
    return THREADS.sub(lambda threads: f"threads={max(1, math.ceil(int(threads.group(1)) * scale))}", stress_cmd)


def scale_dataset_params(params: dict, scale: DatasetScale) -> dict[str, list[str] | str]:
    """Return the scaled stress commands of every sized parameter that `params` sets."""
    scaled = {}
    for param in SCALE_BY_PARAM:
        if not (commands := params.get(param)):
            continue
        param_scale = scale.for_param(param)

        def scale_command(stress_cmd: str, param: str = param, param_scale: Fraction = param_scale) -> str:
            stress_cmd = scale_stress_command(stress_cmd, param_scale)
            return scale_stress_rate(stress_cmd, param_scale) if param in RATE_SCALED_PARAMS else stress_cmd

        if isinstance(commands, str):
            scaled[param] = scale_command(commands)
        else:
            scaled[param] = [scale_command(cmd) for cmd in commands]
    return scaled


def split_stress_commands(stress_cmds: list[str], parts: int) -> list[str] | None:
    """Split commands that differ only in their contiguous key ranges into `parts` commands over equal ranges.

    The commands' ranges `seq=a..b` together cover one range, which is cut into `parts` contiguous ranges of
    (nearly) equal length; a row count `n=` follows its command's range. Returns None when the commands can't be
    split: they differ in more than their range and row count, a command has no range, or the ranges have a gap.
    """
    if len(stress_cmds) == parts:
        return list(stress_cmds)
    if all(is_latte(stress_cmd) for stress_cmd in stress_cmds):
        return split_latte_commands(stress_cmds, parts)
    ranges = []
    templates = set()
    for stress_cmd in stress_cmds:
        if not (seq_range := SEQ_RANGE.search(stress_cmd)):
            return None
        ranges.append((int(seq_range.group(1)), int(seq_range.group(2))))
        templates.add(ROW_COUNT.sub("n=$rows", SEQ_RANGE.sub("seq=$range", stress_cmd, count=1), count=1))
    ranges.sort()
    if len(templates) != 1 or any(start != end + 1 for (_, end), (start, _) in zip(ranges, ranges[1:])):
        return None
    first, last = ranges[0][0], ranges[-1][1]
    length = last - first + 1
    if parts < 1 or parts > length:
        return None
    template = templates.pop()
    split = []
    for part in range(parts):
        start, end = first + length * part // parts, first + length * (part + 1) // parts - 1
        split.append(template.replace("seq=$range", f"seq={start}..{end}").replace("n=$rows", f"n={end - start + 1}"))
    return split


def _split_ranges(ranges: list[tuple[int, int]], parts: int) -> list[tuple[int, int]] | None:
    """Cut contiguous ranges into `parts` contiguous ranges of (nearly) equal length; None when they have a gap."""
    ranges = sorted(ranges)
    if any(start != end + 1 for (_, end), (start, _) in zip(ranges, ranges[1:])):
        return None
    first, last = ranges[0][0], ranges[-1][1]
    length = last - first + 1
    if parts < 1 or parts > length:
        return None
    return [(first + length * part // parts, first + length * (part + 1) // parts - 1) for part in range(parts)]


def split_latte_commands(stress_cmds: list[str], parts: int) -> list[str] | None:
    """Split latte commands that differ only in their contiguous cycle ranges into `parts` commands.

    Each command's offset must be its first cycle minus 1, as in the test-cases, so a split command gets the
    offset of its own range. A --duration in cycles must cover its command's range; it then follows the split
    range. row_count is the same in every command and stays. Returns None when the commands can't be split.
    """
    ranges = []
    templates = set()
    for stress_cmd in stress_cmds:
        start, end = LATTE_START.search(stress_cmd), LATTE_END.search(stress_cmd)
        if not (start and end):
            return None
        first, last = int(start.group(1)), int(end.group(1))
        if (offset := LATTE_OFFSET.search(stress_cmd)) and int(offset.group(1)) != first - 1:
            return None
        if (cycles := LATTE_CYCLES.search(stress_cmd)) and int(cycles.group(1)) != last - first + 1:
            return None
        ranges.append((first, last))
        template = LATTE_START.sub("--start-cycle $start", stress_cmd, count=1)
        template = LATTE_END.sub("--end-cycle $end", template, count=1)
        template = LATTE_OFFSET.sub("-P offset=$offset", template, count=1)
        templates.add(LATTE_CYCLES.sub("--duration $cycles", template, count=1))
    if len(templates) != 1 or (split_ranges := _split_ranges(ranges, parts)) is None:
        return None
    template = templates.pop()
    return [
        template.replace("--start-cycle $start", f"--start-cycle {start}")
        .replace("--end-cycle $end", f"--end-cycle {end}")
        .replace("-P offset=$offset", f"-P offset={start - 1}")
        .replace("--duration $cycles", f"--duration {end - start + 1}")
        for start, end in split_ranges
    ]
