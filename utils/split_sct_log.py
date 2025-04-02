#!/usr/bin/env python
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
# Copyright (c) 2022 ScyllaDB

"""
Splits sct.log file into parts, splitted by nemesis.
Run this script from directory where sct.log is.
Best experience when this script is added to dir that belongs to PATH env variable.
e.g.
chmod +x utils/split_sct_log.py
sudo ln -s <sct_repo_dir>/utils/split_sct_log.py /usr/bin/split_sct_log.py
"""

from pathlib import Path


def get_time(line_content):
    """Gets time from log line. Example line:
    < t:2022-02-11 20:21:40,987 f:nemesis.py      l:1216 c:sdcm.nemesis   [...]"""
    return "_".join(line_content.split(" ")[2:3]).split(",", maxsplit=1)[0]


input_dir = Path.cwd()
Path(input_dir / "parts").mkdir(exist_ok=True)
with open(input_dir / "sct.log", "r", encoding="utf-8") as sct_log:
    idx = 0
    line = sct_log.readline()
    time = get_time(line)
    part_file = open(input_dir / "parts" / f"{idx:03d}_{time}_start.log", "w",
                     encoding="utf-8")
    part_file.write(line)
    for line in sct_log.readlines():
        if ">>>>>Started random_disrupt_method" in line:
            idx += 1
            name = line.split(" ")[-1]
            print(f"processing nemesis: {name[:-1]}...")
            time = get_time(line)
            part_file.close()
            part_file = open(
                input_dir / "parts" / f"{idx:03d}_{time}_{name[:-1]}.log", "w", encoding="utf-8")
        part_file.write(line)
    part_file.close()
