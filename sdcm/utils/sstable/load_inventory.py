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
# Copyright (c) 2021 ScyllaDB
"""
Keep AWS S3 info about test sstables for refresh nemesis
"""

from typing import NamedTuple


class TestDataInventory(NamedTuple):
    sstable_url: str
    sstable_file: str
    sstable_md5: str
    keys_num: int


URL_TEMPLATE = "https://s3.amazonaws.com/scylla-qa-team/%s/%s"
TEMP_FILE_TEMPLATE = "/tmp/%s"
ONE_COLUMN_FOLDER = "refresh_nemesis_c0"
MULTI_COLUMN_FOLDER = "refresh_nemesis"
LOAD_AND_STREAM_FOLDER = f"{MULTI_COLUMN_FOLDER}/load_and_stream"
STANDARD1_100M_FILE_NAME = "keyspace1.standard1.100M.tar.gz"
STANDARD1_FILE_NAME = "keyspace1.standard1.tar.gz"

BIG_SSTABLE_COLUMN_1_DATA = [
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (ONE_COLUMN_FOLDER, STANDARD1_100M_FILE_NAME),
        sstable_file=TEMP_FILE_TEMPLATE % STANDARD1_100M_FILE_NAME,
        sstable_md5="e4f6addc2db8e9af3a906953288ef676",
        keys_num=1001000,
    )
]

COLUMN_1_DATA = [
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (ONE_COLUMN_FOLDER, STANDARD1_FILE_NAME),
        sstable_file=TEMP_FILE_TEMPLATE % STANDARD1_FILE_NAME,
        sstable_md5="c4aee10691fa6343a786f52663e7f758",
        keys_num=1000,
    )
]

# 100G, the big file will be saved to GCE image
# Fixme: It's very slow and unstable to download 100G files from S3 to GCE instances,
#        currently we actually uploaded a small file (3.6 K) to S3.
#        We had a solution to save the file in GCE image, it requires bigger boot disk.
#        In my old test, the instance init is easy to fail. We can try to use a
#        split shared disk to save the 100GB file.
# 100M (500000 rows)
BIG_SSTABLE_MULTI_COLUMNS_DATA = [
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (MULTI_COLUMN_FOLDER, STANDARD1_100M_FILE_NAME),
        sstable_file=TEMP_FILE_TEMPLATE % STANDARD1_100M_FILE_NAME,
        sstable_md5="9c5dd19cfc78052323995198b0817270",
        keys_num=501000,
    )
]

MULTI_COLUMNS_DATA = [
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (MULTI_COLUMN_FOLDER, STANDARD1_FILE_NAME),
        sstable_file=TEMP_FILE_TEMPLATE % STANDARD1_FILE_NAME,
        sstable_md5="c033a3649a1aec3ba9b81c446c6eecfd",
        keys_num=1000,
    )
]

MULTI_NODE_DATA = [
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (LOAD_AND_STREAM_FOLDER, f"node1.{STANDARD1_FILE_NAME}"),
        sstable_file=TEMP_FILE_TEMPLATE % f"node1.{STANDARD1_FILE_NAME}",
        sstable_md5="994bdcef67cd9748c0214a67f0f1864d",
        keys_num=500000,
    ),
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (LOAD_AND_STREAM_FOLDER, f"node2.part1.{STANDARD1_FILE_NAME}"),
        sstable_file=TEMP_FILE_TEMPLATE % f"node2.part1.{STANDARD1_FILE_NAME}",
        sstable_md5="651ceab061723d078525fb996d2e17a4",
        keys_num=500000,
    ),
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (LOAD_AND_STREAM_FOLDER, f"node2.part2.{STANDARD1_FILE_NAME}"),
        sstable_file=TEMP_FILE_TEMPLATE % f"node2.part2.{STANDARD1_FILE_NAME}",
        sstable_md5="a30df21bfa4e66b99d5de30c11456d2b",
        keys_num=500000,
    ),
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (LOAD_AND_STREAM_FOLDER, f"node3.{STANDARD1_FILE_NAME}"),
        sstable_file=TEMP_FILE_TEMPLATE % f"node3.{STANDARD1_FILE_NAME}",
        sstable_md5="ace4eb18bd77811fdf627f1044909f18",
        keys_num=500000,
    ),
    TestDataInventory(
        sstable_url=URL_TEMPLATE % (LOAD_AND_STREAM_FOLDER, f"node4.{STANDARD1_FILE_NAME}"),
        sstable_file=TEMP_FILE_TEMPLATE % f"node4.{STANDARD1_FILE_NAME}",
        sstable_md5="e7ff37333c1e331acac58103e6e0406d",
        keys_num=500000,
    ),
]
