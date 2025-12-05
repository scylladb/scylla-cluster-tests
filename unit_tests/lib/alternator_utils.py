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
# Copyright (c) 2020 ScyllaDB
from sdcm.utils import alternator

ALTERNATOR_PORT = 8000
TEST_PARAMS = dict(
    dynamodb_primarykey_type="HASH_AND_RANGE",
    alternator_use_dns_routing=True,
    alternator_port=ALTERNATOR_PORT,
    docker_network="ycsb_net",
)
ALTERNATOR = alternator.api.Alternator(
    sct_params={
        "alternator_access_key_id": None,
        "alternator_secret_access_key": None,
        "alternator_port": ALTERNATOR_PORT,
    }
)
