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

import random

from libcloud.compute.providers import Provider, get_driver

from sdcm.keystore import KeyStore


GceDriver = get_driver(Provider.GCE)  # pylint: disable=invalid-name


def append_zone(region: str) -> str:
    assert region.startswith("us-east1"), "only `us-east1' region is supported"
    if region.count("-") == 1:
        # us-east1 zones: b, c, and d. Details: https://cloud.google.com/compute/docs/regions-zones#locations
        return f"{region}-{random.choice('bccdd')}"  # choose zones c and d twice as often as zone b
    return region


def _get_gce_service(credentials: dict, datacenter: str) -> GceDriver:
    return GceDriver(user_id=credentials["project_id"] + "@appspot.gserviceaccount.com",
                     key=credentials["private_key"],
                     datacenter=datacenter,
                     project=credentials["project_id"])


def get_gce_services(regions: list) -> dict:
    credentials = KeyStore().get_gcp_credentials()
    return {region_az: _get_gce_service(credentials, region_az) for region_az in map(append_zone, regions)}
