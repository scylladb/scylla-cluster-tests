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

import os
import logging
from typing import Optional

import click

import __main__

from sdcm.cluster import TestConfig
from sdcm.utils.azure_utils import AzureService
from sdcm.utils.azure_region import region_name_to_location
from sdcm.utils.common import (
    all_aws_regions,
    get_all_gce_regions,
)


def get_all_regions(cloud_provider: str) -> list[str] | None:
    regions = None
    if cloud_provider == "aws":
        regions = all_aws_regions(cached=True)
    elif cloud_provider == "gce":
        regions = get_all_gce_regions()
    elif cloud_provider == "azure":
        regions = AzureService().all_regions
    return regions


class CloudRegion(click.ParamType):
    name = "cloud_region"

    def __init__(self, cloud_provider: Optional[str] = None):
        super().__init__()
        self.cloud_provider = cloud_provider

    def convert(self, value, param, ctx):
        cloud_provider = self.cloud_provider or ctx.params["cloud_provider"]
        regions = get_all_regions(cloud_provider)
        if not regions:
            self.fail(f"unknown cloud provider: {cloud_provider}")
        if cloud_provider == "azure":
            value = region_name_to_location(value)
        if value not in regions:
            self.fail(f"invalid region: {value}. (choose from {', '.join(regions)})")
        return value


def get_test_config():
    return TestConfig()


def add_file_logger(level: int = logging.DEBUG) -> None:
    cmd_path = "-".join(click.get_current_context().command_path.split()[1:])
    logdir = get_test_config().make_new_logdir(update_latest_symlink=False, postfix=f"-{cmd_path}")
    handler = logging.FileHandler(os.path.join(logdir, "hydra.log"))
    handler.setLevel(level)

    logger = getattr(__main__, "LOGGER")
    if logger:
        logger.addHandler(handler)
