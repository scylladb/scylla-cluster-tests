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
# Copyright (c) 2021 ScyllaDB
import logging
import os

LOGGER = logging.getLogger(__name__)


def get_job_name() -> str:
    return os.environ.get('JOB_NAME', 'local_run')


def get_job_url() -> str:
    return os.environ.get('BUILD_URL', '')
