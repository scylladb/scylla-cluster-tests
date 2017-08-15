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
# Copyright (c) 2017 ScyllaDB

import logging
import re
import os
import glob

from avocado.utils import process


def _remote_get_hash(remoter, file_path):
    try:
        result = remoter.run('md5sum {}'.format(file_path), verbose=True)
        return result.stdout.strip().split()[0]
    except Exception as details:
        test_logger = logging.getLogger('avocado.test')
        test_logger.error(str(details))
        return None


def _remote_get_file(remoter, src, dst):
    result = remoter.run('curl -L {} -o {}'.format(src, dst), ignore_status=True)


def remote_get_file(remoter, src, dst, hash_expected=None, retries=1):
    if not hash_expected:
        _remote_get_file(remoter, src, dst)
        return
    while retries > 0 and _remote_get_hash(remoter, dst) != hash_expected:
        _remote_get_file(remoter, src, dst)
        retries -= 1
    #assert _remote_get_hash(remoter, dst) == hash_expected


def get_monitor_version(full_version, clone=False):
    """
    Detect matched dashboard version from scylla version.

    :param full_version: version info returned by `scylla --version`
    :param clone: force to clone scylla-grafana-monitoring project
    :return: dashboard version (eg: 1.7, 2.0, master)
    """
    if not os.path.exists('scylla-grafana-monitoring/') or clone:
        process.run('rm -rf scylla-grafana-monitoring/')
        process.run('git clone https://github.com/scylladb/scylla-grafana-monitoring/')
        process.run('cp -r scylla-grafana-monitoring/grafana data_dir/')

    if not full_version or '666.development' in full_version:
        ret = 'master'
    else:
        ret = re.findall("-(\d+\.\d+)", full_version)[0]

    # We only add dashboard for release version, let's use master for pre-release version
    jsons = glob.glob('data_dir/grafana/*.%s.json' % ret)
    if not jsons:
        ret = 'master'

    return ret
