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
import time
import datetime
from functools import wraps

from avocado.utils import process

logger = logging.getLogger('avocado.test')


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
        # clean old files
        process.run('rm -rf scylla-grafana-monitoring/')
        process.run('rm -rf data_dir/grafana')
        process.run('git clone https://github.com/scylladb/scylla-grafana-monitoring/')
        # convert template to final dashboard
        for i in glob.glob('scylla-grafana-monitoring/grafana/*.*.template.json'):
            process.run('cd scylla-grafana-monitoring/ && mkdir -p grafana/build && pwd && ./make_dashboards.py -t grafana/types.json -d %s'
                        % i.replace('scylla-grafana-monitoring', '.'), shell=True, verbose=True)
        # copy dashboard to data_dir
        process.run('cp -r scylla-grafana-monitoring/grafana data_dir/', shell=True)
        process.run('cp scylla-grafana-monitoring/grafana/build/* data_dir/grafana/', shell=True)

    if not full_version or '666.development' in full_version:
        ret = 'master'
    else:
        ret = re.findall("-(\d+\.\d+)", full_version)[0]

    # We only add dashboard for release version, let's use master for pre-release version
    jsons = glob.glob('data_dir/grafana/*.%s.json' % ret)
    if not jsons:
        ret = 'master'

    return ret


class retrying(object):
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """
    def __init__(self, n=3, sleep_time=1, allowed_exceptions=(Exception,)):
        self.n = n  # number of times to retry
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            for i in xrange(self.n):
                try:
                    return func(*args, **kwargs)
                except self.allowed_exceptions as e:
                    logger.debug("retrying: %r" % e)
                    time.sleep(self.sleep_time)
                    if i == self.n - 1:
                        logger.error("Number of retries exceeded!")
                        raise
        return inner


def log_run_info(arg):
    """
        Decorator that prints BEGIN before the function runs and END when function finished running.
        Uses function name as a name of action or string that can be given to the decorator.
        If the function is a method of a class object, the class name will be printed out.

        Usage examples:
            @log_run_info
            def foo(x, y=1):
                pass
            In: foo(1)
            Out:
                BEGIN: foo
                END: foo (ran 0.000164)s

            @log_run_info("Execute nemesis")
            def disrupt():
                pass
            In: disrupt()
            Out:
                BEGIN: Execute nemesis
                END: Execute nemesis (ran 0.000271)s
    """
    def _inner(func, msg=None):
        @wraps(func)
        def inner(*args, **kwargs):
            class_name = ""
            if len(args) > 0 and func.__name__ in dir(args[0]):
                class_name = " <%s>" % args[0].__class__.__name__
            action = "%s%s" % (msg, class_name)
            start_time = datetime.datetime.now()
            logger.info("BEGIN: %s", action)
            res = func(*args, **kwargs)
            end_time = datetime.datetime.now()
            logger.info("END: %s (ran %ss)", action, (end_time - start_time).total_seconds())
            return res
        return inner

    if callable(arg):  # when decorator is used without a string message
        return _inner(arg, arg.__name__)
    else:
        return lambda f: _inner(f, arg)
