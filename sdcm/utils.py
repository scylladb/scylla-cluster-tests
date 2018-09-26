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
import time
import datetime
from functools import wraps
from enum import Enum



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


class retrying(object):
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """
    def __init__(self, n=3, sleep_time=1, allowed_exceptions=(Exception,), message=""):
        self.n = n  # number of times to retry
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            for i in xrange(self.n):
                try:
                    if self.message:
                        logger.info("%s [try #%s]" % (self.message, i))
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
            logger.debug("BEGIN: %s", action)
            res = func(*args, **kwargs)
            end_time = datetime.datetime.now()
            logger.debug("END: %s (ran %ss)", action, (end_time - start_time).total_seconds())
            return res
        return inner

    if callable(arg):  # when decorator is used without a string message
        return _inner(arg, arg.__name__)
    else:
        return lambda f: _inner(f, arg)

class Distro(Enum):
    UNKNOWN = 0
    CENTOS7 = 1
    RHEL7 = 2
    UBUNTU14 = 3
    UBUNTU16 = 4
    DEBIAN8 = 5
    DEBIAN9 = 6
