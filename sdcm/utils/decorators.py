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

import sys
import time
import logging
import datetime
import threading
from functools import wraps, partial


LOGGER = logging.getLogger(__name__)


try:
    from functools import cached_property  # pylint: disable=unused-import,ungrouped-imports
except ImportError:
    # Copy/pasted from https://github.com/python/cpython/blob/3.8/Lib/functools.py#L923

    # TODO: remove this code after switching to Python 3.8+

    ################################################################################
    # cached_property() - computed once per instance, cached as attribute
    ################################################################################

    _NOT_FOUND = object()

    class cached_property:  # pylint: disable=invalid-name,too-few-public-methods
        def __init__(self, func):
            self.func = func
            self.attrname = None
            self.__doc__ = func.__doc__
            self.lock = threading.RLock()

        def __set_name__(self, owner, name):
            if self.attrname is None:
                self.attrname = name
            elif name != self.attrname:
                raise TypeError(
                    "Cannot assign the same cached_property to two different names "
                    f"({self.attrname!r} and {name!r})."
                )

        def __get__(self, instance, owner=None):
            if instance is None:
                return self
            if self.attrname is None:
                raise TypeError(
                    "Cannot use cached_property instance without calling __set_name__ on it.")
            try:
                cache = instance.__dict__
            except AttributeError:  # not all objects have __dict__ (e.g. class defines slots)
                msg = (
                    f"No '__dict__' attribute on {type(instance).__name__!r} "
                    f"instance to cache {self.attrname!r} property."
                )
                raise TypeError(msg) from None
            val = cache.get(self.attrname, _NOT_FOUND)
            if val is _NOT_FOUND:
                with self.lock:
                    # check if another thread filled cache while we awaited lock
                    val = cache.get(self.attrname, _NOT_FOUND)
                    if val is _NOT_FOUND:
                        val = self.func(instance)
                        try:
                            cache[self.attrname] = val
                        except TypeError:
                            msg = (
                                f"The '__dict__' attribute on {type(instance).__name__!r} instance "
                                f"does not support item assignment for caching {self.attrname!r} property."
                            )
                            raise TypeError(msg) from None
            return val


class Retry(Exception):
    pass


class retrying:  # pylint: disable=invalid-name,too-few-public-methods
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, n=3, sleep_time=1,  # pylint: disable=too-many-arguments
                 allowed_exceptions=(Exception,), message="", timeout=0):  # pylint: disable=redefined-outer-name
        if n:
            self.n = n  # number of times to retry  # pylint: disable=invalid-name
        else:
            self.n = sys.maxsize * 2 + 1
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries
        self.timeout = timeout  # if timeout is defined it will raise error
        #   if it is reached even when maximum retries not reached yet

    def __call__(self, func):
        @wraps(func)
        def inner(*args, **kwargs):
            if self.timeout:
                end_time = time.time() + self.timeout
            else:
                end_time = 0
            if self.n == 1:
                # there is no need to retry
                return func(*args, **kwargs)
            for i in range(self.n):
                try:
                    if self.message and i > 0:
                        LOGGER.info("%s [try #%s]", self.message, i)
                    return func(*args, **kwargs)
                except self.allowed_exceptions as ex:
                    LOGGER.debug("'%s': failed with '%r', retrying [#%s]", func.__name__, ex, i)
                    time.sleep(self.sleep_time)
                    if i == self.n - 1 or (end_time and time.time() > end_time):
                        LOGGER.error(f"'{func.__name__}': Number of retries exceeded!")
                        raise

        return inner


timeout = partial(retrying, n=0)  # pylint: disable=invalid-name


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
            if args and func.__name__ in dir(args[0]):
                class_name = " <%s>" % args[0].__class__.__name__
            action = "%s%s" % (msg, class_name)
            start_time = datetime.datetime.now()
            LOGGER.debug("BEGIN: %s", action)
            res = func(*args, **kwargs)
            end_time = datetime.datetime.now()
            LOGGER.debug("END: %s (ran %ss)", action, (end_time - start_time).total_seconds())
            return res
        return inner

    if callable(arg):  # when decorator is used without a string message
        return _inner(arg, arg.__name__)
    else:
        return lambda f: _inner(f, arg)


def measure_time(func):
    """
    For an input given function, it returns a wrapper function that returns how long it takes to execute, and the function's result.

    Example:

    @measure_time
    def _run_repair(self, node):
        result = node.run_nodetool(sub_cmd='repair')
        return result

    repair_time, res = self._run_repair(node=node3)
    :param func:
    :return:
    """
    @wraps(func)
    def wrapped(*args, **kwargs):
        start = time.time()
        func_res = func(*args, **kwargs)
        end = time.time()
        return end - start, func_res

    return wrapped
