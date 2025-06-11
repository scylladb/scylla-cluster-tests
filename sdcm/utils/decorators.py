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
import json
import os
from functools import wraps, partial, cached_property
from typing import Optional, Callable

from botocore.exceptions import ClientError
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.event_counter import EventCounterContextManager
from sdcm.exceptions import UnsupportedNemesis
from sdcm.sct_events.system import TestFrameworkEvent

LOGGER = logging.getLogger(__name__)


class Retry(Exception):
    pass


class retrying:  # pylint: disable=invalid-name,too-few-public-methods
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    # pylint: disable=too-many-arguments,redefined-outer-name
    def __init__(self, n=3, sleep_time=1,
                 allowed_exceptions=(Exception,), message="", timeout=0,
                 raise_on_exceeded=True):
        if n:
            self.n = n  # number of times to retry  # pylint: disable=invalid-name
        else:
            self.n = sys.maxsize * 2 + 1
        self.sleep_time = sleep_time  # number seconds to sleep between retries
        self.allowed_exceptions = allowed_exceptions  # if Exception is not allowed will raise
        self.message = message  # string that will be printed between retries
        self.timeout = timeout  # if timeout is defined it will raise error
        #   if it is reached even when maximum retries not reached yet
        self.raise_on_exceeded = raise_on_exceeded  # if True - raise exception when number of retries exceeded,
        # otherwise - return None

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
                        LOGGER.error("'%s': Number of retries exceeded!", func.__name__)
                        if self.raise_on_exceeded:
                            raise
            return None

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


def latency_calculator_decorator(original_function: Optional[Callable] = None, *, legend: Optional[str] = None):
    """
    Gets the start time, end time and then calculates the latency based on function 'calculate_latency'.

    :param func: Remote method to run.
    :return: Wrapped method.
    """
    # calling this import here, because of circular import
    from sdcm.utils import latency  # pylint: disable=import-outside-toplevel

    def wrapper(func):

        @wraps(func)
        def wrapped(*args, **kwargs):  # pylint: disable=too-many-branches, too-many-locals
            start = time.time()
            start_node_list = args[0].cluster.nodes[:]
            reactor_stall_stats = {}
            with EventCounterContextManager(name=func.__name__,
                                            event_type=(DatabaseLogEvent.REACTOR_STALLED, )) as counter:

                res = func(*args, **kwargs)
                reactor_stall_stats = counter.get_stats().copy()
            end_node_list = args[0].cluster.nodes[:]
            all_nodes_list = list(set(start_node_list + end_node_list))
            end = time.time()
            test_name = args[0].tester.__repr__().split('testMethod=')[-1].split('>')[0]
            if not args[0].monitoring_set or not args[0].monitoring_set.nodes:
                return res
            monitor = args[0].monitoring_set.nodes[0]
            screenshots = args[0].monitoring_set.get_grafana_screenshots(node=monitor, test_start_time=start)
            if 'read' in test_name:
                workload = 'read'
            elif 'write' in test_name:
                workload = 'write'
            elif 'mixed' in test_name:
                workload = 'mixed'
            else:
                return res

            latency_results_file_path = args[0].tester.latency_results_file
            if not os.path.exists(latency_results_file_path):
                latency_results = {}
            else:
                with open(latency_results_file_path, encoding="utf-8") as file:
                    data = file.read().strip()
                    latency_results = json.loads(data or '{}')

            if "steady" not in func.__name__.lower():
                if func.__name__ not in latency_results:
                    latency_results[func.__name__] = {"legend": legend or func.__name__}
                if 'cycles' not in latency_results[func.__name__]:
                    latency_results[func.__name__]['cycles'] = []

            result = latency.collect_latency(monitor, start, end, workload, args[0].cluster, all_nodes_list)
            result["screenshots"] = screenshots
            result["duration"] = f"{datetime.timedelta(seconds=int(end - start))}"
            result["duration_in_sec"] = int(end - start)
            result["hdr"] = args[0].tester.get_cs_range_histogram_by_interval(stress_operation=workload,
                                                                              start_time=start,
                                                                              end_time=end)
            result["hdr_summary"] = args[0].tester.get_cs_range_histogram(stress_operation=workload,
                                                                          start_time=start,
                                                                          end_time=end)
            result["reactor_stalls_stats"] = reactor_stall_stats

            if "steady" in func.__name__.lower():
                if 'Steady State' not in latency_results:
                    latency_results['Steady State'] = result
            else:
                latency_results[func.__name__]['cycles'].append(result)

            with open(latency_results_file_path, 'w', encoding="utf-8") as file:
                json.dump(latency_results, file)

            return res

        return wrapped

    if original_function:
        return wrapper(original_function)

    return wrapper


class NoValue(Exception):
    ...


class optional_cached_property(cached_property):  # pylint: disable=invalid-name,too-few-public-methods
    """Extension for cached_property from Lib/functools.py with ability to ignore calculated result.

    To make it to not cache a value on a property call raise NoValue exception: it will be ignored and
    None will be returned as a result.
    """

    def __get__(self, instance, owner=None):
        try:
            return super().__get__(instance=instance, owner=owner)
        except NoValue:
            return None


def static_init(cls):
    """A decorator for classes that make sure to call 'static_init' method if
    such a method exists.

    Returns:
        The class that was initialized.
    """
    if hasattr(cls, "static_init"):
        cls.static_init()
    return cls


def skip_on_capacity_issues(func: callable) -> callable:
    """
    Decorator to skip nemesis that fail due to capacity issues
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as ex:
            if "InsufficientInstanceCapacity" in str(ex):
                raise UnsupportedNemesis("Capacity Issue") from ex
            raise
    return wrapper


def critical_on_capacity_issues(func: callable) -> callable:
    """
    Decorator to end the test with a critical event due to capacity issues
    This should be used when a failure would leave the cluster in an inconsistent topology state
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as ex:
            if "InsufficientInstanceCapacity" in str(ex):
                TestFrameworkEvent(source=callable.__name__,
                                   message=f"Test failed due to capacity issues: {ex} "
                                   "cluster is probably unbalanced, continuing with test would yield unknown results",
                                   severity=Severity.CRITICAL).publish()
            raise
    return wrapper
