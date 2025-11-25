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
from __future__ import annotations

import sys
import time
import logging
import datetime
import json
import os
from functools import wraps, partial, cached_property
from typing import Optional, Callable, TYPE_CHECKING

from botocore.exceptions import ClientError
from google.api_core.exceptions import ServiceUnavailable

from sdcm.argus_results import send_result_to_argus
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.event_counter import EventCounterContextManager
from sdcm.exceptions import UnsupportedNemesis
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.cluster_tools import check_cluster_layout

if TYPE_CHECKING:
    from sdcm.cluster import BaseCluster

LOGGER = logging.getLogger(__name__)


class Retry(Exception):
    pass


class retrying:
    """
        Used as a decorator to retry function run that can possibly fail with allowed exceptions list
    """

    def __init__(self, n=3, sleep_time=1,
                 allowed_exceptions=(Exception,), message="", timeout=0,
                 raise_on_exceeded=True):
        if n:
            self.n = n  # number of times to retry
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


timeout = partial(retrying, n=0)


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


def _find_hdr_tags(*args):
    for input_arg in args:
        if isinstance(input_arg, dict) and "hdr_tags" in input_arg:
            # NOTE: case when some method has 'hdr_tags' kwarg
            return input_arg["hdr_tags"]
        elif hasattr(input_arg, "hdr_tags"):
            # NOTE: case of 'stress_queue.hdr_tags' and 'nemesis.hdr_tags'
            return input_arg.hdr_tags
        elif isinstance(input_arg, (list, tuple)):
            # NOTE: case when 'stress_queue' is part of a returned tuple/(tuple of lists)
            hdr_tags = []
            for subinput_arg in input_arg:
                try:
                    hdr_tags = _find_hdr_tags(subinput_arg)
                    if hdr_tags:
                        return hdr_tags
                except ValueError:
                    continue
    raise ValueError("Failed to find 'hdr_tags'")


def latency_calculator_decorator(original_function: Optional[Callable] = None, *, legend: Optional[str] = None,
                                 cycle_name: Optional[str] = None, workload_type: Optional[str] = None, row_name: Optional[str] = None):
    """
    Gets the start time, end time and then calculates the latency based on function 'calculate_latency'.

    For proper usage, it requires workload name (write, read, mixed) to be included in the test name
    or setting 'workload_name' test parameter.
    Also requires monitoring set and 'use_hdrhistogram' test parameter to be set to True.

    :param func: Remote method to run.
    :return: Wrapped method.
    """
    # calling this import here, because of circular import
    from sdcm.utils import latency  # noqa: PLC0415

    def wrapper(func):

        @wraps(func)
        def wrapped(*args, **kwargs):  # noqa: PLR0914
            from sdcm.tester import ClusterTester  # noqa: PLC0415
            from sdcm.nemesis import Nemesis  # noqa: PLC0415
            start = time.time()
            # If the decorator is applied dynamically, "self" argument is not transferred  via "args" and may be found in bounded function
            _self = getattr(func, "__self__", None) or args[0]
            if isinstance(_self, ClusterTester):
                cluster = _self.db_cluster
                tester = _self
                monitoring_set = _self.monitors
            elif isinstance(_self, Nemesis):
                cluster = _self.cluster
                tester = _self.tester
                monitoring_set = _self.monitoring_set
            else:
                raise ValueError(
                    f"Not expected instance type '{type(_self)}'. Supported types: 'ClusterTester', 'Nemesis'")

            # Keep for debug purposes
            LOGGER.debug("latency_calculator_decorator cluster: %s", cluster)
            start_node_list = cluster.nodes[:]
            func_name = cycle_name or func.__name__
            with EventCounterContextManager(name=func.__name__,
                                            event_type=(DatabaseLogEvent.REACTOR_STALLED, )) as counter:

                res = func(*args, **kwargs)
                reactor_stall_stats = counter.get_stats().copy()
            end_node_list = cluster.nodes[:]
            all_nodes_list = list(set(start_node_list + end_node_list))
            end = time.time()
            test_name = tester.__repr__().split('testMethod=')[-1].split('>')[0]
            if not monitoring_set or not monitoring_set.nodes:
                return res
            monitor = monitoring_set.nodes[0]
            screenshots = monitoring_set.get_grafana_screenshots(node=monitor, test_start_time=start)
            if workload_type:
                workload = workload_type
            elif 'read' in test_name:
                workload = 'read'
            elif 'write' in test_name:
                workload = 'write'
            elif 'mixed' in test_name:
                workload = 'mixed'
            elif tester.params.get('workload_name'):
                workload = tester.params['workload_name']
            else:
                return res

            latency_results_file_path = tester.latency_results_file
            if not os.path.exists(latency_results_file_path):
                latency_results = {}
            else:
                with open(latency_results_file_path, encoding="utf-8") as file:
                    data = file.read().strip()
                    latency_results = json.loads(data or '{}')

            if "steady" not in func_name.lower():
                if func_name not in latency_results:
                    latency_results[func_name] = {"legend": legend or func_name}
                if 'cycles' not in latency_results[func_name]:
                    latency_results[func_name]['cycles'] = []

            result = latency.collect_latency(monitor, start, end, workload, cluster, all_nodes_list)
            result["screenshots"] = screenshots
            result["duration"] = f"{datetime.timedelta(seconds=int(end - start))}"
            result["duration_in_sec"] = int(end - start)

            try:
                hdr_tags = _find_hdr_tags(kwargs, res, _self)
            except Exception as err:  # noqa: BLE001
                LOGGER.error("Failed to find 'hdr_tags': %s", err)
                hdr_tags = []
            try:
                result["hdr"] = tester.get_hdrhistogram_by_interval(
                    hdr_tags=hdr_tags, stress_operation=workload,
                    start_time=start, end_time=end)
                LOGGER.debug("hdr: %s", result["hdr"])
            except Exception as err:  # noqa: BLE001
                LOGGER.error("Failed to get hdrhistogram_by_interval error: %s", err)
                result["hdr"] = {}

            try:
                result["hdr_summary"] = tester.get_hdrhistogram(
                    hdr_tags=hdr_tags, stress_operation=workload,
                    start_time=start, end_time=end)
                LOGGER.debug("HDR summary added to results: %s", result["hdr_summary"])
            except Exception as err:  # noqa: BLE001
                LOGGER.error("Failed to get hdrhistogram error: %s", err)
                result["hdr_summary"] = {}
            hdr_throughput = 0
            for summary, values in result["hdr_summary"].items():
                hdr_throughput += values["throughput"]
            LOGGER.debug("HDR throughput: %s", hdr_throughput)
            result["cycle_hdr_throughput"] = round(hdr_throughput)
            result["reactor_stalls_stats"] = reactor_stall_stats
            LOGGER.debug("Reactor stalls stats: %s", reactor_stall_stats)
            error_thresholds = tester.params.get("latency_decorator_error_thresholds")
            if "steady" in func_name.lower():
                if 'Steady State' not in latency_results:
                    latency_results['Steady State'] = result
                    send_result_to_argus(
                        argus_client=tester.test_config.argus_client(),
                        workload=workload,
                        name="Steady State",
                        description="Latencies without any operation running",
                        cycle=row_name or 0,
                        result=result,
                        start_time=start,
                        error_thresholds=error_thresholds,
                    )
            else:
                latency_results[func_name]['cycles'].append(result)
                LOGGER.debug("latency_results: %s", latency_results)
                LOGGER.debug("Send to Argus")
                send_result_to_argus(
                    argus_client=tester.test_config.argus_client(),
                    workload=workload,
                    name=f"{func_name}",
                    description=legend or "",
                    cycle=row_name or len(latency_results[func_name]['cycles']),
                    result=result,
                    start_time=start,
                    error_thresholds=error_thresholds,
                )
                LOGGER.debug("Saved in Argus")

            LOGGER.debug("Write results into file")
            with open(latency_results_file_path, 'w', encoding="utf-8") as file:
                json.dump(latency_results, file)
            LOGGER.debug("Results written into file")

            return res

        return wrapped

    if original_function:
        return wrapper(original_function)

    return wrapper


class NoValue(Exception):
    ...


class optional_cached_property(cached_property):
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


def skip_on_capacity_issues(func: Callable | None = None, db_cluster: BaseCluster | None = None):
    """
    Decorator to skip nemesis that fail due to capacity issues.
    Can be used with or without parameters:
        @skip_on_capacity_issues
        def foo(...): ...
    or
        @skip_on_capacity_issues(db_cluster=cluster)
        def foo(...): ...
    """
    def decorator(inner_func):
        @wraps(inner_func)
        def wrapper(*args, **kwargs):
            cluster = db_cluster
            # Try to get db_cluster from inner_func's bound instance if not provided
            if cluster is None and args:
                bound_self = getattr(inner_func, "__self__", None)
                if bound_self and hasattr(bound_self, "nodes"):
                    cluster = bound_self
                else:
                    for arg in args:
                        if hasattr(arg, "nodes"):  # crude check for cluster-like object
                            cluster = arg
                            break
            try:
                return inner_func(*args, **kwargs)
            except ClientError as ex:
                if "InsufficientInstanceCapacity" in str(ex):
                    if not check_cluster_layout(cluster):
                        TestFrameworkEvent(
                            source=inner_func.__name__,
                            message=f"Test failed due to capacity issues: {ex} cluster is unbalanced, continuing with test would yield unknown results",
                            severity=Severity.CRITICAL
                        ).publish()
                    else:
                        raise UnsupportedNemesis("Capacity Issue") from ex
                raise
            except ServiceUnavailable as ex:
                if not check_cluster_layout(cluster):
                    TestFrameworkEvent(
                        source=inner_func.__name__,
                        message=f"Test failed due to service availability issues: {ex} cluster is unbalanced, continuing with test would yield unknown results",
                        severity=Severity.CRITICAL
                    ).publish()
                else:
                    raise UnsupportedNemesis("Capacity Issue") from ex
        return wrapper

    if func is not None and callable(func):
        return decorator(func)
    return decorator


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
                TestFrameworkEvent(source=func.__name__,
                                   message=f"Test failed due to capacity issues: {ex} "
                                   "cluster is probably unbalanced, continuing with test would yield unknown results",
                                   severity=Severity.CRITICAL).publish()
            raise
        except ServiceUnavailable as ex:
            TestFrameworkEvent(source=func.__name__,
                               message=f"Test failed due to service availability issues: {ex} "
                               "cluster is probably unbalanced, continuing with test would yield unknown results",
                               severity=Severity.CRITICAL).publish()
    return wrapper


def optional_stage(stage_names: str | list[str]):
    """
    Decorator skips the decorated functon if the provided test stage is set to be skipped in the test configuration
    'skip_test_stages' parameter.
    More details can be found in https://github.com/scylladb/scylla-cluster-tests/blob/master/docs/faq.md

    :param stage_names: str or list, name of the test stage(s)
    """
    stage_names = stage_names if isinstance(stage_names, list) else [stage_names]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # making import here, to work around circular import issue
            from sdcm.cluster import TestConfig  # noqa: PLC0415
            skip_test_stages = TestConfig().tester_obj().skip_test_stages
            skipped_stages = [stage for stage in stage_names if skip_test_stages[stage]]

            if not skipped_stages:
                return func(*args, **kwargs)
            else:
                skipped_stages_str = ', '.join(skipped_stages)
                LOGGER.warning("'%s' is skipped as '%s' test stage(s) is disabled.", func.__name__, skipped_stages_str)
        return wrapper
    return decorator
