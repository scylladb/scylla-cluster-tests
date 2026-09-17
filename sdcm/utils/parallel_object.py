from __future__ import absolute_import, annotations

import logging
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from concurrent.futures.thread import _global_shutdown_lock, _threads_queues
from functools import wraps
from typing import Iterable, Callable, List

from sdcm.utils.hard_exit import request_hard_exit

LOGGER = logging.getLogger("utils")

# How long clean_up() gives a still-alive pool worker to actually notice
# shutdown(wait=False)'s sentinel, finish its current unit of work and unwind
# before treating it as stuck. shutdown(wait=False) returns immediately without
# waiting for anything, so a perfectly healthy worker that just hasn't won the
# GIL yet will routinely still report is_alive() == True right afterwards; this
# grace period is what turns "still alive this instant" into a meaningful signal.
# It runs on every clean_up() call (i.e. every ParallelObject.run()), so it is
# kept short enough not to noticeably slow down normal teardown while still
# giving a healthy worker a fair chance.
WORKER_JOIN_GRACE_PERIOD = 1  # seconds

# How long clean_up() waits to acquire _global_shutdown_lock before giving up
# and proceeding without it. See the comment at its use below for why this is
# believed to be safe.
GLOBAL_SHUTDOWN_LOCK_TIMEOUT = 1  # seconds


class ParallelObject:
    """
    Run function in with supplied args in parallel using thread.
    """

    def __init__(self, objects: Iterable, timeout: int = 6, num_workers: int = None, disable_logging: bool = False):
        """Constructor for ParallelObject

        Build instances of Parallel object. Item of objects is used as parameter for
        disrupt_func which will be run in parallel.

        :param objects: if item in object is list, it will be upacked to disrupt_func argument, ex *arg
                if item in object is dict, it will be upacked to disrupt_func keyword argument, ex **kwarg
                if item in object is any other type, will be passed to disrupt_func as is.
                if function accept list as parameter, the item shuld be list of list item = [[]]

        :param timeout: global timeout for running all
        :param num_workers: num of parallel threads, defaults to None
        :param disable_logging: disable logging for running disrupt_func, defaults to False
        """
        self.objects = objects
        self.timeout = timeout
        self.num_workers = num_workers
        self.disable_logging = disable_logging
        self._thread_pool = ThreadPoolExecutor(max_workers=self.num_workers)

    def run(self, func: Callable, ignore_exceptions=False, unpack_objects: bool = False) -> List[ParallelObjectResult]:
        """Run callable object "disrupt_func" in parallel

        Allow to run callable object in parallel.
        if ignore_exceptions is true,  return
        list of FutureResult object instances which contains
        two attributes:
            - result - result of callable object execution
            - exc - exception object, if happened during run
        if ignore_exceptions is False, then running will
        terminated on future where happened exception or by timeout
        what has stepped first.

        :param func: Callable object to run in parallel
        :param ignore_exceptions: ignore exception and return result, defaults to False
        :param unpack_objects: set to True when unpacking of objects to the disrupt_func as args or kwargs needed
        :returns: list of FutureResult object
        :rtype: {List[FutureResult]}
        """

        def func_wrap(fun):
            @wraps(fun)
            def inner(*args, **kwargs):
                thread_name = threading.current_thread().name
                fun_args = args
                fun_kwargs = kwargs
                fun_name = fun.__name__
                LOGGER.debug(f"[{thread_name}] {fun_name}({fun_args}, {fun_kwargs})")
                return_val = fun(*args, **kwargs)
                LOGGER.debug(f"[{thread_name}] Done.")
                return return_val

            return inner

        results = []

        if not self.disable_logging:
            LOGGER.debug(f"Executing in parallel: '{func.__name__}' on {self.objects}")
            func = func_wrap(func)

        futures = []

        for obj in self.objects:
            if unpack_objects and isinstance(obj, (list, tuple)):
                futures.append((self._thread_pool.submit(func, *obj), obj))
            elif unpack_objects and isinstance(obj, dict):
                futures.append((self._thread_pool.submit(func, **obj), obj))
            else:
                futures.append((self._thread_pool.submit(func, obj), obj))
        time_out = self.timeout
        for future, target_obj in futures:
            try:
                result = future.result(time_out)
            except FuturesTimeoutError as exception:
                results.append(ParallelObjectResult(obj=target_obj, exc=exception, result=None))
                time_out = 0.001  # if there was a timeout on one of the futures there is no need to wait for all
            except Exception as exception:  # noqa: BLE001
                results.append(ParallelObjectResult(obj=target_obj, exc=exception, result=None))
            else:
                results.append(ParallelObjectResult(obj=target_obj, exc=None, result=result))

        self.clean_up(futures)

        if ignore_exceptions:
            return results

        runs_that_finished_with_exception = [res for res in results if res.exc]
        if runs_that_finished_with_exception:
            raise ParallelObjectException(results=results)
        return results

    def call_objects(self, ignore_exceptions: bool = False) -> list["ParallelObjectResult"]:
        """
        Use the ParallelObject run() method to call a list of
        callables in parallel. Rather than running a single function
        with a number of objects as arguments in parallel, we're
        calling a list of callables in parallel.

        If we need to run multiple callables with some arguments, one
        solution is to use partial objects to pack the callable with
        its arguments, e.g.:

        partial_func_1 = partial(print, "lorem")
        partial_func_2 = partial(sum, (2, 3))
        ParallelObject(objects=[partial_func_1, partial_func_2]).call_objects()

        This can be useful if we need to tightly synchronise the
        execution of multiple functions.
        """
        return self.run(lambda x: x(), ignore_exceptions=ignore_exceptions)

    def clean_up(self, futures):
        # TODO SCT-803: route clean-resources/collect-logs through exit_process() --
        # ParallelObject usage there can arm a hard-exit that's currently silently
        # ignored, since those CLI commands exit through Click's normal return path.
        # if there are futures that didn't run  we cancel them
        for future, _ in futures:
            future.cancel()
        self._thread_pool.shutdown(wait=False)

        # Worker threads must be detached from CPython's own shutdown-join
        # bookkeeping (`_threads_queues`, and on 3.10-3.13 `threading._shutdown_locks`)
        # or an abandoned worker hangs the whole process forever at interpreter exit.
        # `_threads_queues` is guarded by `_global_shutdown_lock`, acquired below with a
        # timeout so a stuck lock can't turn this safety code into another hang. On
        # 3.14+, CPython's own join moved into `_thread._shutdown()`, which this module
        # has no hook into -- os._exit() in sdcm.utils.hard_exit is the real backstop
        # there (see the worker-join check below). Full history: PR #15681 (SCT-803).
        shutdown_locks = getattr(threading, "_shutdown_locks", None)
        lock_acquired = _global_shutdown_lock.acquire(timeout=GLOBAL_SHUTDOWN_LOCK_TIMEOUT)
        if not lock_acquired:
            LOGGER.warning(
                "ParallelObject.clean_up(): could not acquire _global_shutdown_lock "
                "within %ss; proceeding without it to avoid clean_up() itself hanging.",
                GLOBAL_SHUTDOWN_LOCK_TIMEOUT,
            )
        try:
            for thread in self._thread_pool._threads:
                _threads_queues.pop(thread, None)
                tstate_lock = getattr(thread, "_tstate_lock", None)
                if shutdown_locks is not None and tstate_lock is not None:
                    shutdown_locks.discard(tstate_lock)
        finally:
            if lock_acquired:
                _global_shutdown_lock.release()

        # A worker still alive after a short, shared-deadline grace period (mirrors the
        # join(timeout)-then-check pattern `stop_nemesis` in sdcm/cluster.py uses) is
        # treated as stuck and arms the same hard-exit escalation as a final backstop --
        # this is what actually protects us on 3.14+, where popping the registries above
        # no longer guarantees CPython abandons a still-running worker on its own.
        deadline = time.monotonic() + WORKER_JOIN_GRACE_PERIOD
        for thread in self._thread_pool._threads:
            thread.join(timeout=max(0, deadline - time.monotonic()))
        stuck_workers = [thread for thread in self._thread_pool._threads if thread.is_alive()]
        if stuck_workers:
            request_hard_exit(
                f"ParallelObject worker thread(s) still alive after shutdown: "
                f"{[thread.name for thread in stuck_workers]}",
                stuck_workers,
            )

    @staticmethod
    def run_named_tasks_in_parallel(
        tasks: dict[str, Callable], timeout: int, ignore_exceptions: bool = False
    ) -> dict[str, ParallelObjectResult]:
        """
        Allows calling multiple Callables in parallel using Parallel
        Object. Returns a dict with the results. Will raise an exception
        if:
        - ignore_exceptions is set to False and an exception was raised
        during execution
        - timeout is set and timeout was reached

        Example:

        Given:
        tasks = {
            "trigger": partial(time.sleep, 10))
            "interrupt": partial(random.random)
        }

        Result:

        {
            "trigger": ParallelObjectResult >>> time.sleep result
            "interrupt": ParallelObjectResult >>> random.random result
        }
        """
        task_id_map = {str(id(task)): task_name for task_name, task in tasks.items()}
        results_map = {}

        task_results = ParallelObject(objects=tasks.values(), timeout=timeout if timeout else None).call_objects(
            ignore_exceptions=ignore_exceptions
        )

        for result in task_results:
            task_name = task_id_map.get(str(id(result.obj)))
            results_map.update({task_name: result})

        return results_map


class ParallelObjectResult:
    """Object for result of future in ParallelObject

    Return as a result of ParallelObject.run method
    and contain result of disrupt_func was run in parallel
    and exception if it happened during run.
    """

    def __init__(self, obj, result=None, exc=None):
        self.obj = obj
        self.result = result
        self.exc = exc


class ParallelObjectException(Exception):
    def __init__(self, results: List[ParallelObjectResult]):
        super().__init__()
        self.results = results

    def __str__(self):
        ex_str = ""
        for res in self.results:
            if res.exc:
                ex_str += (
                    f"{res.obj}:\n {''.join(traceback.format_exception(type(res.exc), res.exc, res.exc.__traceback__))}"
                )
        return ex_str
