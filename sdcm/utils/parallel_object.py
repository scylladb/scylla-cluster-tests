from __future__ import absolute_import, annotations

import logging
import threading
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
        # if there are futures that didn't run  we cancel them
        for future, _ in futures:
            future.cancel()
        self._thread_pool.shutdown(wait=False)

        # Since CPython bpo-39812 (3.9+), ThreadPoolExecutor worker threads are
        # non-daemon and `daemon` cannot be flipped after start, so they used to be
        # joined twice at interpreter shutdown with no timeout: once via
        # concurrent.futures.thread._python_exit() (registered through the
        # CPython-internal threading._register_atexit(), which is why calling
        # the public atexit.unregister(_python_exit) here never actually removed
        # it) and again via threading._shutdown_locks (Python <3.14; on 3.14+ that
        # second join moved into the C-level _thread._shutdown(), which has no
        # Python-level bypass -- os._exit() in sdcm.utils.hard_exit is what
        # actually protects us there). Removing this pool's worker threads from
        # both registries -- where they still exist -- is the only way to let
        # CPython abandon them instead of joining them forever. This is safe here
        # because clean_up() runs only after every result has already been
        # collected or timed out and the pool has already been told to shut down
        # above, so any still-running worker at this point is already an
        # abandoned zombie as far as this code is concerned -- we are just making
        # CPython agree.
        # `_threads_queues` (and, transitively, `threading._shutdown_locks`) must be
        # mutated while holding `_global_shutdown_lock`: CPython uses that lock
        # internally to serialize worker registration against `_python_exit()`, which
        # copies this same WeakKeyDictionary during interpreter shutdown. Racing that
        # copy unlocked can corrupt shutdown-time iteration.
        #
        # Deadlock consideration: CPython's own `_python_exit()` only holds this lock
        # briefly, to flip an internal `_shutdown` flag, and releases it *before*
        # copying `_threads_queues` and unboundedly joining every worker in it -- the
        # lock is not held during that join. So a thread stuck in that join (the exact
        # crisis this module exists to escalate out of) does not hold this lock while
        # stuck, and acquiring it here should not be able to deadlock against it. That
        # said, this is defensive code whose whole point is to never itself become
        # another way to hang, so we still bound the wait rather than trust that
        # invariant forever (e.g. across CPython versions): if the lock cannot be
        # acquired promptly, log a warning and proceed without it.
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

        # On Python 3.14+, `threading._shutdown()` delegates to the C-level
        # `_thread._shutdown()`, which waits on its own internal `shutdown_handles`
        # list of every non-daemon ThreadPoolExecutor worker -- a list this module has
        # no Python-level access to. Popping the registries above therefore no longer
        # guarantees a still-running worker gets abandoned at interpreter shutdown: if
        # one is still alive here (shutdown(wait=False) above didn't let it finish),
        # arm the same deferred hard-exit escalation `stop_nemesis` uses, so the
        # eventual real exit point force-terminates instead of hanging.
        #
        # shutdown(wait=False) only queues a sentinel and returns immediately -- it does
        # not wait for a worker to notice it, finish its current unit of work and exit.
        # Checking is_alive() right away would treat a perfectly healthy worker that
        # simply hasn't unwound yet as "stuck", arming a hard exit on essentially every
        # clean_up() call. Mirror the join(timeout)-then-check pattern `stop_nemesis`
        # (sdcm/cluster.py) uses: give each worker a real, bounded grace period to
        # actually finish before deciding it is stuck.
        for thread in self._thread_pool._threads:
            thread.join(timeout=WORKER_JOIN_GRACE_PERIOD)
        stuck_workers = [thread for thread in self._thread_pool._threads if thread.is_alive()]
        if stuck_workers:
            request_hard_exit(
                f"ParallelObject worker thread(s) still alive after shutdown: "
                f"{[thread.name for thread in stuck_workers]}"
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
