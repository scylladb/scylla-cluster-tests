from __future__ import absolute_import, annotations

import atexit
import logging
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from concurrent.futures.thread import _python_exit
from functools import wraps
from typing import Iterable, Callable, List

LOGGER = logging.getLogger('utils')


class ParallelObject:
    """
        Run function in with supplied args in parallel using thread.
    """

    def __init__(self, objects: Iterable, timeout: int = 6,
                 num_workers: int = None, disable_logging: bool = False):
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
                LOGGER.debug("[{thread_name}] {fun_name}({fun_args}, {fun_kwargs})".format(thread_name=thread_name,
                                                                                           fun_name=fun_name,
                                                                                           fun_args=fun_args,
                                                                                           fun_kwargs=fun_kwargs))
                return_val = fun(*args, **kwargs)
                LOGGER.debug("[{thread_name}] Done.".format(thread_name=thread_name))
                return return_val

            return inner

        results = []

        if not self.disable_logging:
            LOGGER.debug("Executing in parallel: '{}' on {}".format(func.__name__, self.objects))
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
        # we need to unregister internal function that waits for all threads to finish when interpreter exits
        atexit.unregister(_python_exit)

    @staticmethod
    def run_named_tasks_in_parallel(tasks: dict[str, Callable],
                                    timeout: int,
                                    ignore_exceptions: bool = False) -> dict[str, ParallelObjectResult]:
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

        task_results = ParallelObject(
            objects=tasks.values(),
            timeout=timeout if timeout else None
        ).call_objects(ignore_exceptions=ignore_exceptions)

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
                ex_str += f"{res.obj}:\n {''.join(traceback.format_exception(type(res.exc), res.exc, res.exc.__traceback__))}"
        return ex_str
