import unittest
import time
import logging
import random
import concurrent.futures

import pytest

from sdcm.utils.parallel_object import ParallelObject, ParallelObjectException

LOGGER = logging.getLogger(name=__name__)


class DummyException(Exception):
    pass


def dummy_func_return_tuple(timeout):
    LOGGER.debug('start %s', dummy_func_return_tuple.__name__)
    time.sleep(timeout)
    LOGGER.debug('finished %s', dummy_func_return_tuple.__name__)
    return (timeout, 'test')


def dummy_func_return_single(timeout):
    LOGGER.debug('start %s', dummy_func_return_tuple.__name__)
    time.sleep(timeout)
    LOGGER.debug('finished %s', dummy_func_return_tuple.__name__)
    return timeout


def dummy_func_raising_exception(timeout):
    LOGGER.debug('start %s', dummy_func_raising_exception.__name__)
    raise_after = random.randint(1, timeout)
    while timeout > 0:
        time.sleep(1)
        if timeout == raise_after:
            raise DummyException()
        timeout -= 1
    LOGGER.debug('finished %s', dummy_func_raising_exception.__name__)
    return 'Done'


def dummy_func_accepts_list_as_parameter(accepted_list):
    LOGGER.debug('start %s', dummy_func_return_tuple.__name__)
    time.sleep(accepted_list[0])
    LOGGER.debug('finished %s', dummy_func_return_tuple.__name__)
    return accepted_list[1]


def dummy_func_with_several_parameters(timeout, msg):
    LOGGER.debug('start %s with timeout %s', msg, timeout)
    time.sleep(timeout)
    LOGGER.info('finished %s', dummy_func_return_tuple.__name__)
    return (timeout, msg)


class ParallelObjectTester(unittest.TestCase):
    max_timout = 3
    rand_timeouts = random.sample(range(2, max_timout + 2), max_timout)
    unpacking_args = [[t, f"test{i}"] for i, t in enumerate(rand_timeouts)]
    list_as_arg = [[[t, f"test{i}"]] for i, t in enumerate(rand_timeouts)]
    unpacking_kwargs = [{"timeout": t, "msg": f"test{i}"} for i, t in enumerate(rand_timeouts)]

    def test_successful_parallel_run_func_returning_tuple(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=self.max_timout + 2,
                                         num_workers=len(self.rand_timeouts))
        results = parallel_object.run(dummy_func_return_tuple)
        returned_results = [r.result for r in results]
        expected_results = [(timeout, 'test') for timeout in self.rand_timeouts]
        self.assertListEqual(returned_results, expected_results)

    def test_successful_parallel_run_func_returning_single_value(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=self.max_timout + 2)
        results = parallel_object.run(dummy_func_return_single)
        returned_results = [r.result for r in results]
        self.assertListEqual(returned_results, self.rand_timeouts)

    def test_raised_exception_by_timeout(self):
        test_timeout = min(self.rand_timeouts)
        start_time = time.time()
        with self.assertRaises(ParallelObjectException) as exp:
            parallel_object = ParallelObject(self.rand_timeouts, timeout=test_timeout - 1,
                                             num_workers=len(self.rand_timeouts))
            parallel_object.run(dummy_func_return_tuple)
        assert any(isinstance(e.exc, concurrent.futures.TimeoutError) for e in exp.exception.results)
        run_time = time.time() - start_time
        assert float(test_timeout) == pytest.approx(run_time, rel=1.0e+02)

    def test_parallel_object_exception_raised(self):
        with self.assertRaises(ParallelObjectException):
            parallel_object = ParallelObject(self.rand_timeouts, timeout=self.max_timout + 2)
            parallel_object.run(dummy_func_raising_exception)

    def test_ignore_exception_raised_in_func_and_get_results(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=self.max_timout + 2)
        results = parallel_object.run(dummy_func_raising_exception, ignore_exceptions=True)
        for res_obj in results:
            self.assertIsNotNone(res_obj.obj)
            if res_obj.exc:
                self.assertIsNone(res_obj.result)
                self.assertIsInstance(res_obj.exc, DummyException)
            else:
                self.assertIsNone(res_obj.exc)
                self.assertEqual(res_obj.result, "done")

    def test_ignore_exception_by_timeout(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=min(self.rand_timeouts))
        results = parallel_object.run(dummy_func_return_tuple, ignore_exceptions=True)
        for res_obj in results:
            if res_obj.exc:
                self.assertIsNone(res_obj.result)
                self.assertIsInstance(res_obj.exc, concurrent.futures.TimeoutError)
            else:
                self.assertIsNone(res_obj.exc)
                self.assertIn(res_obj.result, [(timeout, 'test') for timeout in self.rand_timeouts])

    def test_less_number_of_workers_than_length_of_iterable(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=self.max_timout + 2, num_workers=2)
        results = parallel_object.run(dummy_func_return_tuple)
        returned_results = [r.result for r in results]
        expected_results = [(timeout, 'test') for timeout in self.rand_timeouts]
        self.assertListEqual(returned_results, expected_results)

    def test_unpack_args_for_func(self):
        parallel_object = ParallelObject(self.unpacking_args, timeout=self.max_timout + 2, num_workers=2)
        results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
        returned_results = [r.result for r in results]
        expected_results = [tuple(item) for item in self.unpacking_args]
        self.assertListEqual(returned_results, expected_results)

    def test_unpack_kwargs_for_func(self):
        parallel_object = ParallelObject(self.unpacking_kwargs, timeout=self.max_timout + 2, num_workers=2)
        results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
        returned_results = [r.result for r in results]
        expected_results = [(d["timeout"], d["msg"]) for d in self.unpacking_kwargs]
        self.assertListEqual(returned_results, expected_results)

    def test_successfull_parallel_run_func_accepted_list_as_parameter(self):
        parallel_object = ParallelObject(self.list_as_arg, timeout=self.max_timout + 2)
        results = parallel_object.run(dummy_func_accepts_list_as_parameter, unpack_objects=True)
        returned_results = [r.result for r in results]
        expected_results = [r[0][1] for r in self.list_as_arg]
        self.assertListEqual(returned_results, expected_results)
