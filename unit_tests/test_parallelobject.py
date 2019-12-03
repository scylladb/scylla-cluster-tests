# pylint: disable=invalid-name
import unittest
import time
import logging
import random
import concurrent.futures


from sdcm.utils.common import ParallelObject


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

    rand_timeouts = [10, 15, 20, 25, 5]
    unpacking_args = [
        [10, "test1"],
        [15, "test2"],
        [20, "test3"],
    ]
    list_as_arg = [
        [[10, "test1"]],
        [[15, "test2"]],
        [[20, "test3"]],
    ]
    unpacking_kwargs = [
        {"timeout": 10, "msg": "test1"},
        {"timeout": 15, "msg": "test2"},
        {"timeout": 20, "msg": "test3"},
    ]

    def test_successful_parallel_run_func_returning_tuple(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=30, num_workers=len(self.rand_timeouts))
        results = parallel_object.run(dummy_func_return_tuple)
        returned_results = [r.result for r in results]
        expected_results = [(timeout, 'test') for timeout in self.rand_timeouts]
        self.assertListEqual(returned_results, expected_results)

    def test_successful_parallel_run_func_returning_single_value(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=30, num_workers=len(self.rand_timeouts))
        results = parallel_object.run(dummy_func_return_single)
        returned_results = [r.result for r in results]
        self.assertListEqual(returned_results, self.rand_timeouts)

    def test_raised_exception_by_timeout(self):
        with self.assertRaises(concurrent.futures.TimeoutError):
            parallel_object = ParallelObject(self.rand_timeouts, timeout=10, num_workers=len(self.rand_timeouts))
            parallel_object.run(dummy_func_return_tuple)

    def test_exception_raised_in_thread_by_func(self):
        with self.assertRaises(DummyException):
            parallel_object = ParallelObject(self.rand_timeouts, timeout=30, num_workers=len(self.rand_timeouts))
            parallel_object.run(dummy_func_raising_exception)

    def test_ignore_exception_raised_in_func_and_get_results(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=30, num_workers=len(self.rand_timeouts))
        results = parallel_object.run(dummy_func_raising_exception, ignore_exceptions=True)
        for res_obj in results:
            if res_obj.exc:
                self.assertIsNone(res_obj.result)
                self.assertIsInstance(res_obj.exc, DummyException)
            else:
                self.assertIsNone(res_obj.exc)
                self.assertListEqual(res_obj.result, "done")

    def test_ignore_exception_by_timeout(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=10, num_workers=len(self.rand_timeouts))
        results = parallel_object.run(dummy_func_return_tuple, ignore_exceptions=True)
        for res_obj in results:
            if res_obj.exc:
                self.assertIsNone(res_obj.result)
                self.assertIsInstance(res_obj.exc, concurrent.futures.TimeoutError)
            else:
                self.assertIsNone(res_obj.exc)
                self.assertIn(res_obj.result, [(timeout, 'test') for timeout in self.rand_timeouts])

    def test_less_number_of_workers_than_length_of_iterable(self):
        parallel_object = ParallelObject(self.rand_timeouts, timeout=30, num_workers=2)
        results = parallel_object.run(dummy_func_return_tuple)
        returned_results = [r.result for r in results]
        expected_results = [(timeout, 'test') for timeout in self.rand_timeouts]
        self.assertListEqual(returned_results, expected_results)

    def test_unpack_args_for_func(self):
        parallel_object = ParallelObject(self.unpacking_args, timeout=30, num_workers=2)
        results = parallel_object.run(dummy_func_with_several_parameters)
        returned_results = [r.result for r in results]
        expected_results = [(timeout, msg)
                            for timeout, msg in self.unpacking_args]
        self.assertListEqual(returned_results, expected_results)

    def test_unpack_kwargs_for_func(self):
        parallel_object = ParallelObject(self.unpacking_kwargs, timeout=30, num_workers=2)
        results = parallel_object.run(dummy_func_with_several_parameters)
        returned_results = [r.result for r in results]
        expected_results = [(d["timeout"], d["msg"]) for d in self.unpacking_kwargs]
        self.assertListEqual(returned_results, expected_results)

    def test_successfull_parallel_run_func_accepted_list_as_parameter(self):
        parallel_object = ParallelObject(self.list_as_arg, timeout=30, num_workers=len(self.list_as_arg))
        results = parallel_object.run(dummy_func_accepts_list_as_parameter)
        returned_results = [r.result for r in results]
        expected_results = [r[0][1] for r in self.list_as_arg]
        self.assertListEqual(returned_results, expected_results)
