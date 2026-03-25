import time
import logging
import random
import concurrent.futures

import pytest

from sdcm.utils.parallel_object import ParallelObject, ParallelObjectException

LOGGER = logging.getLogger(name=__name__)

MAX_TIMEOUT = 3
RAND_TIMEOUTS = random.sample(range(2, MAX_TIMEOUT + 2), MAX_TIMEOUT)
UNPACKING_ARGS = [[t, f"test{i}"] for i, t in enumerate(RAND_TIMEOUTS)]
LIST_AS_ARG = [[[t, f"test{i}"]] for i, t in enumerate(RAND_TIMEOUTS)]
UNPACKING_KWARGS = [{"timeout": t, "msg": f"test{i}"} for i, t in enumerate(RAND_TIMEOUTS)]


class DummyException(Exception):
    pass


def dummy_func_return_tuple(timeout):
    LOGGER.debug("start %s", dummy_func_return_tuple.__name__)
    time.sleep(timeout)
    LOGGER.debug("finished %s", dummy_func_return_tuple.__name__)
    return (timeout, "test")


def dummy_func_return_single(timeout):
    LOGGER.debug("start %s", dummy_func_return_tuple.__name__)
    time.sleep(timeout)
    LOGGER.debug("finished %s", dummy_func_return_tuple.__name__)
    return timeout


def dummy_func_raising_exception(timeout):
    LOGGER.debug("start %s", dummy_func_raising_exception.__name__)
    raise_after = random.randint(1, timeout)
    while timeout > 0:
        time.sleep(1)
        if timeout == raise_after:
            raise DummyException()
        timeout -= 1
    LOGGER.debug("finished %s", dummy_func_raising_exception.__name__)
    return "Done"


def dummy_func_accepts_list_as_parameter(accepted_list):
    LOGGER.debug("start %s", dummy_func_return_tuple.__name__)
    time.sleep(accepted_list[0])
    LOGGER.debug("finished %s", dummy_func_return_tuple.__name__)
    return accepted_list[1]


def dummy_func_with_several_parameters(timeout, msg):
    LOGGER.debug("start %s with timeout %s", msg, timeout)
    time.sleep(timeout)
    LOGGER.info("finished %s", dummy_func_return_tuple.__name__)
    return (timeout, msg)


def test_successful_parallel_run_func_returning_tuple():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 2, num_workers=len(RAND_TIMEOUTS))
    results = parallel_object.run(dummy_func_return_tuple)
    returned_results = [r.result for r in results]
    expected_results = [(timeout, "test") for timeout in RAND_TIMEOUTS]
    assert returned_results == expected_results


def test_successful_parallel_run_func_returning_single_value():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 2)
    results = parallel_object.run(dummy_func_return_single)
    returned_results = [r.result for r in results]
    assert returned_results == RAND_TIMEOUTS


def test_raised_exception_by_timeout():
    test_timeout = min(RAND_TIMEOUTS)
    start_time = time.time()
    with pytest.raises(ParallelObjectException) as exc_info:
        parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=test_timeout - 1, num_workers=len(RAND_TIMEOUTS))
        parallel_object.run(dummy_func_return_tuple)
    assert any(isinstance(e.exc, concurrent.futures.TimeoutError) for e in exc_info.value.results)
    run_time = time.time() - start_time
    assert float(test_timeout) == pytest.approx(run_time, rel=1.0e02)


def test_parallel_object_exception_raised():
    with pytest.raises(ParallelObjectException):
        parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 2)
        parallel_object.run(dummy_func_raising_exception)


def test_ignore_exception_raised_in_func_and_get_results():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 2)
    results = parallel_object.run(dummy_func_raising_exception, ignore_exceptions=True)
    for res_obj in results:
        assert res_obj.obj is not None
        if res_obj.exc:
            assert res_obj.result is None
            assert isinstance(res_obj.exc, DummyException)
        else:
            assert res_obj.exc is None
            assert res_obj.result == "done"


def test_ignore_exception_by_timeout():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=min(RAND_TIMEOUTS))
    results = parallel_object.run(dummy_func_return_tuple, ignore_exceptions=True)
    for res_obj in results:
        if res_obj.exc:
            assert res_obj.result is None
            assert isinstance(res_obj.exc, concurrent.futures.TimeoutError)
        else:
            assert res_obj.exc is None
            assert res_obj.result in [(timeout, "test") for timeout in RAND_TIMEOUTS]


def test_less_number_of_workers_than_length_of_iterable():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 2, num_workers=2)
    results = parallel_object.run(dummy_func_return_tuple)
    returned_results = [r.result for r in results]
    expected_results = [(timeout, "test") for timeout in RAND_TIMEOUTS]
    assert returned_results == expected_results


def test_unpack_args_for_func():
    parallel_object = ParallelObject(UNPACKING_ARGS, timeout=MAX_TIMEOUT + 2, num_workers=2)
    results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [tuple(item) for item in UNPACKING_ARGS]
    assert returned_results == expected_results


def test_unpack_kwargs_for_func():
    parallel_object = ParallelObject(UNPACKING_KWARGS, timeout=MAX_TIMEOUT + 2, num_workers=2)
    results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [(d["timeout"], d["msg"]) for d in UNPACKING_KWARGS]
    assert returned_results == expected_results


def test_successfull_parallel_run_func_accepted_list_as_parameter():
    parallel_object = ParallelObject(LIST_AS_ARG, timeout=MAX_TIMEOUT + 2)
    results = parallel_object.run(dummy_func_accepts_list_as_parameter, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [r[0][1] for r in LIST_AS_ARG]
    assert returned_results == expected_results
