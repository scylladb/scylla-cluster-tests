import concurrent.futures
import logging
import random
import threading
import time

import pytest

from sdcm.exceptions import KillNemesis
from sdcm.utils.common import raise_exception_in_thread
from sdcm.utils.parallel_object import ParallelObject, ParallelObjectException

LOGGER = logging.getLogger(name=__name__)

MAX_TIMEOUT = 0.3
RAND_TIMEOUTS = random.sample([0.2, 0.3, 0.4], 3)
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
    steps = 3
    raise_after = random.randint(1, steps)
    step_duration = timeout / steps
    for step in range(1, steps + 1):
        time.sleep(step_duration)
        if step == raise_after:
            raise DummyException()
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
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 0.5, num_workers=len(RAND_TIMEOUTS))
    results = parallel_object.run(dummy_func_return_tuple)
    returned_results = [r.result for r in results]
    expected_results = [(timeout, "test") for timeout in RAND_TIMEOUTS]
    assert returned_results == expected_results


def test_successful_parallel_run_func_returning_single_value():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 0.5)
    results = parallel_object.run(dummy_func_return_single)
    returned_results = [r.result for r in results]
    assert returned_results == RAND_TIMEOUTS


def test_raised_exception_by_timeout():
    test_timeout = min(RAND_TIMEOUTS)
    start_time = time.time()
    with pytest.raises(ParallelObjectException) as exc_info:
        parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=test_timeout * 0.5, num_workers=len(RAND_TIMEOUTS))
        parallel_object.run(dummy_func_return_tuple)
    assert any(isinstance(e.exc, concurrent.futures.TimeoutError) for e in exc_info.value.results)
    run_time = time.time() - start_time
    assert float(test_timeout) == pytest.approx(run_time, rel=1.0e02)


def test_parallel_object_exception_raised():
    with pytest.raises(ParallelObjectException):
        parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 0.5)
        parallel_object.run(dummy_func_raising_exception)


def test_ignore_exception_raised_in_func_and_get_results():
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 0.5)
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
    parallel_object = ParallelObject(RAND_TIMEOUTS, timeout=MAX_TIMEOUT + 0.5, num_workers=2)
    results = parallel_object.run(dummy_func_return_tuple)
    returned_results = [r.result for r in results]
    expected_results = [(timeout, "test") for timeout in RAND_TIMEOUTS]
    assert returned_results == expected_results


def test_unpack_args_for_func():
    parallel_object = ParallelObject(UNPACKING_ARGS, timeout=MAX_TIMEOUT + 0.5, num_workers=2)
    results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [tuple(item) for item in UNPACKING_ARGS]
    assert returned_results == expected_results


def test_unpack_kwargs_for_func():
    parallel_object = ParallelObject(UNPACKING_KWARGS, timeout=MAX_TIMEOUT + 0.5, num_workers=2)
    results = parallel_object.run(dummy_func_with_several_parameters, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [(d["timeout"], d["msg"]) for d in UNPACKING_KWARGS]
    assert returned_results == expected_results


def test_successfull_parallel_run_func_accepted_list_as_parameter():
    parallel_object = ParallelObject(LIST_AS_ARG, timeout=MAX_TIMEOUT + 0.5)
    results = parallel_object.run(dummy_func_accepts_list_as_parameter, unpack_objects=True)
    returned_results = [r.result for r in results]
    expected_results = [r[0][1] for r in LIST_AS_ARG]
    assert returned_results == expected_results


def test_wait_without_timeout_gets_kill_nemesis(monkeypatch):
    """stop_nemesis() raises KillNemesis in the nemesis thread, so a wait without timeout must not hold it until the objects end."""
    monkeypatch.setattr("sdcm.utils.parallel_object.WAIT_STEP", 0.1)
    started, release = threading.Event(), threading.Event()
    killed_before_release = []

    def wait_for_object(_):
        started.set()
        release.wait(30)

    def run_objects():
        try:
            ParallelObject([1], timeout=None).run(wait_for_object)
        except KillNemesis:
            killed_before_release.append(not release.is_set())

    waiter = threading.Thread(target=run_objects)
    waiter.start()
    started.wait(5)
    raise_exception_in_thread(waiter, KillNemesis)
    waiter.join(2)
    still_waiting = waiter.is_alive()
    release.set()
    waiter.join()

    assert not still_waiting
    assert killed_before_release == [True]


def test_wait_in_steps_keeps_the_timeout(monkeypatch):
    """Waiting in steps shorter than the timeout still waits for the whole timeout before failing the object."""
    monkeypatch.setattr("sdcm.utils.parallel_object.WAIT_STEP", 0.05)
    start = time.monotonic()

    results = ParallelObject([1], timeout=0.3).run(time.sleep, ignore_exceptions=True)

    assert isinstance(results[0].exc, concurrent.futures.TimeoutError)
    assert time.monotonic() - start >= 0.3


def raise_timeout_error(_):
    raise TimeoutError("ssh")


def test_timeout_error_raised_by_the_object_is_its_result():
    """A TimeoutError raised by the object is its error, not a wait that ran out, so it fails the run at once."""
    start = time.monotonic()

    with pytest.raises(ParallelObjectException, match="ssh"):
        ParallelObject([1], timeout=0.3).run(raise_timeout_error)

    assert time.monotonic() - start < 0.3
