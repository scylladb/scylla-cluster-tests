import time
import logging
import random
import threading
import concurrent.futures
from concurrent.futures.thread import _threads_queues
from unittest.mock import MagicMock, Mock

import pytest

import sdcm.utils.parallel_object as parallel_object_module
from sdcm.utils import hard_exit
from sdcm.utils.parallel_object import ParallelObject, ParallelObjectException

LOGGER = logging.getLogger(name=__name__)


@pytest.fixture(autouse=True)
def _reset_hard_exit_state(monkeypatch):
    """Ensure `_hard_exit_reason` never leaks between tests (clean_up() can arm it)."""
    monkeypatch.setattr(hard_exit, "_hard_exit_reason", None)


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


def test_clean_up_detaches_stuck_worker_from_interpreter_shutdown():
    """A worker stuck past its timeout must be removed from the CPython
    registries `_python_exit()`/`threading._shutdown()` join against, so
    interpreter shutdown doesn't hang on it forever.

    `threading._shutdown_locks` (and `Thread._tstate_lock`) only exist on
    Python <3.14 -- on 3.14 that second join moved into the C-level
    `_thread._shutdown()`, which has no Python-level bypass, so that half of
    the assertion is version-gated.
    """
    block_forever = threading.Event()
    started_threads = []

    def stuck_task():
        started_threads.append(threading.current_thread())
        block_forever.wait()

    try:
        parallel_object = ParallelObject(objects=[stuck_task], timeout=0.1, num_workers=1)
        # clean_up() runs automatically at the end of run() (invoked via call_objects())
        parallel_object.call_objects(ignore_exceptions=True)

        assert len(started_threads) == 1
        worker_thread = started_threads[0]

        assert worker_thread not in _threads_queues

        shutdown_locks = getattr(threading, "_shutdown_locks", None)
        tstate_lock = getattr(worker_thread, "_tstate_lock", None)
        if shutdown_locks is not None and tstate_lock is not None:
            assert tstate_lock not in shutdown_locks

        # The worker is still alive at this point (blocked on block_forever), so on
        # Python 3.14+ the registry-detachment trick above is not enough by itself
        # (see module docstring): clean_up() must have armed the hard exit too.
        assert hard_exit._hard_exit_reason
        assert "ParallelObject" in hard_exit._hard_exit_reason
    finally:
        block_forever.set()
        for thread in started_threads:
            thread.join(timeout=5)


def test_clean_up_arms_hard_exit_when_worker_still_alive_after_shutdown():
    """clean_up() must arm the hard exit if a pool worker is still alive after
    `shutdown(wait=False)`, regardless of CPython version: the registry-pop trick
    alone cannot detach a worker from interpreter shutdown on Python 3.14+."""
    parallel_object = ParallelObject(objects=[], timeout=1)
    stuck_worker = Mock()
    stuck_worker.is_alive.return_value = True
    parallel_object._thread_pool._threads = {stuck_worker}

    parallel_object.clean_up(futures=[])

    assert hard_exit._hard_exit_reason
    assert "ParallelObject" in hard_exit._hard_exit_reason


def test_clean_up_does_not_arm_hard_exit_when_no_worker_alive():
    """No false-positive escalation: if every worker has already finished,
    clean_up() must not arm the hard exit."""
    parallel_object = ParallelObject(objects=[], timeout=1)
    finished_worker = Mock()
    finished_worker.is_alive.return_value = False
    parallel_object._thread_pool._threads = {finished_worker}

    parallel_object.clean_up(futures=[])

    assert not hard_exit._hard_exit_reason


def test_clean_up_mutates_registries_under_global_shutdown_lock(monkeypatch):
    """`_threads_queues`/`threading._shutdown_locks` mutations must happen while
    holding `_global_shutdown_lock`, since CPython uses that same lock internally
    to serialize worker registration against `_python_exit()`."""
    mock_lock = MagicMock()
    monkeypatch.setattr(parallel_object_module, "_global_shutdown_lock", mock_lock)

    parallel_object = ParallelObject(objects=[], timeout=1)
    parallel_object.clean_up(futures=[])

    mock_lock.__enter__.assert_called_once()
    mock_lock.__exit__.assert_called_once()
