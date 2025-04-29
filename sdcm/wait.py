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
# Copyright (c) 2016 ScyllaDB

"""
Wait functions appropriate for tests that have high timing variance.
"""
import time
import logging
from contextlib import contextmanager
from typing import TypeVar, cast
from collections.abc import Callable

import tenacity

from sdcm.exceptions import WaitForTimeoutError, ExitByEventError

LOGGER = logging.getLogger("sdcm.wait")

R = TypeVar("R")


def wait_for(func, step=1, text=None, timeout=None, throw_exc=True, stop_event=None, **kwargs):
    """
    Wrapper function to wait with timeout option.

    :param func: Function to evaluate.
    :param step: Time to sleep between attempts in seconds
    :param text: Text to print while waiting, for debug purposes
    :param timeout: Timeout in seconds
    :param throw_exc: Raise exception if timeout expired, but disrupt_func result is not True
    :param kwargs: Keyword arguments to disrupt_func
    :param stop_event: instance of threading.Event class to stop retrying
    :return: Return value of disrupt_func.
    """
    if not timeout:
        return forever_wait_for(func, step, text, **kwargs)

    res = None

    def retry_logger(retry_state):

        LOGGER.debug(
            'wait_for: Retrying %s: attempt %s ended with: %s',
            text or retry_state.fn.__name__,
            retry_state.attempt_number,
            retry_state.outcome._exception or retry_state.outcome._result,
        )
    stops = [tenacity.stop_after_delay(timeout)]
    if stop_event:
        stops.append(tenacity.stop.stop_when_event_set(stop_event))

    try:
        retry = tenacity.Retrying(
            reraise=throw_exc,
            stop=tenacity.stop_any(*stops),
            wait=tenacity.wait_fixed(step),
            before_sleep=retry_logger,
            retry=(tenacity.retry_if_result(lambda value: not value) | tenacity.retry_if_exception_type())
        )
        res = retry(func, **kwargs)

    except Exception as ex:  # noqa: BLE001
        err = f"Wait for: {text or func.__name__}: timeout - {timeout} seconds - expired"
        raising_exc = WaitForTimeoutError(err)
        if stop_event and stop_event.is_set():
            err = f": {text or func.__name__}: stopped by Event"
            raising_exc = ExitByEventError(err)

        LOGGER.error(err)
        if hasattr(ex, 'last_attempt') and ex.last_attempt.exception() is not None:
            LOGGER.error("last error: %r", ex.last_attempt.exception())
        else:
            LOGGER.error("last error: %r", ex)
        if throw_exc:
            if hasattr(ex, 'last_attempt') and not ex.last_attempt._result:
                raise raising_exc from ex
            raise

    return res


def forever_wait_for(func, step=1, text=None, **kwargs):
    """
    Wait indefinitely until disrupt_func evaluates to True.

    This is similar to avocado.utils.wait.wait(), but there's no
    timeout, we'll just keep waiting for it.

    :param func: Function to evaluate.
    :param step: Amount of time to sleep before another try.
    :param text: Text to log, for debugging purposes.
    :param kwargs: Keyword arguments to disrupt_func
    :return: Return value of disrupt_func.
    """
    ok = False
    start_time = time.time()
    while not ok:
        ok = func(**kwargs)
        time.sleep(step)
        time_elapsed = time.time() - start_time
        if text is not None:
            LOGGER.debug('%s (%s s)', text, time_elapsed)
    return ok


def exponential_retry(func: Callable[[], R],
                      exceptions: tuple[type[BaseException]] | type[BaseException] = Exception,
                      threshold: float = 300,
                      retries: int = 10,
                      logger: logging.Logger | None = LOGGER) -> R:
    """
    Retry using an exponential backoff algorithm, similar to what Amazon recommends for its own service [1].

    :see: [1] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
    """
    def retry_logger(retry_state: tenacity.RetryCallState) -> None:
        logger.error("Call to method %s (retries: %s) failed: %s",
                     retry_state.fn, retry_state.attempt_number, retry_state.outcome.exception())

    retry = tenacity.Retrying(
        stop=tenacity.stop_after_attempt(max_attempt_number=retries),
        wait=tenacity.wait_exponential(multiplier=2, max=threshold),
        retry=tenacity.retry_if_exception_type(exception_types=exceptions),
        before_sleep=retry_logger if logger else None,
    )

    return cast(R, retry(func))


@contextmanager
def wait_for_log_lines(node, start_line_patterns, end_line_patterns, start_timeout=60, end_timeout=120,
                       error_msg_ctx=""):
    """Waits for given lines patterns to appear in node logs despite exception raised"""
    start_ctx = f"Timeout occurred while waiting for start log line {start_line_patterns} on node: {node.name}."
    if error_msg_ctx:
        start_ctx += f" Context: {error_msg_ctx}"
    end_ctx = f"Timeout occurred while waiting for end log line {end_line_patterns} on node: {node.name}"
    if error_msg_ctx:
        end_ctx += f". Context: {error_msg_ctx}"
    start_follower = node.follow_system_log(patterns=start_line_patterns)
    end_follower = node.follow_system_log(patterns=end_line_patterns)
    start_time = time.time()
    try:
        yield
    finally:
        started = any(start_follower)
        while not started and (time.time() - start_time < start_timeout):
            started = any(start_follower)
            time.sleep(0.1)
        if not started:
            raise TimeoutError(start_ctx)
        LOGGER.debug("Start line patterns %s were found.%s", start_line_patterns, error_msg_ctx)
        ended = any(end_follower)
        while not ended and (time.time() - start_time < end_timeout):
            ended = any(end_follower)
            time.sleep(0.1)
        if not ended:
            raise TimeoutError(end_ctx)
        LOGGER.debug("End line patterns %s were found.%s", end_line_patterns, error_msg_ctx)
