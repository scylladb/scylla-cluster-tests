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
from typing import TypeVar, cast
from collections.abc import Callable

import tenacity


LOGGER = logging.getLogger("sdcm.wait")

R = TypeVar("R")  # pylint: disable=invalid-name


def wait_for(func, step=1, text=None, timeout=None, throw_exc=True, **kwargs):
    """
    Wrapper function to wait with timeout option.

    :param func: Function to evaluate.
    :param step: Time to sleep between attempts in seconds
    :param text: Text to print while waiting, for debug purposes
    :param timeout: Timeout in seconds
    :param throw_exc: Raise exception if timeout expired, but func result is not True
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    if not timeout:
        return forever_wait_for(func, step, text, **kwargs)

    res = None

    def retry_logger(retry_state):
        # pylint: disable=protected-access
        LOGGER.debug(
            'wait_for: Retrying %s: attempt %s ended with: %s',
            text or retry_state.fn.__name__,
            retry_state.attempt_number,
            retry_state.outcome._exception or retry_state.outcome._result,
        )

    try:
        retry = tenacity.Retrying(
            reraise=throw_exc,
            stop=tenacity.stop_after_delay(timeout),
            wait=tenacity.wait_fixed(step),
            before_sleep=retry_logger,
            retry=(tenacity.retry_if_result(lambda value: not value) | tenacity.retry_if_exception_type())
        )
        res = retry(func, **kwargs)

    except Exception as ex:  # pylint: disable=broad-except
        err = f"Wait for: {text or func.__name__}: timeout - {timeout} seconds - expired"
        LOGGER.error(err)
        if hasattr(ex, 'last_attempt') and ex.last_attempt.exception() is not None:  # pylint: disable=no-member
            LOGGER.error("last error: %r", ex.last_attempt.exception())  # pylint: disable=no-member
        else:
            LOGGER.error("last error: %r", ex)
        if throw_exc:
            if hasattr(ex, 'last_attempt') and not ex.last_attempt._result:  # pylint: disable=protected-access,no-member
                raise tenacity.RetryError(err) from ex
            raise

    return res


def forever_wait_for(func, step=1, text=None, **kwargs):
    """
    Wait indefinitely until func evaluates to True.

    This is similar to avocado.utils.wait.wait(), but there's no
    timeout, we'll just keep waiting for it.

    :param func: Function to evaluate.
    :param step: Amount of time to sleep before another try.
    :param text: Text to log, for debugging purposes.
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
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
