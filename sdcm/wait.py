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
from avocado.utils import wait

LOGGER = logging.getLogger('sdcm.wait')


class WaitTimeoutExpired(Exception):
    pass


def wait_for(func, step=1, text=None, timeout=None, throw_exc=False, **kwargs):
    """
    Wrapper function to wait with timeout option.
    If timeout received, avocado 'wait_for' method will be used.
    Otherwise the below function will be called.

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
    if kwargs:
        def func_wrap():
            return func(**kwargs)
        res = wait.wait_for(func_wrap, timeout=timeout, step=step, text=text)
    else:
        res = wait.wait_for(func, timeout=timeout, step=step, text=text)
    if res is not True:
        err = 'Wait for: {}: timeout - {} seconds - expired'.format(text, timeout)
        LOGGER.warning(err)
        if throw_exc:
            raise WaitTimeoutExpired(err)
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
