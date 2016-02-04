"""
Wait functions appropriate for tests that have high timing variance.
"""
import time
import logging

LOGGER = logging.getLogger('avocado.test')


def wait_for(func, step=1, text=None, *args, **kwargs):
    """
    Wait indefinitely until func evaluates to True.

    This is similar to avocado.utils.wait.wait(), but there's no
    timeout, we'll just keep waiting for it.

    :param func: Function to evaluate.
    :param step: Amount of time to sleep before another try.
    :param text: Text to log, for debugging purposes.
    :param args: Positional arguments to func
    :param kwargs: Keyword arguments to func
    :return: Return value of func.
    """
    ok = False
    start_time = time.time()
    while not ok:
        ok = func(*args, **kwargs)
        time.sleep(step)
        time_elapsed = time.time() - start_time
        if text is not None:
            LOGGER.debug('%s (%s s)', text, time_elapsed)
    return ok
