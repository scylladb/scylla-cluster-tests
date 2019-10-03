import sys
from functools import wraps
import os
import signal


def background_failure_handler(signum, frame):  # pylint: disable=unused-argument
    raise Exception("Failure/Error in background threads")


signal.signal(signal.SIGUSR2, background_failure_handler)


def test_failing_function(func):
    """
    Decorate a function that is running inside a thread,
    when exception is raised in this function,
    will cause the test to fail by sending a SIGUSR2 signal
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = None
        _test_pid = os.getpid()
        from sdcm.cluster import Setup
        try:
            result = func(*args, **kwargs)
        except AssertionError:
            Setup.tester_obj().result.addFailure(Setup.tester_obj(), sys.exc_info())
            os.kill(_test_pid, signal.SIGUSR2)
        except Exception:  # pylint: disable=broad-except
            Setup.tester_obj().result.addError(Setup.tester_obj(), sys.exc_info())
            os.kill(_test_pid, signal.SIGUSR2)
        return result
    return wrapper
