from functools import wraps
import os
import traceback


def raise_event_on_failure(func):
    """
    Decorate a function that is running inside a thread,
    when exception is raised in this function,
    will raise an Error severity event
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = None
        _test_pid = os.getpid()
        from sdcm.sct_events import ThreadFailedEvent
        try:
            result = func(*args, **kwargs)
        except Exception as ex:  # pylint: disable=broad-except
            ThreadFailedEvent(message=str(ex), traceback=traceback.format_exc())

        return result
    return wrapper
