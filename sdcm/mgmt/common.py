import logging
import datetime

from enum import Enum


DEFAULT_TASK_TIMEOUT = 7200  # 2 hours
LOGGER = logging.getLogger(__name__)


def duration_to_timedelta(duration_string):
    total_seconds = 0
    if "d" in duration_string:
        total_seconds += int(duration_string[:duration_string.find('d')]) * 86400
        duration_string = duration_string[duration_string.find('d') + 1:]
    if "h" in duration_string:
        total_seconds += int(duration_string[:duration_string.find('h')]) * 3600
        duration_string = duration_string[duration_string.find('h') + 1:]
    if "m" in duration_string:
        total_seconds += int(duration_string[:duration_string.find('m')]) * 60
        duration_string = duration_string[duration_string.find('m') + 1:]
    if "s" in duration_string:
        total_seconds += int(duration_string[:duration_string.find('s')])
    return datetime.timedelta(seconds=total_seconds)


class ScyllaManagerError(Exception):
    """
    A custom exception for Manager related errors
    """


class HostSsl(Enum):
    ON = "ON"
    OFF = "OFF"

    @classmethod
    def from_str(cls, output_str):
        if "SSL" in output_str:
            return HostSsl.ON
        return HostSsl.OFF


class HostStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned host status: {}".format(output_str)) from err


class HostRestStatus(Enum):
    UP = "UP"
    DOWN = "DOWN"
    TIMEOUT = "TIMEOUT"
    UNAUTHORIZED = "UNAUTHORIZED"
    HTTP = "HTTP"

    @classmethod
    def from_str(cls, output_str):
        try:
            output_str = output_str.upper()
            if output_str == "-":
                return cls.DOWN
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned host rest status: {}".format(output_str)) from err


class TaskStatus:  # pylint: disable=too-few-public-methods
    NEW = "NEW"
    RUNNING = "RUNNING"
    DONE = "DONE"
    UNKNOWN = "UNKNOWN"
    ERROR = "ERROR"
    ERROR_FINAL = "ERROR (4/4)"
    STOPPED = "STOPPED"
    STARTING = "STARTING"
    ABORTED = "ABORTED"

    @classmethod
    def from_str(cls, output_str) -> str:
        try:
            output_str = output_str.upper()
            return getattr(cls, output_str)
        except AttributeError as err:
            raise ScyllaManagerError("Could not recognize returned task status: {}".format(output_str)) from err
