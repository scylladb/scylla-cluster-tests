import sys
import logging
import logging.config
import warnings
from datetime import datetime

import urllib3
import json

LOGGER = logging.getLogger(__name__)


def setup_stdout_logger(log_level=logging.INFO):
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(logging.StreamHandler(sys.stdout))
    return root_logger


def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    LOGGER.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


class MultilineMessagesFormatter(logging.Formatter):

    def format(self, record):
        """
        This is mostly the same as logging.Formatter.format except for the splitlines() thing.
        This is done so (copied the code) to not make logging a bottleneck. It's not lots of code
        after all, and it's pretty straightforward.
        """
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        if '\n' in record.message:
            splitted = record.message.splitlines()
            output = self._fmt % dict(record.__dict__, message=splitted.pop(0))
            output += '\n'
            output += '\n'.join(
                self._fmt % dict(record.__dict__, message=line)
                for line in splitted
            )
        else:
            output = self._fmt % record.__dict__

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            output += ' ' + self._fmt % record.__dict__ + '\n'
            try:
                output += '\n'.join(
                    self._fmt % dict(record.__dict__, message=line)
                    for index, line in enumerate(record.exc_text.splitlines())
                )
            except UnicodeError:
                output += '\n'.join(
                    self._fmt % dict(record.__dict__, message=line)
                    for index, line
                    in enumerate(record.exc_text.decode(sys.getfilesystemencoding(), 'replace').splitlines())
                )
        return output


class JSONLFormatter(logging.Formatter):
    """
    A custom logging formatter that outputs log records in JSON Lines (JSONL) format.

    Attributes added to the log record:
    - datetime: The current UTC timestamp in ISO 8601 format with a 'Z' suffix.
    - status: The log level name in lowercase.
    - source: Optional source of the log record.
    - action: Optional action associated with the log record.
    - target: Optional target information, if available.
    - trace_id: Optional trace ID for correlation, if available.
    - metadata: Optional metadata dictionary, if available.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "datetime": datetime.utcnow().isoformat() + "Z",
            "status": record.levelname.lower(),
            "source": getattr(record, "source", ""),
            "action": getattr(record, "action", ""),
        }
        if target := getattr(record, "target", None):
            log_entry["target"] = target
        if trace_id := getattr(record, "trace_id", None):
            log_entry["trace_id"] = trace_id
        if metadata := getattr(record, "metadata", None):
            log_entry["metadata"] = metadata
        return json.dumps(log_entry, ensure_ascii=False)


class FilterRemote(logging.Filter):
    def filter(self, record):
        return not record.name == 'sdcm.remote'


def replace_vars(obj, variables, obj_type=None):
    if variables is None:
        return obj
    if obj_type is None:
        obj_type = type(obj)
    if issubclass(obj_type, dict):
        output = {}
        for attr_name, attr_value in obj.items():
            attr_name = replace_vars(attr_name, variables)  # noqa: PLW2901
            attr_value = replace_vars(attr_value, variables)  # noqa: PLW2901
            output[attr_name] = attr_value  # deepcode ignore UnhashableKey: you get same keys type as source
        return output
    if issubclass(obj_type, list):
        output = []
        for element in obj:
            element = replace_vars(element, variables)  # noqa: PLW2901
            output.append(element)  # deepcode ignore InfiniteLoopByCollectionModification: Not even close
        return output
    if issubclass(obj_type, tuple):
        return tuple(replace_vars(obj, variables, list))
    if issubclass(obj_type, str):
        return obj.format(**variables)
    return obj


def configure_logging(exception_handler=None,
                      formatters=None, filters=None, handlers=None, loggers=None, config=None, variables=None):
    urllib3.disable_warnings()
    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    if exception_handler:
        sys.excepthook = exception_handler
    if formatters is None:
        formatters = {
            'default': {
                '()': MultilineMessagesFormatter,
                'format': '%(asctime)s %(levelname)-5s %(filename)-15s:%(lineno)-4s %(message)s',
                'datefmt': '%H:%M:%S,%f'
            },
            'action_logger': {
                '()': JSONLFormatter,
                'format': '%(message)s'
            }
        }
    if filters is None:
        filters = {
            'filter_remote': {
                '()': FilterRemote
            }
        }
    if handlers is None:
        handlers = {
            'console': {
                'level': 'INFO',
                'formatter': 'default',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # Default is stderr
                'filters': ['filter_remote']
            },
            'outfile': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'filename': '{log_dir}/sct.log',
                'mode': 'a',
                'formatter': 'default',
            },
            'argus': {
                'level': 'DEBUG',
                'class': 'logging.FileHandler',
                'filename': '{log_dir}/argus.log',
                'mode': 'a',
                'formatter': 'default',
            },
            'actions': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': '{log_dir}/actions.log',
                'mode': 'a',
                'formatter': 'action_logger',
            }
        }
    if loggers is None:
        loggers = {
            '': {  # root logger
                'handlers': ['console', 'outfile'],
                'level': 'DEBUG',
                'propagate': True
            },
            'botocore': {
                'level': 'CRITICAL'
            },
            'boto3': {
                'level': 'CRITICAL'
            },
            's3transfer': {
                'level': 'CRITICAL'
            },
            'multiprocessing': {
                'level': 'DEBUG',
                'propagate': True,
            },
            'paramiko.transport': {
                'level': 'CRITICAL'
            },
            'cassandra.connection': {
                'level': 'INFO'
            },
            'invoke': {
                'level': 'CRITICAL'
            },
            'anyconfig': {
                'level': 'ERROR'
            },
            'urllib3.connectionpool': {
                'level': 'INFO'
            },
            'argus': {
                'handlers': ['argus'],
                'level': 'DEBUG',
                'propagate': False
            },
            'action_logger': {
                'handlers': ['actions'],
                'level': 'INFO',
                'propagate': False
            },
            'sdcm.argus_test_run': {
                'handlers': ['argus'],
                'level': 'DEBUG',
                'propagate': False
            },
        }
    if config is None:
        config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': formatters,
            'filters': filters,
            'handlers': handlers,
            'loggers': loggers
        }
    logging.config.dictConfig(replace_vars(config, variables))


def disable_loggers_during_startup():
    loggers = {
        'botocore': {
            'level': 'CRITICAL'
        },
        'boto3': {
            'level': 'CRITICAL'
        },
    }
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'loggers': loggers
    }
    logging.config.dictConfig(config)
