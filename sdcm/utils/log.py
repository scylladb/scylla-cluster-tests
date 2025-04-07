import sys
import logging
import logging.config
import warnings

import urllib3

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
                'format': '< t:%(asctime)s f:%(filename)-15s l:%(lineno)-4s c:%(name)-20s p:%(levelname)-5s > %(message)s'
            },
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
