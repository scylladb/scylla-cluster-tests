import sys
import logging
import logging.config

LOGGER = logging.getLogger(__name__)


def setup_stdout_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
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


class MakeFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=0):
        logging.FileHandler.__init__(self, filename, mode, encoding, delay)


class FilterRemote(logging.Filter):  # pylint: disable=too-few-public-methods
    def filter(self, record):
        return not record.name == 'sdcm.remote'


def configure_logging():
    from sdcm.cluster import Setup
    sys.excepthook = handle_exception

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,

        'formatters': {
            'default': {
                '()': MultilineMessagesFormatter,
                'format': '< t:%(asctime)s f:%(filename)-15s l:%(lineno)-4s c:%(name)-20s p:%(levelname)-5s > %(message)s'
            },
        },
        'filters': {
            'filter_remote': {
                '()': FilterRemote
            }
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'formatter': 'default',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',  # Default is stderr
                'filters': ['filter_remote']
            },
            'outfile': {
                'level': 'DEBUG',
                '()': MakeFileHandler,
                'filename': '{}/sct.log'.format(Setup.logdir()),
                'mode': 'w',
                'formatter': 'default',
            }
        },
        'loggers': {
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
            'paramiko.transport': {
                'level': 'CRITICAL'
            },
            'cassandra.connection': {
                'level': 'INFO'
            },
            'invoke': {
                'level': 'CRITICAL'
            }
        }
    })
