class NoFilesFoundToDestroy(Exception):
    pass


class NoKeyspaceFound(Exception):
    pass


class FilesNotCorrupted(Exception):
    pass


class LogContentNotFound(Exception):
    pass


class LdapNotRunning(Exception):
    pass


class TimestampNotFound(Exception):
    pass


class PartitionNotFound(Exception):
    pass


class WatcherCallableException(Exception):
    """ raised when a watcher function in a trigger - watcher pair fails"""


class UnsupportedNemesis(Exception):
    """ raised from within a nemesis execution to skip this nemesis"""


class CdcStreamsWasNotUpdated(Exception):
    """ raised if messages:
          - Generation {}: streams description table already updated
          - CDC description table successfully updated with generation
        were not found in logs
    """


class NemesisSubTestFailure(Exception):
    """ raised if nemesis got error from sub test
    """


class AuditLogTestFailure(Exception):
    """ raised if nemesis got error from audit log validation
    """


class BootstrapStreamErrorFailure(Exception):  # pylint: disable=too-few-public-methods
    """ raised if node was not boostrapped after bootstrap
    streaming was aborted """


class KillNemesis(BaseException):
    """Exception that would be raised, when a nemesis thread is killed at teardown of the test"""
