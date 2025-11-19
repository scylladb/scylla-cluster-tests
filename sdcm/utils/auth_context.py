import logging

from contextlib import contextmanager
from sdcm.cluster import BaseNode

VALID_AUTHENTICATORS = [
    "org.apache.cassandra.auth.PasswordAuthenticator",
    "org.apache.cassandra.auth.AllowAllAuthenticator",
    "com.scylladb.auth.TransitionalAuthenticator",
    "com.scylladb.auth.SaslauthdAuthenticator",
]

LOGGER = logging.getLogger(__name__)


class AuthContextManagerException(Exception):
    pass


@contextmanager
def temp_authenticator(node: BaseNode, authenticator: str) -> None:
    if authenticator not in VALID_AUTHENTICATORS:
        raise AuthContextManagerException(f"Invalid Authenticator: {authenticator}")

    LOGGER.debug("Swapping authenticator to %s on node %s", authenticator, node)
    with node.remote_scylla_yaml() as scylla_yaml:
        original_authenticator = scylla_yaml.authenticator
        scylla_yaml.authenticator = authenticator
    node.restart_scylla()
    try:
        yield
    finally:
        LOGGER.debug("Restoring authenticator to %s on node %s", original_authenticator, node)
        with node.remote_scylla_yaml() as scylla_yaml:
            scylla_yaml.authenticator = original_authenticator
        node.restart_scylla()
