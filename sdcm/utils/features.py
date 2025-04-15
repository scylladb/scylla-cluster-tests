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
#
import logging

from cassandra.cluster import Session  # pylint: disable=no-name-in-module

CONSISTENT_TOPOLOGY_CHANGES_FEATURE = "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES"
CONSISTENT_CLUSTER_MANAGEMENT_FEATURE = "SUPPORTS_RAFT_CLUSTER_MANAGEMENT"

LOGGER = logging.getLogger(__name__)


def get_supported_features(session: Session) -> list[str]:
    """
    helper function to get supported_features from a running cluster,
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """
    result = session.execute("SELECT supported_features FROM system.local WHERE key='local'").one()
    # NOTE: since row_factory can be different on different tests, we need to support multiple options
    if isinstance(result, dict):
        result = result["supported_features"].split(",")
    elif isinstance(result, tuple):
        result = result[0].split(",")
    elif result is None:
        result = []
    else:
        raise NotImplementedError(f"unsupported row_factory={session.row_factory}")
    LOGGER.debug("Supported features %s", result)
    return result


def get_enabled_features(session: Session) -> list[str]:
    """
    helper function to get supported_features from a running cluster,
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """
    result = session.execute("SELECT value FROM system.scylla_local WHERE key='enabled_features'").one()
    # NOTE: since row_factory can be different on different tests, we need to support multiple options
    if isinstance(result, dict):
        result = result["value"].split(",")
    elif isinstance(result, tuple):
        result = result[0].split(",")
    elif result is None:
        result = []
    else:
        raise NotImplementedError(f"unsupported row_factory={session.row_factory}")
    LOGGER.debug("Enabled features %s", result)
    return result


def is_consistent_cluster_management_feature_enabled(session: Session) -> bool:
    """ Check whether raft consistent cluster management feature enabled
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """

    return CONSISTENT_CLUSTER_MANAGEMENT_FEATURE in get_enabled_features(session)


def is_consistent_topology_changes_feature_enabled(session: Session) -> bool:
    """ Check whether raft topology feature enabled
    if you need from a specific node use `patient_exclusive_cql_connection` session
    """

    return CONSISTENT_TOPOLOGY_CHANGES_FEATURE in get_enabled_features(session)


def is_tablets_feature_enabled(node) -> bool:
    """ Check whether tablets enabled
    """
    with node.remote_scylla_yaml() as scylla_yaml:
        # for backward compatibility of 2024.1 and earlier
        if isinstance(scylla_yaml, dict):
            scylla_dict = scylla_yaml
        else:
            scylla_dict = scylla_yaml.dict()

        if "tablets" in scylla_dict.get("experimental_features", []):
            return True
        if scylla_dict.get("enable_tablets"):
            return True
        if scylla_dict.get("tablets_mode_for_new_keyspaces") in ["enabled", "enforced"]:
            return True

    return False
