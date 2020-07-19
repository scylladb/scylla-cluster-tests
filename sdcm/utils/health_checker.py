import logging

from sdcm.sct_events import ClusterHealthValidatorEvent, Severity

LOGGER = logging.getLogger(__name__)


def check_nodes_status(nodes_status, current_node, removed_nodes_list=None):
    node_type = 'target' if current_node.running_nemesis else 'regular'
    if not nodes_status:
        LOGGER.warning("Node status info is not available. Search for the warning above")
        return

    LOGGER.info(f'Status for {node_type} node {current_node.name}')

    for node_ip, node_properties in nodes_status.items():
        if node_properties['status'] != "UN":
            is_target = current_node.print_node_running_nemesis(node_ip)
            LOGGER.debug(f'REMOVED NODES LIST = {removed_nodes_list}')
            if node_ip in removed_nodes_list:
                severity = Severity.ERROR
            else:
                severity = Severity.CRITICAL
            # FIXME: https://github.com/scylladb/scylla-enterprise/issues/1419 must be reverted once it is fixed.
            ClusterHealthValidatorEvent(type='critical', name='NodeStatus', status=severity,
                                        node=current_node.name,
                                        error=f"Current node {current_node.ip_address}. Node with {node_ip}{is_target} "
                                        f"status is {node_properties['status']}")


def check_nulls_in_peers(gossip_info, peers_details, current_node):
    """
    This validation is added to recreate the issue: https://github.com/scylladb/scylla/issues/4652
    Found scenario described in the https://github.com/scylladb/scylla/issues/6397
    """
    if not gossip_info:
        LOGGER.warning("Gossip info is not available. Search for the warning above")
        return

    for ip, node_info in peers_details.items():
        if all(value != "null" for value in node_info.values()):
            continue

        is_target = current_node.print_node_running_nemesis(ip)
        message = f"Current node {current_node.ip_address}. Found nulls in system.peers for " \
            f"node {ip}{is_target} with status {gossip_info[ip]['status']} : " \
            f"{peers_details[ip]}"

        if ip in gossip_info and gossip_info[ip]['status'] not in current_node.GOSSIP_STATUSES_FILTER_OUT:
            ClusterHealthValidatorEvent(type='error', name='NodePeersNulls', status=Severity.ERROR,
                                        node=current_node.name,
                                        error=message)
        else:
            # Issue https://github.com/scylladb/scylla/issues/6397 - Should the info about decommissioned node
            # be kept in the system.peers?
            LOGGER.warning(message)


def check_node_status_in_gossip_and_nodetool_status(gossip_info, nodes_status, current_node):
    if not nodes_status:
        LOGGER.warning("Node status info is not available. Search for the warning above. "
                       "Verify node status can't be performed")
        return

    for ip, node_info in gossip_info.items():
        is_target = current_node.print_node_running_nemesis(ip)
        if ip not in nodes_status:
            if node_info['status'] not in current_node.GOSSIP_STATUSES_FILTER_OUT:
                LOGGER.debug(f"Gossip info: {gossip_info}\nnodetool.status info: {nodes_status}")
                ClusterHealthValidatorEvent(type='error', name='NodeStatus', status=Severity.ERROR,
                                            node=current_node.name,
                                            error=f"Current node {current_node.ip_address}. "
                                            f"The node {ip}{is_target} exists in the gossip but doesn't exist "
                                            f"in the nodetool.status")
            continue

        if (node_info['status'] == 'NORMAL' and nodes_status[ip]['status'] != 'UN') or \
                (node_info['status'] != 'NORMAL' and nodes_status[ip]['status'] == 'UN'):
            ClusterHealthValidatorEvent(type='error', name='NodeStatus', status=Severity.ERROR,
                                        node=current_node.name,
                                        error=f"Current node {current_node.ip_address}. Wrong node status. "
                                        f"Node {ip}{is_target} status in nodetool.status is {nodes_status[ip]['status']}, "
                                        f"but status in gossip {node_info['status']}")

    # Validate that all nodes in nodetool.status exist in gossip
    not_in_gossip = list(set(nodes_status.keys()) - set(gossip_info.keys()))
    for ip in not_in_gossip:
        if nodes_status[ip]['status'] == 'UN':
            is_target = current_node.print_node_running_nemesis(ip)
            LOGGER.debug(f"Gossip info: {gossip_info}\nnodetool.status info: {nodes_status}")
            ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.ERROR,
                                        node=current_node.name,
                                        error=f"Current node {current_node.ip_address}. Node {ip}{is_target} exists in the "
                                        f"nodetool.status but missed in gossip.")


def check_schema_version(gossip_info, peers_details, nodes_status, current_node):
    if nodes_status and len(nodes_status.keys()) == 1:
        LOGGER.debug('There is one node only in the cluster. No peers data. '
                     'Verify schema version can\'t be performed')
        return

    if not peers_details:
        LOGGER.warning("SYSTEM.PEERS info is not availble. Search for the warning above. "
                       "Verify schema version can\'t be performed")
        return

    if not gossip_info:
        LOGGER.warning("Gossip info is not availble. Search for the warning above. "
                       "Verify schema version can\'t be performed")
        return

    debug_message = f"Gossip info: {gossip_info}\nSYSTEM.PEERS info: {peers_details}"
    # Validate schema version
    for ip, node_info in gossip_info.items():
        # SYSTEM.PEERS table includes peers of the current node, so the node itcurrent_node doesn't exist in the list
        if current_node.ip_address == ip:
            continue

        # Can't validate the schema version if the node is not in NORMAL status
        if node_info['status'] != 'NORMAL':
            continue

        is_target = current_node.print_node_running_nemesis(ip)
        if ip not in peers_details.keys():
            LOGGER.debug(debug_message)
            ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.ERROR,
                                        node=current_node.name,
                                        error=f"Current node {current_node.ip_address}. Node {ip}{is_target} exists in "
                                        f"the gossip but missed in SYSTEM.PEERS.")
            continue

        if node_info['schema'] != peers_details[ip]['schema_version']:
            LOGGER.debug(debug_message)
            ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.ERROR,
                                        node=current_node.name,
                                        error=f"Current node {current_node.ip_address}. Wrong Schema version. "
                                        f"Node {ip}{is_target} schema version in SYSTEM.PEERS is "
                                        f"{peers_details[ip]['schema_version']}, "
                                        f"but schema version in gossip {node_info['schema']}")

    # Validate that all nodes in SYSTEM.PEERS exist in gossip
    not_in_gossip = list(set(peers_details.keys()) - set(gossip_info.keys()))
    if not_in_gossip:
        LOGGER.debug(debug_message)
        ClusterHealthValidatorEvent(type='error', name='NodeSchemaVersion', status=Severity.ERROR,
                                    node=current_node.name,
                                    error=f"Current node {current_node.ip_address}. Nodes {','.join(ip for ip in not_in_gossip)} "
                                    f"exists in the SYSTEM.PEERS but missed in gossip.")

    # Validate that same schema on all nodes in the gossip
    schema_version_on_all_nodes = [values['schema'] for values in gossip_info.values()
                                   if values['status'] not in current_node.GOSSIP_STATUSES_FILTER_OUT]

    if len(set(schema_version_on_all_nodes)) > 1:
        LOGGER.debug(debug_message)
        gossip_info_str = '\n'.join(
            f"{ip}: {schema_version['schema']}" for ip, schema_version in gossip_info.items())
        ClusterHealthValidatorEvent(type='warning', name='NodeSchemaVersion', status=Severity.WARNING,
                                    node=current_node.name,
                                    message=f'Current node {current_node.ip_address}. Schema version is not same on '
                                    f'all nodes in gossip info: {gossip_info_str}')

    # Validate that same schema on all nodes in the SYSTEM.PEERS
    schema_version_on_all_nodes = [values['schema_version'] for ip, values in peers_details.items()
                                   if gossip_info[ip]['status'] not in current_node.GOSSIP_STATUSES_FILTER_OUT]

    if len(set(schema_version_on_all_nodes)) > 1:
        LOGGER.debug(debug_message)
        peers_info_str = '\n'.join(
            f"{ip}: {schema_version['schema_version']}" for ip, schema_version in gossip_info.items())
        ClusterHealthValidatorEvent(type='warning', name='NodeSchemaVersion', status=Severity.WARNING,
                                    node=current_node.name,
                                    message=f'Current node {current_node.ip_address}. Schema version is not same on all '
                                    f'nodes in SYSTEM.PEERS info: {peers_info_str}')


def check_schema_agreement_in_gossip_and_peers(node):
    message_pref = f'Check for schema agreement on the node {node.name} ({node.ip_address}).'
    gossip_info = node.get_gossip_info()
    peers_info = node.get_peers_info()
    LOGGER.debug(f'{message_pref} Gossip info: {gossip_info}')

    gossip_schema = {data["schema"] for data in gossip_info.values() if data['status'] == "NORMAL"}
    if len(gossip_schema) > 1:
        LOGGER.warning(f'{message_pref} Schema version is not same on nodes in the gossip')
        return False

    LOGGER.debug(f'{message_pref} Peers info: {peers_info}')

    for ip, data in gossip_info.items():
        if data['status'] == "NORMAL" and ip in peers_info:
            if data["schema"] != peers_info[ip]['schema_version']:
                LOGGER.warning(f'{message_pref} Schema version is not same in the gossip and peers for {ip}')
                return False

    LOGGER.info('Schema agreement has been completed on all nodes')
    return True
