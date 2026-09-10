"""Shared node-topology operations reused by topology-changing nemesis.

These primitives take the ``NemesisRunner`` as their first argument so any
monkey module (and ``NemesisRunner`` itself, e.g. ``NemesisSequence``) can
reuse them without inheriting from a common base class. Keeping them in
``sdcm.nemesis.utils`` (which does not import from ``sdcm.nemesis``) lets
both sides import them at the top level without circular imports.
"""

import time
from typing import Optional, Union

from sdcm.cluster import BaseNode
from sdcm.nemesis.utils import NEMESIS_TARGET_POOLS, DefaultValue
from sdcm.sct_events.system import InfoEvent


def decommission_nodes_by_criteria(
    runner,
    nodes_number: int,
    rack: Optional[int],
    is_seed: Optional[Union[bool, DefaultValue]] = DefaultValue,
    dc_idx: Optional[int] = None,
    exact_nodes: list[BaseNode] | None = None,
) -> None:
    """Decommission either the given nodes or ``nodes_number`` nodes matching the criteria.

    Args:
        runner: The ``NemesisRunner`` owning the cluster and the node allocator.
        nodes_number: How many nodes to pick when ``exact_nodes`` is not given.
        rack: Rack to pick the nodes from, ``None`` to round-robin over all racks.
        is_seed: Seed filter passed to the target node selection.
        dc_idx: Datacenter index to pick the nodes from.
        exact_nodes: Nodes to decommission, bypassing the selection logic.
    """
    nodes_to_decommission = []
    if exact_nodes:
        nodes_to_decommission = exact_nodes
        for node in exact_nodes:
            runner.node_allocator.set_running_nemesis(node, runner.current_disruption)
    else:
        for idx in range(nodes_number):
            if runner._is_it_on_kubernetes():
                if rack is None:
                    rack = 0
                runner.set_target_node_pool_type(NEMESIS_TARGET_POOLS.data_nodes)
                runner.set_target_node(rack=rack, is_seed=is_seed, allow_only_last_node_in_rack=True)
            else:
                rack_idx = rack if rack is not None else idx % runner.cluster.racks_count
                # if rack is not specified, round-robin racks
                runner.set_target_node(is_seed=is_seed, dc_idx=dc_idx, rack=rack_idx)
            nodes_to_decommission.append(runner.target_node)
            runner.target_node = (
                None  # otherwise node.running_nemesis will be taken off the node by runner.set_target_node
            )
    try:
        if runner.cluster.parallel_node_operations:
            runner.decommission_nodes(nodes_to_decommission)
        else:
            for node in nodes_to_decommission:
                runner.decommission_nodes([node])
    except Exception as exc:  # noqa: BLE001
        InfoEvent(
            f"FinishEvent - ShrinkCluster failed decommissioning a node {runner.target_node} with error {exc!s}"
        ).publish()


def grow_cluster(runner, rack: Optional[int] = None) -> list[BaseNode]:
    """Add ``nemesis_add_node_cnt`` nodes to the cluster and release them from the nemesis.

    Args:
        runner: The ``NemesisRunner`` owning the cluster and the node allocator.
        rack: Rack to add the new nodes to, ``None`` to round-robin over all racks.

    Returns:
        The nodes that were added to the cluster.
    """
    if rack is None and runner._is_it_on_kubernetes():
        rack = 0
    add_nodes_number = runner.tester.params.get("nemesis_add_node_cnt")
    new_nodes = []
    with runner.action_log_scope(f"Grow cluster by {add_nodes_number} nodes"):
        if runner.cluster.parallel_node_operations:
            new_nodes = runner.add_new_nodes(
                count=add_nodes_number,
                rack=rack,
                instance_type=runner.tester.params.get("nemesis_grow_shrink_instance_type"),
            )
        else:
            for idx in range(add_nodes_number):
                # if rack is not specified, round-robin racks to spread nodes evenly
                rack_idx = rack if rack is not None else idx % runner.cluster.racks_count
                new_nodes += runner.add_new_nodes(
                    count=1,
                    rack=rack_idx,
                    instance_type=runner.tester.params.get("nemesis_grow_shrink_instance_type"),
                )
    time.sleep(runner.interval)
    for node in new_nodes:
        runner.node_allocator.unset_running_nemesis(node, runner.current_disruption)
    return new_nodes


def shrink_cluster(runner, rack: Optional[int] = None, new_nodes: list[BaseNode] | None = None) -> None:
    """Decommission nodes from the target node datacenter back towards the initial cluster size.

    Args:
        runner: The ``NemesisRunner`` owning the cluster and the node allocator.
        rack: Rack to decommission the nodes from, ``None`` to round-robin over all racks.
        new_nodes: Exact nodes to decommission, ``None`` to select them by criteria.

    Raises:
        Exception: If the cluster does not have enough nodes above its initial size.
    """
    add_nodes_number = runner.tester.params.get("nemesis_add_node_cnt")
    InfoEvent(message=f"Start shrink cluster by {add_nodes_number} nodes").publish()
    # Check that number of nodes is enough for decommission:
    runner.log.debug(
        "Current target_node %s, is zero_node: %s, dc_idx: %s",
        runner.target_node.name,
        runner.target_node._is_zero_token_node,
        runner.target_node.dc_idx,
    )
    cur_num_nodes_in_dc = len([n for n in runner.cluster.data_nodes if n.dc_idx == runner.target_node.dc_idx])
    initial_db_size = runner.tester.params.get("n_db_nodes")
    if runner._is_it_on_kubernetes():
        k8s_size = runner.tester.params.get("k8s_n_scylla_pods_per_cluster")
        initial_db_size = [k8s_size] * len(initial_db_size) if k8s_size else initial_db_size

    initial_db_size_in_dc = initial_db_size[runner.target_node.dc_idx]
    decommission_nodes_number = min(cur_num_nodes_in_dc - initial_db_size_in_dc, add_nodes_number)

    if decommission_nodes_number < 1:
        error = "Not enough nodes for decommission"
        runner.log.warning("Shrink cluster skipped. Error: %s", error)
        raise Exception(error)

    runner.log.info("Start shrink cluster by %s nodes", decommission_nodes_number)
    # Currently on kubernetes first two nodes of each rack are getting seed status
    # Because of such behavior only way to get them decommission is to enable decommissioning
    # TBD: After https://github.com/scylladb/scylla-operator/issues/292 is fixed remove is_seed parameter
    decommission_nodes_by_criteria(
        runner,
        decommission_nodes_number,
        rack,
        is_seed=None if runner._is_it_on_kubernetes() else DefaultValue,
        dc_idx=runner.target_node.dc_idx,
        exact_nodes=new_nodes,
    )
    num_of_nodes = len(runner.cluster.data_nodes)
    runner.log.info("Cluster shrink finished. Current number of data nodes %s", num_of_nodes)
    InfoEvent(message=f"Cluster shrink finished. Current number of data nodes {num_of_nodes}").publish()
