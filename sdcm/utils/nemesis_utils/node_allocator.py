import logging
import random
import threading
from enum import Enum
from contextlib import contextmanager
from collections.abc import Iterable
from functools import wraps
from typing import TYPE_CHECKING, Union

from sdcm.utils.nemesis_utils import unique_disruption_name
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.metaclasses import Singleton
from sdcm.utils.nemesis_utils import NEMESIS_TARGET_POOLS, DefaultValue

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class NemesisNodeAllocationError(Exception):
    """Base exception for allocation failures."""


class AllNodesRunNemesisError(NemesisNodeAllocationError):
    """Raised when all candidate nodes are already running nemeses."""


class NoNodeMatchesCriteriaError(NemesisNodeAllocationError):
    """Raised when no available nodes match the specified filters."""


class NemesisNodeAllocator(metaclass=Singleton):
    def __init__(self, tester_obj):
        self.lock = threading.Lock()
        self._tester = tester_obj
        self.active_nemesis_on_nodes: dict['BaseNode', str] = {}

    def _get_pool_type_nodes(self, pool_type: Enum) -> list['BaseNode']:
        all_nodes = self._tester.all_db_nodes
        match pool_type:
            case NEMESIS_TARGET_POOLS.all_nodes:
                return all_nodes
            case NEMESIS_TARGET_POOLS.zero_nodes:
                return [node for node in all_nodes if node._is_zero_token_node]
            case NEMESIS_TARGET_POOLS.data_nodes:
                return [node for node in all_nodes if not node._is_zero_token_node]
            case _:
                raise NemesisNodeAllocationError(f"Unsupported pool type: {pool_type}")

    @staticmethod
    def _filter_nodes(nodes_to_filter: list['BaseNode'],
                      is_seed: bool | DefaultValue | None = DefaultValue,
                      dc_idx: int | None = None,
                      rack: int | None = None,
                      filter_seed: bool = False) -> list['BaseNode']:
        """
        Filters nodes based on is_seed, dc_idx, rack criteria.

        :param nodes_to_filter: list of nodes to filter.
        :param is_seed: specifies whether to filter nodes based on their seed status.
            - DefaultValue: no filtering based on seed status
            - True: include only seed nodes
            - False: exclude seed nodes
        :param dc_idx: data center index to filter nodes by.
        :param rack: rack index to filter nodes by.
        :param filter_seed: if True, excludes seed nodes by default unless `is_seed` is explicitly set.
        """
        if is_seed is DefaultValue:
            is_seed = False if filter_seed else None

        filtered = list(nodes_to_filter)
        if is_seed is not None:
            filtered = [node for node in filtered if node.is_seed == is_seed]
        if dc_idx is not None:
            filtered = [node for node in filtered if node.dc_idx == dc_idx]
        if rack is not None:
            filtered = [node for node in filtered if node.rack == rack]
        return filtered

    @contextmanager
    def run_nemesis(self, nemesis_label: str, node_list: list['BaseNode'] | None = None,
                    pool_type: NEMESIS_TARGET_POOLS = NEMESIS_TARGET_POOLS.data_nodes,
                    filter_seed: bool = False):
        """
        Select a node from node_list and mark it as running nemesis for the duration of the context.

        :param nemesis_label: nemesis operation
        :param node_list: list of candidate nodes (if None, uses all nodes from pool_type)
        :param pool_type: Type of node pool to select from
        :param filter_seed: Whether to filter out seed nodes
        """
        reserved_node = None
        unique_nemesis_label = unique_disruption_name(nemesis_label)
        try:
            reserved_node = self.select_target_node(
                nemesis_name=unique_nemesis_label,
                pool_type=pool_type,
                filter_seed=filter_seed,
                node_list=node_list
            )
            yield reserved_node
        finally:
            if reserved_node:
                self.unset_running_nemesis(reserved_node, unique_nemesis_label)

    def select_target_node(self,
                           nemesis_name: str,
                           pool_type: Enum,
                           filter_seed: bool,
                           is_seed: bool | DefaultValue | None = DefaultValue,
                           dc_idx: int | None = None,
                           rack: int | None = None,
                           allow_only_last_node_in_rack: bool = False,
                           node_list: list['BaseNode'] | None = None
                           ) -> 'BaseNode':
        """
        Selects a not allocated node as a target for the nemesis.

        Raises instance of NemesisNodeAllocationError if no nodes are available or match the criteria.
        """
        with self.lock:
            nodes = self._get_pool_type_nodes(pool_type)
            available_for_selection = [node for node in nodes if node not in self.active_nemesis_on_nodes]
            if node_list:
                available_for_selection = list(set(available_for_selection) & set(node_list))

            if not available_for_selection:
                reserved_nodes_map = {n.name: nem for n, nem in self.active_nemesis_on_nodes.items()}
                reason = f"All nodes from pool '{pool_type.value}' are running nemeses:\n{reserved_nodes_map}"
                raise AllNodesRunNemesisError(f"{nemesis_name}: node allocation failed.\n{reason}")

            candidate_nodes = self._filter_nodes(
                available_for_selection, is_seed=is_seed, dc_idx=dc_idx, rack=rack, filter_seed=filter_seed)

            if not candidate_nodes:
                dc_str = f'dc_idx={dc_idx}' if dc_idx is not None else ''
                rack_str = f'rack={rack}' if rack is not None else ''
                seed_str = f'is_seed={is_seed}' if is_seed is not DefaultValue else ''
                filter_str = ", ".join([dc_str, rack_str, seed_str])
                reason = (
                    f"The following '{pool_type.value}' nodes are available for selection, but none of "
                    f"them match the specified criteria ({filter_str}).\n"
                    f"{[n.name for n in available_for_selection]}.")
                raise NoNodeMatchesCriteriaError(f"{nemesis_name}: node allocation failed.\n{reason}")

            selected_node: 'BaseNode'
            if allow_only_last_node_in_rack and rack is not None:
                selected_node = candidate_nodes[-1]
            else:
                selected_node = random.choice(candidate_nodes)

            self.active_nemesis_on_nodes[selected_node] = nemesis_name
            selected_node.running_nemesis = nemesis_name
            LOGGER.info("%s: selected target node %s. Marked it as running nemesis.",
                        nemesis_name, selected_node.name)
            return selected_node

    def set_running_nemesis(self, node: 'BaseNode', nemesis_name: str) -> bool:
        """
        Sets running nemesis for the node.

        This is for the cases when e.g. a new node is added by a nemesis, which is running on another target node.
        """
        with self.lock:
            if node in self.active_nemesis_on_nodes:
                TestFrameworkEvent(
                    source=self.__class__.__name__,
                    message=f"{nemesis_name}: setting running nemesis failed.\nTried to set running nemesis for "
                    f"node {node.name}, but it was already reserved by '{self.active_nemesis_on_nodes[node]}' nemesis.",
                    severity=Severity.ERROR
                ).publish()
                return False

            self.active_nemesis_on_nodes[node] = nemesis_name
            node.running_nemesis = nemesis_name
            LOGGER.info("%s: set running nemesis for node %s.", nemesis_name, node.name)
            return True

    def unset_running_nemesis(self, node: 'BaseNode', nemesis_name: str):
        """Unsets running nemesis, making the node available for other nemeses."""
        with self.lock:
            if node in self.active_nemesis_on_nodes:
                if self.active_nemesis_on_nodes[node] == nemesis_name:
                    del self.active_nemesis_on_nodes[node]
                    node.running_nemesis = None
                    LOGGER.info("%s: unset running nemesis for node %s.", nemesis_name, node.name)
                else:
                    TestFrameworkEvent(
                        source=self.__class__.__name__,
                        message=f"{nemesis_name}: unsetting running nemesis failed.\nTried to unset '{nemesis_name}' "
                        f"nemesis from node {node.name}, but it was running another "
                        f"'{self.active_nemesis_on_nodes[node]}' nemesis.",
                        severity=Severity.ERROR
                    ).publish()
            else:
                TestFrameworkEvent(
                    source=self.__class__.__name__,
                    message=f"{nemesis_name}: unsetting running nemesis failed.\nTried to unset '{nemesis_name}' "
                    f"nemesis from node {node.name}, but it was not running any nemesis.",
                    severity=Severity.ERROR
                ).publish()

    def unset_running_nemesis_from_all_nodes(self, nemesis_name: str):
        """
        Unsets the specified nemesis from all nodes that run it.

        The primary use case is when a nemesis creates new node(s), which are supposed to stay in the cluster
        after the nemesis is finished. To avoid stale records in the allocator, and for the subsequent nemeses to
        be able to use the new node(s), we need to release them after disruption is finished.
        """
        with self.lock:
            nodes_to_release = [node for node, owner in self.active_nemesis_on_nodes.items() if owner == nemesis_name]
            if nodes_to_release:
                LOGGER.info("%s: unset running nemesis from nodes: %s",
                            nemesis_name, [n.name for n in nodes_to_release])
                for node in nodes_to_release:
                    del self.active_nemesis_on_nodes[node]
                    node.running_nemesis = None

    def switch_target_node(self, old_node: 'BaseNode', new_node: 'BaseNode', nemesis_name: str) -> bool:
        """
        Switches the nemesis target from an old node to a new one.

        Raises NemesisNodeAllocationError if the new node is already running another nemesis.
        """
        if old_node == new_node:
            return True

        with self.lock:
            if new_node in self.active_nemesis_on_nodes:
                reason = f"Requested node is already running nemesis '{self.active_nemesis_on_nodes[new_node]}'"
                raise NemesisNodeAllocationError(f"{nemesis_name}: node switching failed.\n{reason}")
            if old_node and old_node in self.active_nemesis_on_nodes:
                if self.active_nemesis_on_nodes[old_node] == nemesis_name:
                    del self.active_nemesis_on_nodes[old_node]
                    old_node.running_nemesis = None
                    LOGGER.info("%s: released old target node %s during switch.", nemesis_name, old_node.name)
                else:
                    TestFrameworkEvent(
                        source=self.__class__.__name__,
                        message=f"{nemesis_name}: switching target node failed.\nDuring switch, old node "
                        f"{old_node.name} was running another '{self.active_nemesis_on_nodes[old_node]}' nemesis.",
                        severity=Severity.ERROR
                    ).publish()

            self.active_nemesis_on_nodes[new_node] = nemesis_name
            new_node.running_nemesis = nemesis_name
            LOGGER.info("%s: switched target node to %s.", nemesis_name, new_node.name)
            return True

    @contextmanager
    def nodes_running_nemesis(self, nodes: Union[Iterable['BaseNode'], 'BaseNode'], nemesis_name: str):
        """Temporarily marks nodes as running the specified nemesis."""
        if not isinstance(nodes, Iterable):
            nodes = [nodes]
        marked_nodes = []
        try:
            for node in nodes:
                if not self.set_running_nemesis(node, nemesis_name):
                    for marked_node in marked_nodes:
                        self.unset_running_nemesis(marked_node, nemesis_name)
                    marked_nodes.clear()
                    raise NemesisNodeAllocationError(
                        f"{nemesis_name}: failed to set running nemesis for all requested nodes; "
                        f"{node.name} was already reserved.")
                marked_nodes.append(node)
            yield
        finally:
            for node in marked_nodes:
                self.unset_running_nemesis(node, nemesis_name)


def mark_new_nodes_as_running_nemesis(func):
    """
    Decorator for `add_nodes` API of a cluster object.

    If `disruption_name` parameter is passed to the `add_nodes` method of a cluster, the newly added nodes are
    immediately marked as running the disruption in question.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        allocator = getattr(self.test_config.tester_obj(), 'nemesis_allocator', None)
        disruption_name = kwargs.pop('disruption_name', None)

        new_nodes = func(self, *args, **kwargs)
        if new_nodes and allocator and disruption_name:
            for node in new_nodes:
                allocator.set_running_nemesis(node, disruption_name)
        return new_nodes
    return wrapper
