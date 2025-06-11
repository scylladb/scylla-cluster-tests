import logging
import random
import threading
from enum import Enum
from contextlib import contextmanager

from sdcm.cluster import BaseNode
from sdcm.utils.metaclasses import Singleton
from sdcm.utils.nemesis_utils import NEMESIS_TARGET_POOLS, DefaultValue


LOGGER = logging.getLogger(__name__)


class NemesisNodeAllocator(metaclass=Singleton):
    def __init__(self, tester_obj):
        self.lock = threading.Lock()
        self._tester = tester_obj
        self.active_nemeses_on_nodes: dict[BaseNode, str] = {}

    def _get_pool_type_nodes(self, pool_type: Enum) -> list[BaseNode]:
        all_nodes = self._tester.all_db_nodes
        match pool_type:
            case NEMESIS_TARGET_POOLS.all_nodes:
                return all_nodes
            case NEMESIS_TARGET_POOLS.zero_nodes:
                return [node for node in all_nodes if node._is_zero_token_node]
            case NEMESIS_TARGET_POOLS.data_nodes:
                return [node for node in all_nodes if not node._is_zero_token_node]
            case _:
                raise ValueError(f"Unsupported pool type: {pool_type}")

    @staticmethod
    def _filter_nodes(nodes_to_filter: list[BaseNode],
                      is_seed: bool | DefaultValue | None = DefaultValue,
                      dc_idx: int | None = None,
                      rack: int | None = None,
                      filter_seed: bool = False) -> list[BaseNode]:
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

    def select_target_node(self,
                           nemesis_name: str,
                           pool_type: Enum,
                           filter_seed: bool,
                           is_seed: bool | DefaultValue | None = DefaultValue,
                           dc_idx: int | None = None,
                           rack: int | None = None,
                           allow_only_last_node_in_rack: bool = False,
                           node_list: list[BaseNode] | None = None
                           ) -> tuple[BaseNode | None, str | None]:
        """
        Selects a not occupied node as a target for the nemesis.

        Returns the selected node or None if no suitable node is found.
        """
        with self.lock:
            nodes = self._get_pool_type_nodes(pool_type)
            available_for_selection = [node for node in nodes if node not in self.active_nemeses_on_nodes]
            if node_list:
                available_for_selection = list(set(available_for_selection) & set(node_list))

            if not available_for_selection:
                busy_nodes_map = {n.name: nem for n, nem in self.active_nemeses_on_nodes.items()}
                reason = f"All nodes from pool '{pool_type.value}' are currently busy with nemeses:\n{busy_nodes_map}"
                LOGGER.warning("%s: node allocation failed. %s", nemesis_name, reason)
                return None, reason

            candidate_nodes = self._filter_nodes(
                available_for_selection, is_seed=is_seed, dc_idx=dc_idx, rack=rack, filter_seed=filter_seed)

            if not candidate_nodes:
                dc_str = f'dc_idx={dc_idx}' if dc_idx is not None else ''
                rack_str = f'rack={rack}' if rack is not None else ''
                seed_str = f'is_seed={is_seed}' if is_seed is not DefaultValue else ''
                filter_str = ", ".join([dc_str, rack_str, seed_str])
                reason = (
                    f"The following pool '{pool_type.value}' nodes are available (not busy) for selection, but none of "
                    f"them match the specified criteria ({filter_str}).\n"
                    f"{[n.name for n in available_for_selection]}.")
                LOGGER.warning("%s: node allocation failed. %s", nemesis_name, reason)
                return None, reason

            selected_node: BaseNode
            if allow_only_last_node_in_rack and rack is not None:
                selected_node = candidate_nodes[-1]
            else:
                selected_node = random.choice(candidate_nodes)

            self.active_nemeses_on_nodes[selected_node] = nemesis_name
            LOGGER.info("%s: selected target node %s. Marked it as busy by allocator.",
                        nemesis_name, selected_node.name)
            return selected_node, None

    def mark_node_as_busy(self, node: BaseNode, nemesis_name: str):
        """
        Marks a node as busy with a nemesis.

        This is for the cases when e.g. a new node is added by a nemesis, which is running on another target node.
        """
        with self.lock:
            if node in self.active_nemeses_on_nodes:
                LOGGER.warning("%s: Tried to mark node %s as busy, but it was already busy with %s.",
                               nemesis_name, node.name, self.active_nemeses_on_nodes[node])
                return False

            self.active_nemeses_on_nodes[node] = nemesis_name
            LOGGER.info("%s: marked node %s as busy.", nemesis_name, node.name)
            return True

    def release_busy_node(self, node: BaseNode, nemesis_name: str):
        """Releases the node, making it available for other nemeses."""
        with self.lock:
            if node in self.active_nemeses_on_nodes:
                if self.active_nemeses_on_nodes[node] == nemesis_name:
                    del self.active_nemeses_on_nodes[node]
                    LOGGER.info("%s: released busy node %s.", nemesis_name, node.name)
                else:
                    LOGGER.warning("%s: tried to release node %s, but it was busy with a different nemesis %s.",
                                   nemesis_name, node.name, self.active_nemeses_on_nodes[node])
            else:
                LOGGER.warning("%s: tried to release node %s, but it was not marked as busy by the allocator.",
                               nemesis_name, node.name)

    def switch_target_node(self, old_node: BaseNode, new_node: BaseNode, nemesis_name: str) -> bool:
        """Switches the nemesis target from an old node to a new one."""
        if old_node == new_node:
            LOGGER.debug(
                "%s: Switch requested to the same node (%s). No action needed.", nemesis_name, new_node.name)
            return True

        with self.lock:
            if new_node in self.active_nemeses_on_nodes:
                LOGGER.warning("%s: Cannot switch to node %s. It is already busy with nemesis: %s",
                               nemesis_name, new_node.name, self.active_nemeses_on_nodes[new_node])
                return False
            if old_node and old_node in self.active_nemeses_on_nodes:
                if self.active_nemeses_on_nodes[old_node] == nemesis_name:
                    del self.active_nemeses_on_nodes[old_node]
                    LOGGER.info("%s: Released old target node %s during switch.", nemesis_name, old_node.name)
                else:
                    LOGGER.warning(
                        "%s: During switch, old_node %s was busy with a different nemesis (%s), not releasing.",
                        nemesis_name, old_node.name, self.active_nemeses_on_nodes[old_node])

            self.active_nemeses_on_nodes[new_node] = nemesis_name
            LOGGER.info("%s: Switched target node to %s.", nemesis_name, new_node.name)
            return True

    @contextmanager
    def mark_nodes_as_busy(self, nodes: list[BaseNode] | BaseNode, nemesis_name: str):
        """Temporarily marks a list of nodes as busy."""
        if isinstance(nodes, BaseNode):
            nodes = [nodes]
        marked_nodes = []
        try:
            for node in nodes:
                if not self.mark_node_as_busy(node, nemesis_name):
                    for marked_node in marked_nodes:
                        self.release_busy_node(marked_node, nemesis_name)
                    raise RuntimeError(
                        f"{nemesis_name}: failed to allocate all requested nodes; {node.name} was already occupied.")
                marked_nodes.append(node)
            yield
        finally:
            for node in nodes:
                self.release_busy_node(node, nemesis_name)
