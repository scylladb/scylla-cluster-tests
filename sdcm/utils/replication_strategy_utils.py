import ast
import logging
import re

from contextlib import ContextDecorator
from typing import Callable, Dict, TYPE_CHECKING

from sdcm.utils.cql_utils import cql_quote_if_needed
from sdcm.utils.database_query_utils import is_system_keyspace, LOGGER
if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class ReplicationStrategy:  # pylint: disable=too-few-public-methods

    @classmethod
    def from_string(cls, replication_string):
        # To solve the problem when another curly braces were added (tablets related).
        # Example:
        # CREATE KEYSPACE scylla_bench WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy',
        # 'replication_factor': '1'} AND durable_writes = true AND tablets = {'enabled': false};
        LOGGER.debug("Analyze replication string '%s'", replication_string)
        replication_value = re.search(r".*replication[\s]=[\s](\{.*?\})", replication_string, flags=re.IGNORECASE)

        strategy_params = ast.literal_eval(replication_value[1])
        strategy_class = strategy_params.pop("class")
        for class_ in replication_strategies:
            # To cover short and long class name, like:
            #   - SimpleStrategy
            #   - org.apache.cassandra.locator.SimpleStrategy
            if strategy_class.endswith(class_.class_):
                return class_(**strategy_params)
        raise ValueError(f"Couldn't find such replication strategy: {replication_value}")

    @classmethod
    def get(cls, node: 'BaseNode', keyspace: str):
        create_ks_statement = node.run_cqlsh(f"describe {keyspace}").stdout.splitlines()[1]
        return ReplicationStrategy.from_string(create_ks_statement)

    def apply(self, node: 'BaseNode', keyspace: str):
        cql = f'ALTER KEYSPACE {cql_quote_if_needed(keyspace)} WITH replication = {self}'
        with node.parent_cluster.cql_connection_patient(node) as session:
            session.execute(cql)

    @property
    def replication_factors(self) -> list:  # pylint: disable=no-self-use
        return [0]


class SimpleReplicationStrategy(ReplicationStrategy):

    class_: str = 'SimpleStrategy'

    def __init__(self, replication_factor: int):
        self._replication_factor = replication_factor

    def __str__(self):
        return f"{{'class': '{self.class_}', 'replication_factor': {self._replication_factor}}}"

    @property
    def replication_factors(self) -> list:
        return [int(self._replication_factor)]


class NetworkTopologyReplicationStrategy(ReplicationStrategy):

    class_: str = 'NetworkTopologyStrategy'

    def __init__(self, default_rf: int | None = None, **replication_factors: int):
        if default_rf is not None:
            self.replication_factors_per_dc = {"replication_factor": default_rf}
        else:
            self.replication_factors_per_dc = {}
        self.replication_factors_per_dc.update(**replication_factors)
        if not self.replication_factors_per_dc:
            raise ValueError("At least one replication factor should be provided or default_rf should be set")

    def __str__(self):
        factors = ', '.join([f"'{key}': {value}" for key, value in self.replication_factors_per_dc.items()])
        return f"{{'class': '{self.class_}', {factors}}}"

    @property
    def replication_factors(self) -> list:
        return [int(rf) for rf in self.replication_factors_per_dc.values()]


class LocalReplicationStrategy(ReplicationStrategy):

    class_: str = 'LocalStrategy'

    def __str__(self):
        return f"{{'class': '{self.class_}'}}"


replication_strategies = [SimpleReplicationStrategy, NetworkTopologyReplicationStrategy, LocalReplicationStrategy]


class temporary_replication_strategy_setter(ContextDecorator):  # pylint: disable=invalid-name
    """Context manager that allows to set replication strategy
     and preserves all modified keyspaces for automatic rollback on exit."""

    def __init__(self, node: 'BaseNode') -> None:
        self.node = node
        self.preserved: Dict[str, ReplicationStrategy] = {}

    def __enter__(self) -> Callable[..., None]:
        return self

    def __exit__(self, *exc) -> bool:
        self(**self.preserved)
        return False

    def _preserve_replication_strategy(self, keyspace: str) -> None:
        if keyspace in self.preserved:
            return  # already preserved
        self.preserved[keyspace] = ReplicationStrategy.get(self.node, keyspace)

    def __call__(self, **keyspaces: ReplicationStrategy) -> None:
        for keyspace, strategy in keyspaces.items():
            self._preserve_replication_strategy(keyspace)
            strategy.apply(self.node, keyspace)


class DataCenterTopologyRfChange:
    """
        If any keyspace RF equals to number-of-cluster-nodes, where tablets are in use,
        then a decommission is not supported.
        In this case, the user has to decrease the replication-factor of any such keyspace first.
        Later on, after adding a new node, such a keyspace can be reconfigured back to its original
        replication-factor value.
    """

    def __init__(self, target_node: 'BaseNode') -> None:
        self.target_node = target_node
        self.cluster = target_node.parent_cluster
        self.datacenter = target_node.datacenter
        self.decreased_rf_keyspaces = []
        self.original_nodes_number = self._get_original_nodes_number(target_node)

    def _get_original_nodes_number(self, node: 'BaseNode') -> int:
        # Get the original number of nodes in the data center
        return len([n for n in self.cluster.nodes if n.dc_idx == node.dc_idx])

    def _get_keyspaces_to_decrease_rf(self, session) -> list:
        """
        Returns a list of keyspaces of the data-center that have the specified replication factor.

        Example:
            For a replication_factor of 3 and dc of "dc1", the output might be:
            ["keyspace1", "scylla_bench"]
        """
        query = "SELECT keyspace_name, replication FROM system_schema.keyspaces"
        cql_result = session.execute(query)

        matching_keyspaces = []

        for row in cql_result.current_rows:
            keyspace_name = row.keyspace_name

            if is_system_keyspace(keyspace_name):
                continue

            replication = row.replication

            if 'SimpleStrategy' in replication['class']:
                continue  # Skip keyspace using SimpleStrategy

            if 'NetworkTopologyStrategy' in replication['class']:
                rf = int(replication.get(self.datacenter))
                if rf == self.original_nodes_number:
                    matching_keyspaces.append(keyspace_name)
            else:
                LOGGER.warning("Unexpected replication strategy found: %s", replication['class'])

        return matching_keyspaces

    def _alter_keyspace_rf(self, keyspace: str, replication_factor: int, session):
        # Alter the replication factor for keyspace of the data-center.

        alter_ks_cmd = f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{self.datacenter}':{replication_factor} }}"
        message = f"Altering {keyspace} RF with: {alter_ks_cmd}"
        LOGGER.debug(message)
        try:
            session.execute(alter_ks_cmd)
        except Exception as error:
            LOGGER.error(f"{message} Failed with: {error}")
            raise error

    def revert_to_original_keyspaces_rf(self):
        LOGGER.debug(f"Reverting keyspaces replication factor to original value of {self.datacenter}..")
        with self.cluster.cql_connection_patient(self.cluster.nodes[0]) as session:
            for keyspace in self.decreased_rf_keyspaces:
                self._alter_keyspace_rf(keyspace=keyspace, replication_factor=self.original_nodes_number,
                                        session=session)

    def decrease_keyspaces_rf(self):
        node = self.target_node
        with self.cluster.cql_connection_patient(node) as session:
            # Ensure that nodes_num is 2 or greater
            if self.original_nodes_number > 1:
                if decreased_rf_keyspaces := self._get_keyspaces_to_decrease_rf(session=session):
                    LOGGER.debug(
                        f"Found the following keyspaces with replication factor to decrease: {decreased_rf_keyspaces}")
                    try:
                        for keyspace in decreased_rf_keyspaces:
                            self._alter_keyspace_rf(keyspace=keyspace, replication_factor=self.original_nodes_number - 1,
                                                    session=session)
                            self.decreased_rf_keyspaces.append(keyspace)
                    except Exception as error:
                        self.revert_to_original_keyspaces_rf()
                        LOGGER.error(
                            f"Decreasing keyspace replication factor failed with: ({error}), aborting operation")
                        raise error
            else:
                LOGGER.error(
                    f"DC {self.datacenter} has {self.original_nodes_number} nodes. Cannot alter replication factor")
