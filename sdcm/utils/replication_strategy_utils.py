import ast
import re

from contextlib import ContextDecorator
from typing import Callable, Dict, TYPE_CHECKING

from sdcm.utils.cql_utils import cql_quote_if_needed
if TYPE_CHECKING:
    from sdcm.cluster import BaseNode


class ReplicationStrategy:  # pylint: disable=too-few-public-methods

    @classmethod
    def from_string(cls, replication_string):
        replication_value = re.search(r".*replication[\s]*=[\s]*(\{.*\})", replication_string, flags=re.IGNORECASE)
        strategy_params = ast.literal_eval(replication_value[1])
        strategy_class = strategy_params.pop("class")
        for class_ in replication_strategies:
            if strategy_class == class_.class_:
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
