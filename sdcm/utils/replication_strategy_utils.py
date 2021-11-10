import re

from contextlib import ContextDecorator
from dataclasses import dataclass
from typing import List, Callable

from sdcm.cluster import BaseNode


@dataclass
class KeyspaceReplicationStrategy:
    keyspace: str
    strategy: str


class temporary_replication_strategy_setter(ContextDecorator):  # pylint: disable=invalid-name
    """Context manager that allows to set replication strategy
     and preserves all modified keyspaces for automatic rollback on exit."""

    def __init__(self, node: BaseNode) -> None:
        self.node = node
        self.preserved: List[KeyspaceReplicationStrategy] = []

    def __enter__(self) -> Callable[[List[KeyspaceReplicationStrategy]], None]:
        return self

    def __exit__(self, *exc) -> bool:
        self(self.preserved)
        return False

    def _preserve_replication_strategy(self, keyspace: str):
        if keyspace in [rep.keyspace for rep in self.preserved]:
            return  # already preserved
        create_ks_statement = self.node.run_cqlsh(f"describe {keyspace}").stdout.splitlines()[1]
        replication_strategy = re.search(r".*replication = (\{.*\})", create_ks_statement).group(1)
        self.preserved.append(KeyspaceReplicationStrategy(keyspace, replication_strategy))

    def __call__(self, replication_strategies: List[KeyspaceReplicationStrategy]) -> None:
        for replication_strategy in replication_strategies:
            self._preserve_replication_strategy(replication_strategy.keyspace)
            cql = f"ALTER KEYSPACE {replication_strategy.keyspace} WITH replication = {replication_strategy.strategy}"
            self.node.run_cqlsh(cql)
