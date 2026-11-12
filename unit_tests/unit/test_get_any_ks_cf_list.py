import logging
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sdcm.cluster import BaseCluster


def test_filter_empty_tables_keeps_table_with_rows_but_no_sstable_partitions():
    """SCT-1380: a logstor table has no sstables, so cfstats estimates 0 partitions; a CQL read must decide."""
    rows = [
        SimpleNamespace(keyspace_name="ks", table_name="logstor_tbl", type="int"),
        SimpleNamespace(keyspace_name="ks", table_name="empty_tbl", type="int"),
    ]
    session = MagicMock()
    session.execute.side_effect = lambda query: MagicMock(
        one=MagicMock(return_value=("row",) if "logstor_tbl" in query else None)
    )

    @contextmanager
    def cql_connection_patient(*_, **__):
        yield session

    cluster = BaseCluster.__new__(BaseCluster)
    cluster.log = logging.getLogger(__name__)
    cluster.cql_connection_patient = cql_connection_patient
    db_node = MagicMock()
    db_node._parse_cfstats.return_value = {"Number of partitions (estimate)": 0}

    with patch("sdcm.cluster.PageFetcher") as page_fetcher:
        page_fetcher.return_value.request_all.return_value.all_data.return_value = rows
        assert cluster.get_any_ks_cf_list(db_node) == ["ks.logstor_tbl"]
