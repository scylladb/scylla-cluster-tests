from collections import namedtuple

from sdcm.nemesis import NemesisRunner
from unit_tests.unit.nemesis import TestRunner

SnapshotDetails = namedtuple("SnapshotDetails", ["keyspace_name", "table_name"])


def _make_runner(scylla_version, ks_cf):
    runner = TestRunner()
    runner.target_node.scylla_version = scylla_version
    runner.cluster.get_any_ks_cf_list.return_value = ks_cf
    return runner


def test_validate_snapshot_cf_snapshot_skips_table_listing():
    """A "-cf" snapshot takes the expected tables from the command, so the cluster-wide listing is not needed."""
    runner = _make_runner(scylla_version="2026.2.3", ks_cf=[])
    snapshot_content = [SnapshotDetails("ks", "t1"), SnapshotDetails("ks", "t2")]
    NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot ks -cf t1,t2", snapshot_content=snapshot_content)
    runner.cluster.get_any_ks_cf_list.assert_not_called()
