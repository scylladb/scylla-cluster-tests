"""
Tests for clear_snapshots function in sdcm.utils.common
"""

from unittest.mock import MagicMock

import pytest

from sdcm.utils.common import clear_snapshots

LISTSNAPSHOTS_OUTPUT = """Snapshot Details:
Snapshot name Keyspace name Column family name True size Size on disk
pre-drop-111 ks1 tbl_view 0 bytes 290 GiB
pre-drop-222 ks1 other 0 bytes 1 GiB
pre-drop-333 ks2 tbl_view 0 bytes 1 GiB
pre-scrub-444 ks1 tbl 0 bytes 1 GiB
pre-scrub-444 ks1 tbl_view 0 bytes 1 GiB
sm_20261004UTC ks1 tbl_view 0 bytes 1 GiB

Total TrueDiskSpaceUsed: 0 bytes
"""


def make_node(listsnapshots_output: str) -> MagicMock:
    node = MagicMock()
    node.run_nodetool.side_effect = lambda sub_cmd, args="": MagicMock(
        stdout=listsnapshots_output if sub_cmd == "listsnapshots" else ""
    )
    return node


def cleared(node: MagicMock) -> list[str]:
    return [c.kwargs["args"] for c in node.run_nodetool.call_args_list if c.args[0] == "clearsnapshot"]


@pytest.mark.parametrize(
    "tag_prefix, keyspace, table, expected",
    [
        pytest.param("pre-drop-", "ks1", "tbl_view", ["-t pre-drop-111 ks1"], id="table"),
        pytest.param("pre-scrub-", "ks1", None, ["-t pre-scrub-444 ks1"], id="keyspace"),
        pytest.param("pre-drop-", "ks3", None, [], id="no_match"),
        pytest.param("pre-drop-222", None, None, ["-t pre-drop-222"], id="full_tag"),
        pytest.param("pre-drop-", None, "tbl_view", ["-t pre-drop-111", "-t pre-drop-333"], id="table_any_keyspace"),
        pytest.param(
            "",
            None,
            None,
            ["-t pre-drop-111", "-t pre-drop-222", "-t pre-drop-333", "-t pre-scrub-444", "-t sm_20261004UTC"],
            id="all",
        ),
    ],
)
def test_clear_snapshots(tag_prefix, keyspace, table, expected):
    node = make_node(LISTSNAPSHOTS_OUTPUT)
    clear_snapshots([node], tag_prefix, keyspace, table)
    assert cleared(node) == expected
    listed = sum(c.args[0] == "listsnapshots" for c in node.run_nodetool.call_args_list)
    assert listed == (2 if expected else 1), "listsnapshots should be logged again only after a clear"


def test_clear_snapshots_without_snapshots():
    node = make_node("There are no snapshots\n")
    clear_snapshots([node], "pre-drop-", "ks1")
    assert cleared(node) == []


def test_clear_snapshots_on_all_nodes():
    nodes = [make_node(LISTSNAPSHOTS_OUTPUT), make_node(LISTSNAPSHOTS_OUTPUT)]
    clear_snapshots(nodes, "pre-drop-", "ks1", "tbl_view")
    for node in nodes:
        assert cleared(node) == ["-t pre-drop-111 ks1"]
