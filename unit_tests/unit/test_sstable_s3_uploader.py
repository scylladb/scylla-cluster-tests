# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Tests for the S3 uploads which stream the files over an SSH session to the node."""

from unittest.mock import MagicMock, Mock, patch

from sdcm.cluster import BaseNode
from sdcm.cluster_k8s import BasePodContainer
from sdcm.utils.sstable.s3_uploader import upload_sstables_to_s3, upload_system_table_to_s3

TEST_ID = "4352b7b6-0630-4ceb-ac5b-3ba5bd44ceba"
S3_LINK = "https://cloudius-jenkins-test.s3.amazonaws.com/fake-upload.tar.gz"
SNAPSHOT_PATH = "/var/lib/scylla/data/keyspace1/standard1-abcd/snapshots/sct-20260802_221441"
SSH_LOGIN_INFO = {"hostname": "10.0.0.1", "user": "scylla-test", "key_file": "~/.ssh/scylla_test_id_ed25519"}


def make_remoter() -> MagicMock:
    """A remoter which records every command and answers the two commands whose output is read."""

    def run(cmd, **_):
        if cmd.startswith("stat -c%s"):
            # NOTE: bigger than the 100 bytes the uploader considers an empty table
            return Mock(stdout="2048", ok=True)
        if cmd.startswith("find "):
            return Mock(stdout=SNAPSHOT_PATH, ok=True)
        return Mock(stdout="", ok=True)

    remoter = MagicMock()
    remoter.run.side_effect = run
    return remoter


def make_db_node() -> Mock:
    node = Mock(spec=BaseNode)
    node.name = "db-node-1"
    node.remoter = make_remoter()
    node.ssh_login_info = SSH_LOGIN_INFO
    return node


def commands_run(node: Mock) -> list[str]:
    return [call.args[0] for call in node.remoter.run.call_args_list]


def test_k8s_nodes_report_being_docker():
    """The callers skip these uploads on 'is_docker()' alone, which is what covers k8s as well."""
    assert BasePodContainer.is_docker() is True


def test_upload_system_table_to_s3_uploads_from_ssh_node():
    node = make_db_node()

    with patch(
        "sdcm.utils.sstable.s3_uploader.upload_remote_files_directly_to_s3", return_value=S3_LINK
    ) as upload_mock:
        s3_link, s3_filename = upload_system_table_to_s3(
            node=node, table_name="system.compaction_history", test_id=TEST_ID
        )

    assert s3_link == S3_LINK
    assert s3_filename.startswith("system_compaction_history-")
    upload_mock.assert_called_once()
    node._gen_cqlsh_cmd.assert_called_once_with(
        command="SELECT JSON * FROM system.compaction_history", keyspace=None, timeout=300, connect_timeout=60
    )


def test_upload_sstables_to_s3_uploads_from_ssh_node():
    node = make_db_node()

    with patch(
        "sdcm.utils.sstable.s3_uploader.upload_remote_files_directly_to_s3", return_value=S3_LINK
    ) as upload_mock:
        s3_link = upload_sstables_to_s3(node=node, keyspace="keyspace1", test_id=TEST_ID)

    assert s3_link == S3_LINK
    assert upload_mock.call_args.args[1] == [SNAPSHOT_PATH]
    commands = commands_run(node)
    assert any(cmd.startswith("nodetool snapshot -t sct-") for cmd in commands)
    assert any(cmd.startswith("find /var/lib/scylla/data ") for cmd in commands)
    assert any(cmd.startswith("nodetool clearsnapshot -t sct-") for cmd in commands)
