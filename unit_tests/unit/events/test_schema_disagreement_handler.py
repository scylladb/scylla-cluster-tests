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

"""Tests for the sstables collected when the loaders report a schema disagreement."""

from unittest.mock import MagicMock, patch

from sdcm.sct_events.handlers.schema_disagreement import SchemaDisagreementHandler
from sdcm.sct_events.loaders import CassandraStressLogEvent

S3_LINK = "https://cloudius-jenkins-test.s3.amazonaws.com/fake-upload.tar.gz"
PROXIED_S3_LINK = "https://argus.scylladb.com/api/v1/s3/cloudius-jenkins-test/fake-upload.tar.gz"
SSH_LOGIN_INFO = {"hostname": "10.0.0.1", "user": "scylla-test", "key_file": "~/.ssh/scylla_test_id_ed25519"}


def make_node(name: str, is_docker: bool = False, ssh_login_info: dict | None = SSH_LOGIN_INFO) -> MagicMock:
    node = MagicMock()
    node.name = name
    node.is_docker.return_value = is_docker
    node.ssh_login_info = ssh_login_info
    return node


def handle(nodes: list[MagicMock], link: str = S3_LINK) -> tuple[MagicMock, list[str]]:
    """Runs the handler over the given nodes, returning the upload and the recorded sstable links."""
    tester = MagicMock()
    tester.db_cluster.nodes = nodes
    with (
        patch("sdcm.sct_events.handlers.schema_disagreement.upload_sstables_to_s3", return_value=link) as upload_mock,
        # NOTE: a stub event -- a real one would need the whole event device to be published to
        patch("sdcm.sct_events.handlers.schema_disagreement.SchemaDisagreementErrorEvent") as event_class,
    ):
        SchemaDisagreementHandler().handle(CassandraStressLogEvent.SchemaDisagreement(), tester)
    event = event_class.return_value
    return upload_mock, [call.args[0] for call in event.add_sstable_link.call_args_list]


def test_uploads_only_from_the_nodes_with_ssh_access():
    """The upload opens an SSH session of its own, which the docker, k8s and xcloud nodes have not."""
    docker_node = make_node("docker-node-1", is_docker=True)
    xcloud_node = make_node("xcloud-node-1", ssh_login_info=None)
    ssh_node = make_node("db-node-1")

    upload_mock, links = handle([docker_node, xcloud_node, ssh_node])

    assert [call.args[0] for call in upload_mock.call_args_list] == [ssh_node]
    assert links == [PROXIED_S3_LINK]
    # the gossip info works on those nodes, so the skipped ones must still be asked for it
    docker_node.get_gossip_info.assert_called_once()


def test_records_no_link_when_the_upload_fails():
    _, links = handle([make_node("db-node-1")], link="")

    assert links == []
