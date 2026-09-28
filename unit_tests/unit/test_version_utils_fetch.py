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

"""Repository lookups at config time must survive downloads.scylladb.com throttling (SCT-1153).

Served by a real local HTTP server, so urllib3's retry logic runs as it does against the real repository.
"""

import gzip
import socket
import threading
import time
from unittest.mock import MagicMock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from sdcm.utils import version_utils
from sdcm.utils.repo_parser import Parser


class _ScriptedHandler(BaseHTTPRequestHandler):
    """Answers each request to a path with the next action of the server's script, the last one repeating."""

    def do_GET(self):
        server = self.server
        with server.lock:
            path_hits = server.path_hits.get(self.path, 0)
            server.path_hits[self.path] = path_hits + 1
            server.hits += 1
            action = server.script[min(path_hits, len(server.script) - 1)]
        if time.monotonic() < server.throttled_until:
            action = "503"
        if action == "reset":
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
            self.connection.close()
            return
        if action == "stall":  # outlasts the client's read timeout, so it has gone before an answer
            time.sleep(2)
            return
        status = int(action) if action.isdigit() else 503 if action == "retry-after" else 200
        body = server.files.get(self.path, b"repo-content") if status == 200 else b"error"
        self.send_response(status)
        if action == "retry-after":
            self.send_header("Retry-After", "60")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


@pytest.fixture(name="repo_server")
def fixture_repo_server(monkeypatch):
    monkeypatch.setattr(version_utils, "SCYLLA_URL_REQUEST_BACKOFF", 0)
    version_utils.get_url_content.cache_clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ScriptedHandler)
    server.lock = threading.Lock()
    server.hits = 0
    server.path_hits = {}
    server.files = {}
    server.script = ["200"]
    server.throttled_until = 0
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    server.base_url = f"http://127.0.0.1:{server.server_address[1]}"
    server.url = f"{server.base_url}/scylla.repo"
    yield server
    server.shutdown()
    server.server_close()
    version_utils.get_url_content.cache_clear()


@pytest.mark.parametrize(
    "script",
    [
        pytest.param(["503", "503", "200"], id="throttled-with-503"),
        pytest.param(["429", "200"], id="too-many-requests"),
        pytest.param(["reset", "reset", "200"], id="connection-reset"),
    ],
)
def test_transient_failures_are_retried(repo_server, script):
    repo_server.script = script

    assert version_utils.get_url_content(repo_server.url) == ["repo-content"]
    assert repo_server.hits == len(script)


def test_throttling_outlasting_a_second_is_ridden_out(repo_server, monkeypatch):
    """What failed runs: the old 10 retries 0.1s apart were all spent within a second of throttling."""
    monkeypatch.setattr(version_utils, "SCYLLA_URL_REQUEST_BACKOFF", 1)  # the real backoff
    repo_server.throttled_until = time.monotonic() + 1.5

    assert version_utils.get_url_content(repo_server.url) == ["repo-content"]


def test_stalled_request_times_out_and_is_retried(repo_server, monkeypatch):
    monkeypatch.setattr(version_utils, "SCYLLA_URL_REQUEST_TIMEOUT", (1, 0.5))
    repo_server.script = ["stall", "200"]

    assert version_utils.get_url_content(repo_server.url) == ["repo-content"]
    assert repo_server.hits == 2


def test_retries_are_bounded(repo_server):
    repo_server.script = ["503"]

    with pytest.raises(requests.exceptions.RetryError):
        version_utils.get_url_content(repo_server.url)
    assert repo_server.hits == version_utils.SCYLLA_URL_REQUEST_RETRIES + 1


def test_each_fetch_closes_its_session(repo_server, monkeypatch):
    sessions = []
    create_retry_session = version_utils.create_retry_session

    def tracked_session(**kwargs):
        sessions.append(session := create_retry_session(**kwargs))
        session.close = MagicMock(wraps=session.close)
        return session

    monkeypatch.setattr(version_utils, "create_retry_session", tracked_session)

    assert version_utils.get_url_bytes(repo_server.url) == b"repo-content"
    assert version_utils.get_url_content(repo_server.url) == ["repo-content"]
    assert len(sessions) == 2
    assert all(session.close.called for session in sessions)


def test_missing_url_fails_without_retrying(repo_server):
    repo_server.script = ["404"]

    with pytest.raises(ValueError, match="is incorrect"):
        version_utils.get_url_content(repo_server.url)
    with pytest.raises(requests.HTTPError):
        version_utils.get_url_bytes(repo_server.url)
    assert repo_server.hits == 2


def test_retry_after_does_not_outlast_the_lookup_timeout(repo_server):
    """A server asking to wait longer than a lookup may take is retried on the backoff instead."""
    repo_server.script = ["retry-after", "200"]
    start = time.monotonic()

    assert version_utils.get_url_content(repo_server.url) == ["repo-content"]
    assert time.monotonic() - start < 10


def test_lookup_timeout_outlasts_the_retries_of_its_requests():
    """The parallel lookup must not give up while one of its requests is still retrying."""
    connect, read = version_utils.SCYLLA_URL_REQUEST_TIMEOUT
    retries = version_utils.SCYLLA_URL_REQUEST_RETRIES
    backoff = sum(2**attempt for attempt in range(1, retries))  # urllib3 sleeps 0, 2, 4 with backoff_factor=1

    assert version_utils.repository_lookup_timeout(sequential_requests=1) > (retries + 1) * (connect + read) + backoff
    assert version_utils.repository_lookup_timeout(sequential_requests=4) == pytest.approx(
        4 * version_utils.repository_lookup_timeout(sequential_requests=1)
    )


def test_yum_lookup_outlasts_every_request_spending_its_retries(repo_server, monkeypatch):
    """Each phase of the lookup retries its requests in turn, so the whole lookup outlasts a single request's retries."""
    monkeypatch.setattr(version_utils, "SCYLLA_URL_REQUEST_TIMEOUT", (0.05, 0.25))
    repo_server.script = ["stall"] * version_utils.SCYLLA_URL_REQUEST_RETRIES + ["200"]
    repo_server.files = {
        "/scylla.repo": f"[scylla]\nbaseurl={repo_server.base_url}/scylladb.com/\n".encode(),
        "/scylladb.com/repodata/repomd.xml": b'<location href="repodata/primary.xml.gz"/>',
        "/scylladb.com/repodata/primary.xml.gz": gzip.compress(
            b'<?xml version="1.0"?><metadata xmlns="http://linux.duke.edu/metadata/common" packages="1">'
            b'<package type="rpm"><name>scylla-server</name><version epoch="0" ver="2026.1.3" rel="0.1"/></package>'
            b"</metadata>"
        ),
    }
    start = time.monotonic()

    assert version_utils.get_branch_version_for_multiple_repositories([repo_server.url]) == ["2026.1.3"]
    # the lookup did outlast one request's retries, which a single-request budget would have cut short
    assert time.monotonic() - start > version_utils.repository_lookup_timeout(sequential_requests=1)


def test_parser_parses_the_given_data():
    primary_xml = (
        b'<?xml version="1.0"?><metadata xmlns="http://linux.duke.edu/metadata/common" packages="1">'
        b'<package type="rpm"><name>scylla</name><version epoch="0" ver="2025.3.8" rel="0.1"/></package>'
        b"</metadata>"
    )

    parser = Parser(url="https://repo.example/primary.xml", data=primary_xml)

    assert [package["name"][0] for package in parser.getList()] == ["scylla"]


def test_single_requests_are_not_given_the_whole_lookup_budget(monkeypatch):
    """repository_lookup_timeout() bounds a parallel lookup, retries included; one request keeps its own timeout."""
    session = MagicMock()
    session.__enter__.return_value = session
    session.get.return_value.content = b"docker-image-name: scylla-nightly:2026.1.0-dev-0.20260928.abcdef"
    monkeypatch.setattr(version_utils, "create_retry_session", lambda **_: session)

    assert version_utils.get_specific_tag_of_docker_image("scylladb/scylla-nightly") == "2026.1.0-dev-0.20260928.abcdef"
    assert session.get.call_args.kwargs["timeout"] == version_utils.SCYLLA_URL_REQUEST_TIMEOUT
