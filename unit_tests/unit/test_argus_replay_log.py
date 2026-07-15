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

"""Unit tests for the Argus request replay log (argus/client/replay_log.py)
and the SCT integration that replaces MagicMock with replay-only mode.

argus-alm 0.16.0 replaced the old queue + background-writer-thread design
(ReplayRecord built in memory, enqueued via a ReplayLog.record() context
manager) with synchronous, lock-serialized writes via ReplayLog.write() -
the caller passes the completed outcome (success/error) directly and the
record is on disk before write() returns. See replay_log.py's module
docstring for the rationale. There is no ReplayRecord class anymore and no
"give the writer thread time to flush" delay needed in these tests.
"""

import json
import sys
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from argus.client.replay_log import ReplayLog, ReplayLogOnlyResponse
from argus.client.sct.client import ArgusSCTClient
from sdcm.utils.argus import ReplayOnlyArgusSCTClient


@pytest.fixture
def log_dir(tmp_path):
    """Provide a temporary directory for replay log files."""
    return str(tmp_path)


class TestReplayLog:
    def test_creates_file_with_correct_name(self, log_dir):
        replay_log = ReplayLog(
            log_dir=log_dir, run_id="550e8400-e29b-41d4-a716-446655440000", test_type="scylla-cluster-tests"
        )
        try:
            assert replay_log.path.parent == Path(log_dir)
            assert "argus_replay_log_" in replay_log.path.name
            assert "550e8400-e29b-41d4-a716-446655440000" in replay_log.path.name
            assert replay_log.path.suffix == ".jsonl"
        finally:
            replay_log.close()

    def test_sanitizes_run_id(self, log_dir):
        replay_log = ReplayLog(log_dir=log_dir, run_id="../../evil/path", test_type="scylla-cluster-tests")
        try:
            # Slashes and dots should be replaced with underscores
            assert "/" not in replay_log.path.name
            assert ".." not in replay_log.path.name.replace(".jsonl", "")
        finally:
            replay_log.close()

    def test_writes_jsonl_format(self, log_dir):
        replay_log = ReplayLog(log_dir=log_dir, run_id="test-run-1", test_type="scylla-cluster-tests")
        try:
            replay_log.write("POST", "/sct/$id/event/submit", {"id": "run-1"}, None, {"data": "event1"}, success=True)
        finally:
            replay_log.close()

        lines = replay_log.path.read_text().strip().split("\n")
        assert len(lines) == 1

        outcome = json.loads(lines[0])
        assert outcome["method"] == "POST"
        assert outcome["endpoint"] == "/sct/$id/event/submit"
        assert outcome["location_params"] == {"id": "run-1"}
        assert outcome["body"] == {"data": "event1"}
        assert outcome["success"] is True
        assert outcome["test_type"] == "scylla-cluster-tests"
        assert "error" not in outcome  # omitted (not null) when there is none

    def test_writes_failure_with_error_message(self, log_dir):
        """write(success=False, error=...) records the caller's failure verdict verbatim."""
        replay_log = ReplayLog(log_dir=log_dir, run_id="test-run-2", test_type="scylla-cluster-tests")
        try:
            replay_log.write(
                "POST",
                "/sct/$id/nemesis/submit",
                {"id": "run-2"},
                None,
                {"nemesis": "stop"},
                success=False,
                error="ConnectionError: Connection refused",
            )
        finally:
            replay_log.close()

        lines = replay_log.path.read_text().strip().split("\n")
        assert len(lines) == 1

        outcome = json.loads(lines[0])
        assert outcome["success"] is False
        assert outcome["error"] == "ConnectionError: Connection refused"

    def test_write_is_synchronous(self, log_dir):
        """write() has no background writer thread or queue - by the time it returns, the
        record is already flushed to disk, with no delay needed before reading it back."""
        replay_log = ReplayLog(log_dir=log_dir, run_id="sync-test", test_type="scylla-cluster-tests")
        try:
            replay_log.write("POST", "/testrun/$type/submit", {"type": "sct"}, None, {"run_id": "x"}, success=True)
            lines = replay_log.path.read_text().strip().split("\n")  # no sleep before reading
        finally:
            replay_log.close()

        assert len(lines) == 1
        outcome = json.loads(lines[0])
        assert outcome["success"] is True
        assert outcome["body"] == {"run_id": "x"}

    def test_thread_safety(self, log_dir):
        """Multiple threads writing concurrently should produce valid, non-interleaved JSONL."""
        replay_log = ReplayLog(log_dir=log_dir, run_id="thread-test", test_type="scylla-cluster-tests")
        num_threads = 8
        records_per_thread = 20
        barrier = threading.Barrier(num_threads)

        def writer(thread_id):
            barrier.wait()
            for i in range(records_per_thread):
                replay_log.write(
                    "POST",
                    "/sct/$id/event/submit",
                    {"id": "run-1"},
                    None,
                    {"thread": thread_id, "seq": i},
                    success=True,
                )

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        replay_log.close()

        lines = replay_log.path.read_text().strip().split("\n")
        # One write() call per operation, serialized by the log's own lock.
        assert len(lines) == num_threads * records_per_thread

        # Every line should be valid, non-interleaved JSON.
        for line in lines:
            record = json.loads(line)
            assert record["method"] == "POST"

    def test_close_is_idempotent(self, log_dir):
        replay_log = ReplayLog(log_dir=log_dir, run_id="close-test", test_type="scylla-cluster-tests")
        replay_log.close()
        replay_log.close()  # Should not raise

    def test_context_manager(self, log_dir):
        with ReplayLog(log_dir=log_dir, run_id="ctx-test", test_type="scylla-cluster-tests") as replay_log:
            replay_log.write(
                "POST",
                "/testrun/$type/submit",
                {"type": "scylla-cluster-tests"},
                None,
                {"run_id": "abc"},
                success=True,
            )

        assert len(replay_log.path.read_text().strip().split("\n")) == 1

    def test_write_after_close_is_dropped_not_raised(self, log_dir):
        """A write() racing (or simply arriving after) close() must never raise - the
        record for that one call is just dropped, not worth blocking shutdown to drain."""
        replay_log = ReplayLog(log_dir=log_dir, run_id="post-close-test", test_type="scylla-cluster-tests")
        replay_log.close()

        replay_log.write("POST", "/sct/$id/event/submit", {"id": "run-1"}, None, {}, success=True)

        assert replay_log.path.read_text() == ""

    def test_write_when_log_dir_unwritable_is_dropped_not_raised(self, tmp_path):
        """If the log file can't be opened (e.g. bad log_dir), the instance still constructs
        successfully and write()/close() are silent no-ops - a broken replay log must never
        prevent the real Argus client from being created or making its real HTTP calls."""
        blocking_file = tmp_path / "not_a_directory"
        blocking_file.write_text("")  # a plain file, so mkdir() underneath it must fail

        replay_log = ReplayLog(log_dir=blocking_file / "nested", run_id="broken-dir-test", test_type="sct")
        try:
            replay_log.write("POST", "/sct/$id/event/submit", {"id": "run-1"}, None, {}, success=True)
        finally:
            replay_log.close()


class TestArgusClientReplayOnly:
    def test_replay_only_post_returns_stub_response(self, log_dir):
        client = ArgusSCTClient(
            run_id="test-uuid-1234",
            auth_token="",
            base_url="",
            log_dir=log_dir,
            replay_log_only=True,
        )
        try:
            response = client.post(
                endpoint="/sct/$id/event/submit",
                location_params={"id": "test-uuid-1234"},
                body={"data": "test"},
            )
            assert isinstance(response, ReplayLogOnlyResponse)
            assert response.status_code == 200
            assert response.json() == {"status": "ok", "response": {}}
        finally:
            client.close()

    def test_replay_only_get_returns_stub(self, log_dir):
        """In replay-log-only mode, GET returns a stub instead of making a call."""
        client = ArgusSCTClient(
            run_id="test-uuid-1234",
            auth_token="",
            base_url="",
            log_dir=log_dir,
            replay_log_only=True,
        )
        try:
            response = client.get(
                endpoint="/testrun/$type/$id/get",
                location_params={"type": "scylla-cluster-tests", "id": "test-uuid-1234"},
            )
            assert isinstance(response, ReplayLogOnlyResponse)
            assert response.json() == {"status": "ok", "response": {}}
        finally:
            client.close()

    def test_replay_only_writes_jsonl(self, log_dir):
        client = ArgusSCTClient(
            run_id="test-uuid-5678",
            auth_token="",
            base_url="",
            log_dir=log_dir,
            replay_log_only=True,
        )
        try:
            # Call a high-level method that uses post()
            client.submit_sct_run(
                job_name="test-job",
                job_url="http://jenkins/job/1",
                started_by="test-user",
                commit_id="abc123",
                origin_url="https://github.com/org/repo",
                branch_name="master",
                sct_config={"param": "value"},
            )
        finally:
            client.close()

        # Find the replay log file
        jsonl_files = list(Path(log_dir).glob("argus_replay_log_*.jsonl"))
        assert len(jsonl_files) == 1

        lines = jsonl_files[0].read_text().strip().split("\n")
        assert len(lines) == 1

        outcome = json.loads(lines[0])
        assert outcome["method"] == "POST"
        assert outcome["endpoint"] == "/testrun/$type/submit"
        assert outcome["body"]["job_name"] == "test-job"
        # In replay-log-only mode the HTTP call is skipped, so "not sent" means "not successful".
        assert outcome["success"] is False
        assert "error" not in outcome

    def test_replay_only_no_session_created(self, log_dir):
        client = ArgusSCTClient(
            run_id="test-uuid-nosess",
            auth_token="",
            base_url="",
            log_dir=log_dir,
            replay_log_only=True,
        )
        try:
            assert client.session is None
        finally:
            client.close()

    def test_replay_only_attribute(self, log_dir):
        client = ReplayOnlyArgusSCTClient(run_id="test-uuid-attr", log_dir=log_dir)
        try:
            assert isinstance(client, ReplayOnlyArgusSCTClient)
            assert client.session is None
        finally:
            client.close()

    def test_normal_mode_replay_only_is_false(self, log_dir):
        client = ArgusSCTClient(
            run_id="test-uuid-normal",
            auth_token="fake-token",
            base_url="http://localhost:9999",
            log_dir=log_dir,
            replay_log_only=False,
        )
        try:
            assert not isinstance(client, ReplayOnlyArgusSCTClient)
            assert client.session is not None
        finally:
            client.close()

    def test_check_response_with_replay_stub(self, log_dir):
        """check_response should not raise on a ReplayLogOnlyResponse stub."""
        client = ArgusSCTClient(
            run_id="test-uuid-check",
            auth_token="",
            base_url="",
            log_dir=log_dir,
            replay_log_only=True,
        )
        try:
            response = ReplayLogOnlyResponse(endpoint="/testrun/$type/$id/get")
            # Should not raise
            client.check_response(response)
        finally:
            client.close()


class TestArgusClientNormalModeWithReplayLog:
    """Test that the replay log works in normal (non-replay-only) mode."""

    def test_post_records_to_replay_log_on_success(self, log_dir):
        """When a POST succeeds, the replay log should record success=True."""
        client = ArgusSCTClient(
            run_id="test-uuid-success",
            auth_token="fake-token",
            base_url="http://localhost:9999",
            log_dir=log_dir,
            replay_log_only=False,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.url = "http://localhost:9999/api/v1/client/sct/test-uuid-success/event/submit"
        mock_response.json.return_value = {"status": "ok", "response": {}}

        try:
            with patch.object(client.session, "post", return_value=mock_response):
                client.post(
                    endpoint="/sct/$id/event/submit",
                    location_params={"id": "test-uuid-success"},
                    body={"data": "event"},
                )
        finally:
            client.close()

        jsonl_files = list(Path(log_dir).glob("argus_replay_log_*.jsonl"))
        assert len(jsonl_files) == 1
        lines = jsonl_files[0].read_text().strip().split("\n")
        assert len(lines) == 1
        outcome = json.loads(lines[-1])
        assert outcome["success"] is True
        assert outcome["endpoint"] == "/sct/$id/event/submit"

    def test_post_records_to_replay_log_on_http_error(self, log_dir):
        """When a POST gets an HTTP error, the replay log should record success=False."""
        client = ArgusSCTClient(
            run_id="test-uuid-httperr",
            auth_token="fake-token",
            base_url="http://localhost:9999",
            log_dir=log_dir,
            replay_log_only=False,
        )
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.ok = False
        mock_response.url = "http://localhost:9999/api/v1/client/sct/test-uuid-httperr/event/submit"
        mock_response.json.return_value = {"status": "error", "response": {"arguments": ["Internal error"]}}

        try:
            with patch.object(client.session, "post", return_value=mock_response):
                client.post(
                    endpoint="/sct/$id/event/submit",
                    location_params={"id": "test-uuid-httperr"},
                    body={"data": "event"},
                )
        finally:
            client.close()

        jsonl_files = list(Path(log_dir).glob("argus_replay_log_*.jsonl"))
        assert len(jsonl_files) == 1
        lines = jsonl_files[0].read_text().strip().split("\n")
        assert len(lines) == 1
        outcome = json.loads(lines[-1])
        assert outcome["success"] is False
        assert "HTTP 500" in outcome["error"]

    def test_post_records_to_replay_log_on_connection_error(self, log_dir):
        """When a POST raises a ConnectionError, the replay log should capture it."""
        client = ArgusSCTClient(
            run_id="test-uuid-connerr",
            auth_token="fake-token",
            base_url="http://localhost:9999",
            log_dir=log_dir,
            replay_log_only=False,
        )

        try:
            with patch.object(client.session, "post", side_effect=ConnectionError("Connection refused")):
                with pytest.raises(ConnectionError):
                    client.post(
                        endpoint="/sct/$id/event/submit",
                        location_params={"id": "test-uuid-connerr"},
                        body={"data": "event"},
                    )
        finally:
            client.close()

        jsonl_files = list(Path(log_dir).glob("argus_replay_log_*.jsonl"))
        assert len(jsonl_files) == 1
        lines = jsonl_files[0].read_text().strip().split("\n")
        assert len(lines) == 1
        outcome = json.loads(lines[-1])
        assert outcome["success"] is False
        assert "ConnectionError" in outcome["error"]

    def test_post_records_logical_error_from_argus(self, log_dir):
        """When Argus returns HTTP 200 but status != 'ok', record as failure."""
        client = ArgusSCTClient(
            run_id="test-uuid-logical",
            auth_token="fake-token",
            base_url="http://localhost:9999",
            log_dir=log_dir,
            replay_log_only=False,
        )
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.ok = True
        mock_response.url = "http://localhost:9999/api/v1/client/sct/test-uuid-logical/event/submit"
        mock_response.json.return_value = {"status": "error", "response": {"arguments": ["Run not found"]}}

        try:
            with patch.object(client.session, "post", return_value=mock_response):
                client.post(
                    endpoint="/sct/$id/event/submit",
                    location_params={"id": "test-uuid-logical"},
                    body={"data": "event"},
                )
        finally:
            client.close()

        jsonl_files = list(Path(log_dir).glob("argus_replay_log_*.jsonl"))
        assert len(jsonl_files) == 1
        lines = jsonl_files[0].read_text().strip().split("\n")
        assert len(lines) == 1
        outcome = json.loads(lines[-1])
        assert outcome["success"] is False


class TestLogCollectorRegistration:
    """Verify that argus_replay_log_*.jsonl is registered for collection."""

    def test_replay_log_in_log_entities(self):
        # Import the FileLog class directly to avoid circular import issues
        # with sdcm.monitorstack. We verify the log_entities list by importing
        # the module with the monitorstack dependency mocked out.
        mock_monitoring = MagicMock()
        with patch.dict(sys.modules, {"sdcm.monitorstack": mock_monitoring, "sdcm.monitorstack.ui": mock_monitoring}):
            # Force re-import if already cached
            if "sdcm.logcollector" in sys.modules:
                del sys.modules["sdcm.logcollector"]
            # cyclic-import: sdcm.logcollector imports sdcm.monitorstack.ui which
            # re-imports sdcm.logcollector; mocking the monitorstack modules above
            # breaks the cycle so we can import here safely.
            try:
                from sdcm.logcollector import BaseSCTLogCollector  # noqa: PLC0415

                entity_names = [e.name for e in BaseSCTLogCollector.log_entities]
                assert "argus_replay_log_*.jsonl" in entity_names
            finally:
                # Don't leak a logcollector imported under mocked monitorstack
                # into sys.modules; later tests must re-import it cleanly.
                sys.modules.pop("sdcm.logcollector", None)
