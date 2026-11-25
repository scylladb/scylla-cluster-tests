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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for the Grafana dashboard API payload helpers.

The server-side behaviour these helpers are built around is asserted against real Grafana
containers in ``unit_tests/integration/test_grafana_api.py``; here we only pin the pure
payload/uid logic and the endpoint choice made by ``upload_dashboard``.
"""

import http
import json

import pytest
import requests

from sdcm.utils.common import get_data_dir_path
from sdcm.utils.grafana_api import (
    GRAFANA_ANNOTATIONS_API_PATH,
    GRAFANA_DASHBOARD_API_PATH,
    GRAFANA_LEGACY_DASHBOARD_API_PATH,
    GRAFANA_SEARCH_API_PATH,
    GRAFANA_UID_MAX_LENGTH,
    convert_dashboard_payload_to_new_api,
    dashboard_uid_from_payload,
    dashboard_upsert_url,
    sanitize_dashboard_uid,
    upload_dashboard,
)

BASE_URL = "http://grafana.example:3000"


class FakeResponse:
    """Minimal response carrying only the status code and text the helpers read."""

    def __init__(self, status_code: int):
        self.status_code = status_code
        self.text = ""

    @property
    def ok(self):
        """True for any status below 400, as requests.Response.ok."""
        return self.status_code < http.HTTPStatus.BAD_REQUEST


class FakeSession:
    """Records the requests made, so tests can assert which endpoint was used."""

    def __init__(self, put_status: int = http.HTTPStatus.OK, post_status: int = http.HTTPStatus.OK):
        self.put_status = put_status
        self.post_status = post_status
        self.calls: list[tuple[str, str, dict]] = []

    def put(self, url, **kwargs):
        """Record a PUT and answer with put_status."""
        self.calls.append(("PUT", url, kwargs))
        return FakeResponse(self.put_status)

    def post(self, url, **kwargs):
        """Record a POST and answer with post_status."""
        self.calls.append(("POST", url, kwargs))
        return FakeResponse(self.post_status)


def test_payload_is_wrapped_into_metadata_and_spec():
    """convert_dashboard_payload_to_new_api moves title and uid into metadata and spec."""
    converted = convert_dashboard_payload_to_new_api({"dashboard": {"title": "some dash", "uid": "dash-uid"}})

    assert converted["metadata"]["name"] == "dash-uid"
    assert converted["spec"]["title"] == "some dash"
    # Grafana rejects a payload whose spec.uid disagrees with metadata.name
    assert converted["spec"]["uid"] == converted["metadata"]["name"]


def test_payload_without_dashboard_wrapper_is_accepted():
    """A bare dashboard model without the ``dashboard`` wrapper converts the same way."""
    converted = convert_dashboard_payload_to_new_api({"title": "bare dash", "uid": "bare-uid"})

    assert converted["metadata"]["name"] == "bare-uid"
    assert converted["spec"]["title"] == "bare dash"


def test_folder_uid_is_carried_over_as_an_annotation():
    """The payload folderUid becomes the grafana.app/folder annotation."""
    converted = convert_dashboard_payload_to_new_api({"dashboard": {"uid": "u"}, "folderUid": "folder-1"})

    assert converted["metadata"]["annotations"]["grafana.app/folder"] == "folder-1"


def test_name_is_always_set_so_uploads_can_be_replayed():
    """``generateName`` would mint a new dashboard per upload; a stable name updates in place."""
    converted = convert_dashboard_payload_to_new_api({"dashboard": {"title": "no uid here"}})

    assert "generateName" not in converted["metadata"]
    assert converted["metadata"]["name"]


def test_uid_is_derived_from_the_title_and_is_stable():
    """dashboard_uid_from_payload is deterministic per title and differs between titles."""
    payload = {"dashboard": {"title": "Scylla Per Server Metrics"}}

    assert dashboard_uid_from_payload(payload) == dashboard_uid_from_payload(payload)
    assert dashboard_uid_from_payload(payload) != dashboard_uid_from_payload({"dashboard": {"title": "other"}})


def test_uid_from_payload_prefers_an_explicit_uid():
    """An explicit dashboard uid wins over the uid derived from the title."""
    assert dashboard_uid_from_payload({"dashboard": {"title": "t", "uid": "explicit"}}) == "explicit"


@pytest.mark.parametrize(
    ("uid", "expected"),
    [
        pytest.param("plain-uid_1", "plain-uid_1", id="plain"),
        pytest.param("has.dots", "has-dots", id="dots"),
        pytest.param("has spaces", "has-spaces", id="spaces"),
        pytest.param("weird/chars:here", "weird-chars-here", id="slashes-and-colons"),
    ],
)
def test_illegal_uid_characters_are_replaced(uid, expected):
    """Grafana answers 400 'uid contains illegal characters' for anything outside [a-zA-Z0-9_-]."""
    assert sanitize_dashboard_uid(uid) == expected


def test_overlong_uid_is_truncated_but_stays_unique():
    """Grafana caps the uid at 40 chars ('uid too long'), so long uids must not simply collide."""
    first = sanitize_dashboard_uid("x" * 60 + "-one")
    second = sanitize_dashboard_uid("x" * 60 + "-two")

    assert len(first) <= GRAFANA_UID_MAX_LENGTH
    assert len(second) <= GRAFANA_UID_MAX_LENGTH
    assert first != second


def test_derived_uid_fits_grafana_limits_for_the_real_dashboard():
    """The uid derived from the shipped nemesis dashboard fits Grafana's length and charset rules."""
    with open(get_data_dir_path("scylla-dash-per-server-nemesis.master.json"), encoding="utf-8") as dashboard_file:
        payload = json.load(dashboard_file)

    uid = dashboard_uid_from_payload(payload)
    assert len(uid) <= GRAFANA_UID_MAX_LENGTH
    assert sanitize_dashboard_uid(uid) == uid


def test_upsert_url_targets_the_named_resource():
    """dashboard_upsert_url appends the dashboard uid to the new API path."""
    assert dashboard_upsert_url(BASE_URL, "some-uid") == f"{BASE_URL}{GRAFANA_DASHBOARD_API_PATH}/some-uid"


def test_upload_uses_put_on_the_new_api():
    """PUT upserts; a POST to the collection would answer 409 on the second upload."""
    session = FakeSession()

    response = upload_dashboard(BASE_URL, {"dashboard": {"uid": "dash-1"}}, session=session)

    assert response.ok
    method, url, kwargs = session.calls[0]
    assert method == "PUT"
    assert url == f"{BASE_URL}{GRAFANA_DASHBOARD_API_PATH}/dash-1"
    assert kwargs["timeout"] > 0, "every grafana request must be bounded"


@pytest.mark.parametrize(
    ("put_status", "reason"),
    [
        pytest.param(
            http.HTTPStatus.NOT_FOUND,
            "grafana < 12 has no /apis surface at all and answers 404 there",
            id="grafana-below-12",
        ),
        pytest.param(
            http.HTTPStatus.SERVICE_UNAVAILABLE,
            "the apiserver behind /apis is registered but not ready yet",
            id="apis-not-ready",
        ),
    ],
)
def test_upload_falls_back_to_the_legacy_endpoint_when_apis_cannot_serve_it(put_status, reason):
    """upload_dashboard retries on the legacy endpoint when PUT on /apis answers 404 or 503."""
    session = FakeSession(put_status=put_status)

    response = upload_dashboard(BASE_URL, {"dashboard": {"uid": "dash-1"}}, session=session)

    assert response.ok, reason
    assert [call[0] for call in session.calls] == ["PUT", "POST"]
    assert session.calls[1][1] == f"{BASE_URL}{GRAFANA_LEGACY_DASHBOARD_API_PATH}"
    # the legacy endpoint needs the overwrite flag to accept a re-upload
    assert session.calls[1][2]["json"]["overwrite"] is True


def test_upload_does_not_fall_back_on_other_errors():
    """A 500 means the new API is there but unhappy; retrying on the legacy path would mask it."""
    session = FakeSession(put_status=http.HTTPStatus.INTERNAL_SERVER_ERROR)

    response = upload_dashboard(BASE_URL, {"dashboard": {"uid": "dash-1"}}, session=session)

    assert not response.ok
    assert [call[0] for call in session.calls] == ["PUT"]


def test_upload_uses_a_retry_session_by_default(monkeypatch):
    """upload_dashboard builds its session with create_retry_session when none is passed."""
    created = []

    def fake_create_retry_session(*args, **kwargs):
        session = FakeSession()
        created.append(session)
        return session

    monkeypatch.setattr("sdcm.utils.grafana_api.create_retry_session", fake_create_retry_session)
    upload_dashboard(BASE_URL, {"dashboard": {"uid": "dash-1"}})

    assert created, "upload_dashboard must not issue bare requests calls"


def test_annotations_and_search_remain_on_the_legacy_api():
    """No /apis replacement exists for these, so they must not be migrated."""
    assert GRAFANA_ANNOTATIONS_API_PATH == "/api/annotations"
    assert GRAFANA_SEARCH_API_PATH == "/api/search"


def test_dashboard_api_path_targets_v1beta1():
    """v1 does not exist before Grafana 13 and the v2 schemas reject the legacy dashboard model."""
    assert GRAFANA_DASHBOARD_API_PATH.startswith("/apis/dashboard.grafana.app/v1beta1/")


def test_requests_connection_error_is_left_for_the_caller():
    """The grafana-startup wait loop in cluster.py relies on seeing ConnectionError."""

    class ExplodingSession(FakeSession):
        def put(self, url, **kwargs):
            raise requests.ConnectionError("connection refused")

    with pytest.raises(requests.ConnectionError):
        upload_dashboard(BASE_URL, {"dashboard": {"uid": "u"}}, session=ExplodingSession())
