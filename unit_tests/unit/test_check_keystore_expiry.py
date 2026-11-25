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

"""Tests for the keystore expiry check used by the weekly GitHub workflow.

The script lives under ``.github/scripts``, which is not an importable
package, so it is loaded by path.
"""

import datetime
import importlib.util
import io
import json
import urllib.error
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "check_keystore_expiry.py"

script_spec = importlib.util.spec_from_file_location("check_keystore_expiry", SCRIPT_PATH)
check_keystore_expiry = importlib.util.module_from_spec(script_spec)
script_spec.loader.exec_module(check_keystore_expiry)

# Test epoch. Every expectation below is relative to this date, not to the
# real clock, so the boundary cases stay stable as time passes.
TODAY = datetime.date(2026, 9, 22)
WARN_DAYS = 30


def fake_azure(expiry, warning=None):
    """Return an azure_secret_expiry stand-in that always answers ``(expiry, warning)``."""
    return lambda client, name: (expiry, warning)


def http_error(code, body):
    """Build the HTTPError urllib raises for a Graph or token endpoint answering ``code`` with ``body``."""
    return urllib.error.HTTPError("url", code, "err", {}, io.BytesIO(body.encode()))


@pytest.mark.parametrize(
    "raw, expected",
    [
        pytest.param("2027-09-22", datetime.date(2027, 9, 22), id="date-only"),
        pytest.param("  2027-09-22  ", datetime.date(2027, 9, 22), id="date-with-spaces"),
        # what `az ad app credential list` prints for endDateTime
        pytest.param("2027-09-22T05:59:58Z", datetime.date(2027, 9, 22), id="zulu-timestamp"),
        pytest.param("2027-09-22T05:59:58+00:00", datetime.date(2027, 9, 22), id="utc-offset-timestamp"),
        # a non-UTC offset must not shift the date
        pytest.param("2027-09-22T23:30:00+02:00", datetime.date(2027, 9, 22), id="non-utc-offset-keeps-date"),
        pytest.param("", None, id="empty"),
        pytest.param("not-a-date", None, id="garbage"),
    ],
)
def test_parse_expiry(raw, expected):
    """parse_expiry keeps the calendar date of each accepted format and returns None when unparsable."""
    assert check_keystore_expiry.parse_expiry(raw) == expected


@pytest.mark.parametrize(
    "expires_on, expected_status, expected_days",
    [
        # boundaries around the warning window and expiry
        pytest.param("2026-09-21", check_keystore_expiry.STATUS_EXPIRED, -1, id="expired-yesterday"),
        pytest.param("2026-09-22", check_keystore_expiry.STATUS_EXPIRING, 0, id="expires-today"),
        pytest.param("2026-10-22", check_keystore_expiry.STATUS_EXPIRING, 30, id="last-day-of-window"),
        pytest.param("2026-10-23", check_keystore_expiry.STATUS_OK, 31, id="first-day-after-window"),
    ],
)
def test_classify_boundaries(expires_on, expected_status, expected_days):
    """Classify flags expiry and warning-window boundaries exactly at the day count."""
    result = check_keystore_expiry.classify("sct/azure.json", {"expires_on": expires_on}, TODAY, WARN_DAYS)
    assert result["status"] == expected_status
    assert result["days_left"] == expected_days


def test_classify_missing_tag_is_untracked():
    """Classify marks an entry without an expires_on tag as untracked, with no day count."""
    result = check_keystore_expiry.classify("sct/azure.json", {"team": "sct"}, TODAY, WARN_DAYS)
    assert result["status"] == check_keystore_expiry.STATUS_UNTRACKED
    assert result["days_left"] is None


def test_classify_unparsable_tag_is_untracked_and_keeps_raw_value():
    """Classify marks an unparsable expires_on as untracked and keeps the raw value for the report."""
    result = check_keystore_expiry.classify("sct/azure.json", {"expires_on": "whenever"}, TODAY, WARN_DAYS)
    assert result["status"] == check_keystore_expiry.STATUS_UNTRACKED
    assert result["expires_on"] == "whenever"


def test_render_markdown_orders_worst_first_and_counts():
    """render_markdown lists expired first, then expiring, untracked and healthy, with a summary line."""
    results = [
        check_keystore_expiry.classify("sct/ok.json", {"expires_on": "2027-01-01"}, TODAY, WARN_DAYS),
        check_keystore_expiry.classify("sct/untracked.json", {}, TODAY, WARN_DAYS),
        check_keystore_expiry.classify("sct/expired.json", {"expires_on": "2026-01-01"}, TODAY, WARN_DAYS),
        check_keystore_expiry.classify("sct/soon.json", {"expires_on": "2026-10-01"}, TODAY, WARN_DAYS),
    ]
    markdown = check_keystore_expiry.render_markdown(results, WARN_DAYS)

    body = markdown.splitlines()
    names_in_order = [line.split("`")[1] for line in body if line.startswith("| :")]
    assert names_in_order == ["sct/expired.json", "sct/soon.json", "sct/untracked.json", "sct/ok.json"]
    assert "1 expired, 1 expiring within 30 days, 1 untracked, 1 healthy." in markdown


def test_apply_azure_truth_overrides_tag_and_flags_drift(monkeypatch):
    """Azure AD wins over the hand-written tag, and the disagreement is shown."""
    results = [check_keystore_expiry.classify("sct/azure.json", {"expires_on": "2026-12-01"}, TODAY, WARN_DAYS)]
    monkeypatch.setattr(check_keystore_expiry, "azure_secret_expiry", fake_azure(datetime.date(2027, 9, 22)))

    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert results[0]["expires_on"] == "2027-09-22"
    assert results[0]["source"] == "azure"
    assert results[0]["status"] == check_keystore_expiry.STATUS_OK
    assert "tag says 2026-12-01" in results[0]["note"]


def test_apply_azure_truth_fills_in_a_missing_tag(monkeypatch):
    """apply_azure_truth fills in an untagged entry from Azure AD and classifies it."""
    results = [check_keystore_expiry.classify("sct/azure.json", {}, TODAY, WARN_DAYS)]
    monkeypatch.setattr(check_keystore_expiry, "azure_secret_expiry", fake_azure(datetime.date(2026, 10, 1)))

    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert results[0]["status"] == check_keystore_expiry.STATUS_EXPIRING
    assert results[0]["note"] == "no expires_on tag; value read from Azure AD"


def test_apply_azure_truth_surfaces_a_hint_mismatch_warning(monkeypatch):
    """The stored secret not matching any app credential must be visible."""
    results = [check_keystore_expiry.classify("sct/azure.json", {"expires_on": "2027-09-22"}, TODAY, WARN_DAYS)]
    monkeypatch.setattr(
        check_keystore_expiry,
        "azure_secret_expiry",
        fake_azure(datetime.date(2027, 9, 22), "stored secret does not match any credential on the app"),
    )

    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert "does not match any credential" in results[0]["note"]


def test_apply_azure_truth_marks_expired_only_on_a_credential_rejection(monkeypatch):
    """A rejected token request is itself the answer: the secret is dead."""
    results = [check_keystore_expiry.classify("sct/azure.json", {"expires_on": "2027-09-22"}, TODAY, WARN_DAYS)]

    def _rejected(client, name):
        raise http_error(401, '{"error":"invalid_client","error_description":"AADSTS7000222 expired"}')

    monkeypatch.setattr(check_keystore_expiry, "azure_secret_expiry", _rejected)
    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert results[0]["status"] == check_keystore_expiry.STATUS_EXPIRED
    assert "rejected the stored secret" in results[0]["note"]


@pytest.mark.parametrize(
    "raiser",
    [
        pytest.param(lambda: http_error(503, "Service Unavailable"), id="graph-5xx"),
        pytest.param(lambda: http_error(403, "Forbidden"), id="graph-403"),
        pytest.param(lambda: urllib.error.URLError("dns failure"), id="dns"),
        pytest.param(lambda: TimeoutError("read timed out"), id="read-timeout"),
        pytest.param(lambda: OSError("connection reset"), id="socket"),
        pytest.param(lambda: json.JSONDecodeError("bad", "", 0), id="unparsable-response"),
    ],
)
def test_apply_azure_truth_keeps_the_tag_when_the_lookup_is_unreliable(monkeypatch, raiser):
    """Infrastructure trouble must never be reported as an expired secret."""
    results = [check_keystore_expiry.classify("sct/azure.json", {"expires_on": "2027-09-22"}, TODAY, WARN_DAYS)]

    def _boom(client, name):
        raise raiser()

    monkeypatch.setattr(check_keystore_expiry, "azure_secret_expiry", _boom)
    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert results[0]["status"] == check_keystore_expiry.STATUS_OK
    assert results[0].get("source") != "azure"
    assert "Azure lookup failed" in results[0]["note"]


def test_apply_azure_truth_flags_an_app_with_no_secret(monkeypatch):
    """An Azure AD app with no credential left is reported as expired."""
    results = [check_keystore_expiry.classify("sct/azure.json", {"expires_on": "2027-09-22"}, TODAY, WARN_DAYS)]
    monkeypatch.setattr(check_keystore_expiry, "azure_secret_expiry", fake_azure(None))

    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert results[0]["status"] == check_keystore_expiry.STATUS_EXPIRED


def test_apply_azure_truth_is_a_noop_when_azure_json_is_absent(monkeypatch):
    """Entries other than azure.json never trigger an Azure AD lookup."""
    results = [check_keystore_expiry.classify("sct/docker.json", {"expires_on": "2027-01-01"}, TODAY, WARN_DAYS)]
    monkeypatch.setattr(
        check_keystore_expiry,
        "azure_secret_expiry",
        lambda client, name: pytest.fail("must not be called when there is no azure.json row"),
    )

    check_keystore_expiry.apply_azure_truth(results, client=None, prefix="sct/", today=TODAY, warn_days=WARN_DAYS)

    assert "source" not in results[0]
    assert results[0]["status"] == check_keystore_expiry.STATUS_OK
