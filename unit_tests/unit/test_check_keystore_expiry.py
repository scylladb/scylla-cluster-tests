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
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / ".github" / "scripts" / "check_keystore_expiry.py"

_spec = importlib.util.spec_from_file_location("check_keystore_expiry", SCRIPT_PATH)
check_keystore_expiry = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_keystore_expiry)

TODAY = datetime.date(2026, 9, 22)
WARN_DAYS = 30


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2027-09-22", datetime.date(2027, 9, 22)),
        ("  2027-09-22  ", datetime.date(2027, 9, 22)),
        # what `az ad app credential list` prints for endDateTime
        ("2027-09-22T05:59:58Z", datetime.date(2027, 9, 22)),
        ("2027-09-22T05:59:58+00:00", datetime.date(2027, 9, 22)),
        ("", None),
        ("not-a-date", None),
    ],
)
def test_parse_expiry(raw, expected):
    assert check_keystore_expiry.parse_expiry(raw) == expected


@pytest.mark.parametrize(
    "expires_on, expected_status, expected_days",
    [
        # boundaries around the warning window and expiry
        ("2026-09-21", check_keystore_expiry.STATUS_EXPIRED, -1),
        ("2026-09-22", check_keystore_expiry.STATUS_EXPIRING, 0),
        ("2026-10-22", check_keystore_expiry.STATUS_EXPIRING, 30),
        ("2026-10-23", check_keystore_expiry.STATUS_OK, 31),
    ],
)
def test_classify_boundaries(expires_on, expected_status, expected_days):
    result = check_keystore_expiry.classify("sct/azure.json", {"expires_on": expires_on}, TODAY, WARN_DAYS)
    assert result["status"] == expected_status
    assert result["days_left"] == expected_days


def test_classify_missing_tag_is_untracked():
    result = check_keystore_expiry.classify("sct/azure.json", {"team": "sct"}, TODAY, WARN_DAYS)
    assert result["status"] == check_keystore_expiry.STATUS_UNTRACKED
    assert result["days_left"] is None


def test_classify_unparsable_tag_is_untracked_and_keeps_raw_value():
    result = check_keystore_expiry.classify("sct/azure.json", {"expires_on": "whenever"}, TODAY, WARN_DAYS)
    assert result["status"] == check_keystore_expiry.STATUS_UNTRACKED
    assert result["expires_on"] == "whenever"


def test_render_markdown_orders_worst_first_and_counts():
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
