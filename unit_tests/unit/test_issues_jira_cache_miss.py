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

"""A Jira issue missing from the S3 cache must be fetched live, not dropped as `None`.

`SkipPerIssues` filters out `None`, so a silent miss turns into "issue not open" and the
skip is not applied.
"""

from types import SimpleNamespace

from sdcm.utils.issues import Issue, JiraIssueRetriever


def test_jira_cache_miss_falls_back_to_live_api(monkeypatch):
    live_issue = SimpleNamespace(
        key="SCT-999999",
        fields=SimpleNamespace(summary="live", status=SimpleNamespace(name="Open"), labels=["sct-2026.1-skip"]),
    )
    retriever = JiraIssueRetriever()
    monkeypatch.setattr(retriever.s3_cache, "get_project", lambda project: {})
    monkeypatch.setitem(retriever.__dict__, "jira", SimpleNamespace(issue=lambda issue_id, expand: live_issue))

    assert retriever.get_issue("SCT-999999") == Issue(
        number="SCT-999999", state="open", labels=["sct-2026.1-skip"], title="live"
    )
