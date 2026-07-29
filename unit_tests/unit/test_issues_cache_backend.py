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

"""The issue caches must keep reading from S3 whatever `keystore_backend` says.

`issues/*.csv` are bulk data, not credentials: the largest is over 1 MB (the
Secrets Manager limit is 64 KB) and they are rewritten every 6 hours by
`.github/workflows/cache-issues.yaml` / `cache-jira-issues.yaml`, which upload
to S3 only.  Reading them through Secrets Manager can only ever miss, and both
retrievers swallow a cache miss and fall back to the live API -- so the
regression is silent, and shows up as GitHub rate limiting instead.
"""

import pytest

from sdcm.utils.issues import CachedGitHubIssues, CachedJiraIssues


@pytest.fixture(name="sm_backend_env")
def fixture_sm_backend_env(monkeypatch):
    """Put the process on the secretsmanager backend, as a real run would be."""
    monkeypatch.delenv("SCT_KEYSTORE_BACKEND", raising=False)
    yield


@pytest.mark.parametrize("cache_class", [CachedJiraIssues, CachedGitHubIssues], ids=["jira", "github"])
def test_issue_cache_is_pinned_to_s3(cache_class, sm_backend_env):  # noqa: ARG001
    assert cache_class().storage._backend == "s3"


@pytest.mark.parametrize("cache_class", [CachedJiraIssues, CachedGitHubIssues], ids=["jira", "github"])
def test_issue_cache_ignores_explicit_sm_backend(cache_class, monkeypatch):
    """Even an explicit opt-in to secretsmanager must not redirect the bulk cache."""
    monkeypatch.setenv("SCT_KEYSTORE_BACKEND", "secretsmanager")
    assert cache_class().storage._backend == "s3"
