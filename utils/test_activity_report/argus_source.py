"""Read-only Argus queries for the test activity report, through the SCT Argus client."""

import logging
import tempfile
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime

from argus.client.base import ArgusClient
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

SCT_PLUGIN = "scylla-cluster-tests"
# SCT sends the actual run cost to Argus since 2026-10-04. Older runs have none.
ACTUAL_COST_SINCE = "2026-10-01"
RUN_FIELDS = (
    "id", "test_id", "release_id", "build_number", "status", "investigation_status",
    "start_time", "end_time", "heartbeat", "started_by", "cloud_setup", "region_name",
)  # fmt: skip


class ArgusReader(ArgusClient):
    """Argus client for the web API routes, which are not under /client."""

    non_client_routes = frozenset(
        {
            "/releases",
            "/groups",
            "/tests",
            "/test/$test_id/runs",
            "/issues/get",
            "/cost/run/$run_id",
            "/release/assignees/tests",
            "/users",
        }
    )

    def __init__(self, workers: int = 6):
        creds = KeyStore().get_argus_rest_credentials_per_provider()
        super().__init__(
            auth_token=creds["token"],
            base_url=creds["baseUrl"],
            extra_headers=creds.get("extra_headers"),
            log_dir=tempfile.mkdtemp(prefix="test-activity-report-"),
        )
        self.workers = workers

    @property
    def base_url(self) -> str:
        return self._base_url

    def fetch(self, endpoint: str, location: dict | None = None, params: dict | None = None):
        response = self.get(endpoint, location_params=location or {}, params=params or {})
        self.check_response(response)
        return response.json()["response"]

    def fetch_many(self, calls: Iterable[tuple[str, dict | None, dict | None]]) -> list:
        """Run the GET calls in parallel and return the responses in the order of the calls."""
        calls = list(calls)
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            return list(pool.map(lambda call: self.fetch(*call), calls))


@dataclass
class ArgusData:
    releases: list[dict]
    tests: list[dict]
    runs: list[dict]
    issues: list[dict]
    actual_costs: dict[str, float]
    assignees: dict[str, list[str]]
    users: dict[str, str]
    last_runs: dict[str, tuple[str, str]] = field(default_factory=dict)


def fetch_tests(reader: ArgusReader) -> tuple[list[dict], list[dict], list[dict]]:
    """Return all releases, groups and tests, the disabled ones included."""
    releases = reader.fetch("/releases", params={"all": "true"})
    groups = [
        group
        for groups in reader.fetch_many(("/groups", None, {"releaseId": rel["id"], "all": "true"}) for rel in releases)
        for group in groups
    ]
    tests = [
        test
        for tests in reader.fetch_many(("/tests", None, {"groupId": group["id"], "all": "true"}) for group in groups)
        for test in tests
    ]
    return releases, groups, tests


def fetch_runs(reader: ArgusReader, tests: list[dict], since: datetime, until: datetime) -> list[dict]:
    """Return the runs of the SCT tests that started in the period, with only the fields the report uses."""
    sct_tests = [test for test in tests if test.get("plugin_name") == SCT_PLUGIN]
    LOGGER.info("Reading the runs of %d SCT tests", len(sct_tests))
    responses = reader.fetch_many(
        ("/test/$test_id/runs", {"test_id": test["id"]}, {"limit": 20000, "full": "true", "after": since.timestamp()})
        for test in sct_tests
    )
    until_iso = until.isoformat()[:19]
    return [
        {name: run.get(name) for name in RUN_FIELDS}
        for runs in responses
        for run in runs
        if run["start_time"][:19] < until_iso
    ]


def fetch_issues(reader: ArgusReader, release_ids: Iterable[str]) -> list[dict]:
    """Return the GitHub and Jira issues with their run links, merged over the releases."""
    issues = {}
    responses = reader.fetch_many(
        ("/issues/get", None, {"filterKey": "release_id", "id": rel_id, "aggregateByIssue": "true"})
        for rel_id in release_ids
    )
    for response in responses:
        for issue in response:
            merged = issues.setdefault(issue["id"], {**issue, "links": []})
            merged["links"].extend(issue.get("links") or [])
    return list(issues.values())


def fetch_actual_costs(reader: ArgusReader, runs: list[dict]) -> dict[str, float]:
    run_ids = [run["id"] for run in runs if run["start_time"] >= ACTUAL_COST_SINCE]
    LOGGER.info("Reading the cost of %d runs", len(run_ids))
    responses = reader.fetch_many(("/cost/run/$run_id", {"run_id": run_id}, None) for run_id in run_ids)
    return {
        run_id: cost["actual_cost"]
        for run_id, cost in zip(run_ids, responses)
        if cost and cost.get("actual_cost") is not None
    }


def fetch_assignees(reader: ArgusReader, group_ids: Iterable[str]) -> tuple[dict[str, list[str]], dict[str, str]]:
    """Return the release-plan assignees of each test, and the display name of each user."""
    assignees = {}
    for response in reader.fetch_many(("/release/assignees/tests", None, {"groupId": gid}) for gid in group_ids):
        for test_id, user_ids in (response or {}).items():
            assignees.setdefault(test_id, set()).update(user_ids or [])
    users = {
        user_id: user.get("full_name") or user.get("username") or user_id
        for user_id, user in (reader.fetch("/users") or {}).items()
    }
    return {test_id: sorted(ids) for test_id, ids in assignees.items()}, users


def fetch_last_runs(reader: ArgusReader, test_ids: list[str]) -> dict[str, tuple[str, str]]:
    """Return the start time and status of the latest run ever of each test."""
    responses = reader.fetch_many(("/test/$test_id/runs", {"test_id": tid}, {"limit": 1}) for tid in test_ids)
    return {test_id: (runs[0]["start_time"], runs[0]["status"]) for test_id, runs in zip(test_ids, responses) if runs}
