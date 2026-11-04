from pathlib import Path

import pytest

from utils.test_activity_report import analysis, pipelines
from utils.test_activity_report.report import page_payload

RELEASES = {"rel-a": "release-a", "rel-b": "release-b"}
TESTS = {
    "t-1": {"build_system_id": "release-a/longevity/longevity-1h-test", "release_id": "rel-a"},
    "t-2": {"build_system_id": "release-b/longevity/longevity-1h-test", "release_id": "rel-b"},
    "t-3": {"build_system_id": "release-a/longevity/longevity-2h-test", "release_id": "rel-a"},
}


def make_run(run_id, test_id="t-1", status="passed", cost=1.0, hours=1.0, start="2026-01-01T00:00:00"):
    return {
        "id": run_id,
        "test_id": test_id,
        "release_id": TESTS[test_id]["release_id"],
        "build_number": 1,
        "status": status,
        "investigation_status": "not_investigated",
        "start_time": start,
        "started_by": "timer",
        "cloud_setup": {"backend": "aws"},
        "_cost": cost,
        "_kind": "estimated" if cost is not None else "no-data",
        "_hours": hours,
    }


def make_issue(key, area):
    return analysis.Issue(key=key, title="", url="", area=area, source="", state="open")


def test_build_row_uses_the_maximum_run_cost_and_duration_times_the_runs():
    runs = [make_run("r1", cost=1.0, hours=2.0), make_run("r2", cost=5.0, hours=1.0), make_run("r3", cost=None)]

    row = analysis.build_row("longevity/longevity-1h", runs, {}, {})

    assert row["cost_max_run"] == 5.0
    assert row["cost_est_total"] == 15.0
    assert row["cost_runs"] == 2
    assert row["hours_est_total"] == 6.0


def test_build_row_without_a_priced_run_has_no_cost():
    row = analysis.build_row("k", [make_run("r1", cost=None)], {}, {})

    assert row["cost_max_run"] is None
    assert row["cost_est_total"] is None
    assert row["cost_kind"] == "no-data"


@pytest.mark.parametrize(
    "run,expected",
    [
        ({"start_time": "2026-01-01T00:00:00", "end_time": "2026-01-01T03:00:00", "status": "passed"}, 3.0),
        ({"start_time": "2026-01-01T00:00:00", "end_time": None, "heartbeat": 1767232800, "status": "failed"}, 2.0),
        # A run that stopped in setup: no end time, heartbeat equal to the start.
        ({"start_time": "2026-01-01T00:00:00", "end_time": "1970-01-01T00:00:00", "heartbeat": 1767225600,
          "status": "test_error"}, 0.0),
        ({"start_time": "2026-01-01T00:00:00", "end_time": None, "heartbeat": 0, "status": "running"}, None),
    ],
)  # fmt: skip
def test_run_hours(run, expected):
    assert analysis.run_hours(run) == expected


def test_estimate_run_cost_sums_the_node_roles():
    run = {
        "cloud_setup": {
            "backend": "aws",
            "db_node": {"instance_type": "db.type", "node_amount": 3},
            "loader_node": {"instance_type": "loader.type", "node_amount": 1},
            "monitor_node": {"instance_type": "loader.type", "node_amount": 0},
        },
        "region_name": ["region-1"],
    }
    rates = {"db.type": 2.0, "loader.type": 0.5}

    cost, reason = analysis.estimate_run_cost(run, 2.0, lambda cloud, region, itype: rates[itype])

    assert (cost, reason) == (13.0, "estimate")


@pytest.mark.parametrize(
    "setup,expected",
    [
        ({"backend": "docker"}, (0.0, "non-cloud")),
        ({}, (None, "no-setup")),
        ({"backend": "gce", "db_node": {"instance_type": "UNKNOWN", "node_amount": 1}}, (None, "no-instance-type")),
        ({"backend": "gce", "db_node": {"instance_type": "unpriced", "node_amount": 1}}, (None, "no-price")),
    ],
)
def test_estimate_run_cost_reports_why_there_is_no_cost(setup, expected):
    assert analysis.estimate_run_cost({"cloud_setup": setup}, 1.0, lambda *_: None) == expected


def test_price_runs_prefers_the_actual_cost():
    run = {
        "id": "r1",
        "status": "passed",
        "start_time": "2026-01-01T00:00:00",
        "end_time": "2026-01-01T01:00:00",
        "cloud_setup": {"backend": "aws", "db_node": {"instance_type": "x.large", "node_amount": 1}},
    }

    analysis.price_runs([run], {"r1": 7.0}, lambda *_: 1.0)

    assert (run["_cost"], run["_kind"]) == (7.0, "actual")


@pytest.mark.parametrize(
    "raw,area",
    [
        ({"subtype": "github", "owner": "scylladb", "repo": "scylladb", "number": 1, "title": "crash"}, "product"),
        ({"subtype": "github", "owner": "ScyllaDB", "repo": "ScyllaDB", "number": 2, "title": "x"}, "product"),
        ({"subtype": "github", "owner": "scylladb", "repo": "scylla-cluster-tests", "number": 3, "title": "x"}, "framework"),
        ({"subtype": "github", "owner": "example", "repo": "driver", "number": 4, "title": "x"}, "other"),
        ({"subtype": "jira", "key": "SCYLLADB-5", "summary": "Oversized allocation (262144 bytes) in x"}, "oversized"),
        ({"subtype": "github", "owner": "scylladb", "repo": "scylladb", "number": 6,
          "title": "[OVERSIZED_ALLOCATION]: 135168 bytes"}, "oversized"),
        ({"subtype": "jira", "key": "SCT-7", "summary": "oversized allocation check fails"}, "framework"),
        ({"subtype": "jira", "key": "OTHER-8", "summary": "x"}, "other"),
    ],
)  # fmt: skip
def test_classify_issue(raw, area):
    assert analysis.classify_issue(raw).area == area


def test_link_issues_merges_records_that_differ_in_owner_casing():
    raw = [
        {"subtype": "github", "owner": "scylladb", "repo": "scylladb", "number": 1, "title": "a",
         "links": [{"run_id": "r1"}]},
        {"subtype": "github", "owner": "ScyllaDB", "repo": "scylladb", "number": 1, "title": "a",
         "links": [{"run_id": "r2"}, {"run_id": "out-of-scope"}]},
    ]  # fmt: skip

    run_issues, issues = analysis.link_issues(raw, {"r1", "r2"})

    assert list(issues) == ["scylladb/scylladb#1"]
    assert run_issues == {"r1": {"scylladb/scylladb#1"}, "r2": {"scylladb/scylladb#1"}}


def test_release_rows_use_only_the_runs_of_their_release():
    runs = [
        make_run("a1", "t-1", status="failed", cost=10.0),
        make_run("a2", "t-1", cost=2.0),
        make_run("b1", "t-2", cost=1.0),
        make_run("c1", "t-3", cost=1.0),
    ]
    issues = {"p1": make_issue("P-1", "product"), "p2": make_issue("P-2", "product")}
    run_issues = {"a1": {"p1"}, "b1": {"p2"}, "c1": {"p2"}}

    rows = {row["test"]: row for row in analysis.build_rows(runs, TESTS, RELEASES, run_issues, issues)}
    row = rows["longevity/longevity-1h"]
    release_a, release_b = row["by_release"]["release-a"], row["by_release"]["release-b"]

    assert row["runs"] == 3
    assert (release_a["runs"], release_a["failed"], release_a["cost_est_total"]) == (2, 1, 20.0)
    assert (release_b["runs"], release_b["cost_est_total"]) == (1, 1.0)
    assert release_a["issue_ids"] == {"product": ["p1"]}
    assert release_b["issue_ids"] == {"product": ["p2"]}
    # Over all releases, two tests found P-2. In release-b only longevity-1h ran, so P-2 is unique there.
    assert row["product_unique_ids"] == ["p1"]
    assert release_b["product_unique_ids"] == ["p2"]
    assert rows["longevity/longevity-2h"]["product_unique_ids"] == []


def test_page_payload_keeps_release_counts_and_run_links():
    runs = [make_run("a1", "t-1"), make_run("b1", "t-2", start="2026-02-01T00:00:00")]
    issues = {"p1": make_issue("P-1", "product")}
    rows = analysis.build_rows(runs, TESTS, RELEASES, {"b1": {"p1"}}, issues)
    for row in rows:
        row["owner"] = {"teams": [], "people": []}
    data = {"summary": {}, "rows": rows, "issues": issues, "not_run_defined": [], "not_run_jobs": [],
            "unreferenced_cases": []}  # fmt: skip

    payload_row = page_payload(data)["rows"][0]

    assert [run[0] for run in payload_row["runs"]] == ["b1", "a1"]
    assert payload_row["byRel"]["release-a"]["iss"] == []
    assert payload_row["byRel"]["release-b"]["iss"] == [[0, [0], 1]]


@pytest.fixture
def sct_tree(tmp_path: Path) -> Path:
    files = {
        "jenkins-pipelines/oss/longevity/longevity-1h.jenkinsfile": "test_config: 'test-cases/longevity/one.yaml'",
        "jenkins-pipelines/oss/longevity/longevity-2h.jenkinsfile": "test_config: 'test-cases/longevity/two.yaml'",
        "jenkins-pipelines/oss/longevity/longevity-trigger.jenkinsfile": "",
        "jenkins-pipelines/oss/longevity/longevity-trigger-arm.jenkinsfile": "",
        "jenkins-pipelines/quarantined/tier2/longevity-broken.jenkinsfile": "",
        "jenkins-pipelines/other/mirror/_symlinks.yaml": (
            "symlinks:\n"
            "  - name: longevity-2h\n    target: jenkins-pipelines/oss/longevity/longevity-2h.jenkinsfile\n"
            "  - name: longevity-broken\n    target: jenkins-pipelines/quarantined/tier2/longevity-broken.jenkinsfile\n"
        ),
        "test-cases/longevity/one.yaml": "test_metadata:\n  team_ownership: team-a\n",
        "test-cases/longevity/two.yaml": "test_duration: 60\n",
        "test-cases/longevity/unused.yaml": "test_duration: 60\n",
    }
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_pipelines_with_no_run_leave_out_quarantine_and_triggers(sct_tree):
    defined = pipelines.defined_pipelines(sct_tree)

    no_run = pipelines.pipelines_with_no_run(defined, ran_keys={"longevity/longevity-1h"})

    assert [(p.key, p.file) for p in no_run] == [
        ("longevity/longevity-2h", "jenkins-pipelines/oss/longevity/longevity-2h.jenkinsfile"),
        ("mirror/longevity-2h", "jenkins-pipelines/other/mirror/_symlinks.yaml"),
    ]


def test_owning_teams_and_unreferenced_test_cases(sct_tree):
    defined = pipelines.defined_pipelines(sct_tree)
    cases = pipelines.jenkinsfile_test_cases(sct_tree, defined)

    assert pipelines.owning_teams(sct_tree, defined, cases) == {"longevity/longevity-1h": ["team-a"]}
    assert pipelines.unreferenced_test_cases(sct_tree, cases) == ["test-cases/longevity/unused.yaml"]
