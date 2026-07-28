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

"""Invariants for the SCT-716 biweekly alternation of tier1 / rolling-upgrade triggers.

Each matrix splits its `weekly` jobs into two halves labelled `week-a` / `week-b`, and has
two cron lines selecting one half each. These tests guard the properties that make the
split lossless and keep it from silently degrading back to "everything, every week".
"""

import re
from pathlib import Path

import pytest

from sdcm.utils.trigger_matrix import filter_jobs, load_matrix_config

TRIGGERS_DIR = Path(__file__).parent.parent.parent / "configurations" / "triggers"
BIWEEKLY_MATRICES = ["tier1.yaml", "rolling-upgrade.yaml"]
WEEK_LABELS = {"week-a", "week-b"}


@pytest.fixture(name="config", params=BIWEEKLY_MATRICES)
def fixture_config(request):
    return load_matrix_config(TRIGGERS_DIR / request.param)


def test_every_weekly_job_has_exactly_one_week_label(config):
    """A weekly job with no week label would never run; with both it would run every week."""
    for job in config.jobs:
        if "weekly" not in job.labels:
            continue
        week_labels = WEEK_LABELS.intersection(job.labels)
        assert len(week_labels) == 1, (
            f"Job '{job.job_name}' has week labels {week_labels or 'none'}, expected exactly one"
        )


def test_week_labels_only_on_weekly_jobs(config):
    """The cron selectors are the only consumers of week labels — a non-weekly job carrying
    one (e.g. a `release`-only rolling-upgrade job) would start firing on the cron."""
    for job in config.jobs:
        if stray := WEEK_LABELS.intersection(job.labels):
            assert "weekly" in job.labels, f"Job '{job.job_name}' has {stray} but is not labelled 'weekly'"


def test_cron_selectors_partition_the_weekly_set(config):
    """week-a and week-b must be disjoint and together cover exactly the full weekly set."""
    selectors = [cron.params["labels_selector"] for cron in config.cron_triggers]
    assert len(selectors) == 2, f"Expected 2 biweekly cron lines, got {selectors}"

    halves = [
        {job.job_name for job in filter_jobs(config.jobs, scylla_version="master:latest", labels_selector=selector)}
        for selector in selectors
    ]
    full = {job.job_name for job in filter_jobs(config.jobs, scylla_version="master:latest", labels_selector="weekly")}

    assert not halves[0] & halves[1], f"Halves overlap: {halves[0] & halves[1]}"
    assert halves[0] | halves[1] == full, f"Halves do not cover the weekly set: {full - (halves[0] | halves[1])}"
    assert halves[0] and halves[1], "Both halves must be non-empty"


def test_duplicate_job_entries_share_a_week_label(config):
    """Entries resolving to the same Jenkins job (x86 + aarch64 twins) are deduplicated by
    resolved path, so only the first is triggered. Twins on different weeks would make the
    job fire on both weeks — silently defeating the alternation."""
    labels_by_job: dict[str, set[frozenset[str]]] = {}
    for job in config.jobs:
        if "weekly" not in job.labels:
            continue
        week_label = frozenset(WEEK_LABELS.intersection(job.labels))
        labels_by_job.setdefault(job.job_name, set()).add(week_label)

    for job_name, week_labels in labels_by_job.items():
        assert len(week_labels) == 1, (
            f"Duplicate entries for '{job_name}' use different week labels {week_labels} — "
            f"the job would be triggered on both weeks"
        )


def test_cron_lines_same_time_of_day_and_disjoint_day_windows(config):
    """Both halves fire at the same time of day, and their day-of-month windows tile 1-31
    without overlap — so exactly one half runs on any given Saturday."""
    schedules = [cron.schedule.split() for cron in config.cron_triggers]
    assert len(schedules) == 2

    minutes_hours = {(fields[0], fields[1]) for fields in schedules}
    assert len(minutes_hours) == 1, f"Halves fire at different times of day: {minutes_hours}"

    days_of_week = {fields[4] for fields in schedules}
    assert len(days_of_week) == 1, f"Halves fire on different days of the week: {days_of_week}"

    def days(field: str) -> set[int]:
        result: set[int] = set()
        for part in field.split(","):
            if match := re.fullmatch(r"(\d+)-(\d+)", part):
                result |= set(range(int(match.group(1)), int(match.group(2)) + 1))
            else:
                result.add(int(part))
        return result

    window_a, window_b = (days(fields[2]) for fields in schedules)
    assert not window_a & window_b, f"Day-of-month windows overlap on {sorted(window_a & window_b)}"
    assert window_a | window_b == set(range(1, 32)), (
        f"Day-of-month windows leave gaps: {sorted(set(range(1, 32)) - (window_a | window_b))}"
    )


def test_tier1_and_rolling_upgrade_are_in_opposite_phase():
    """tier1's heavy half must not share a weekend with rolling-upgrade's heavy half."""
    phases = {}
    for filename in BIWEEKLY_MATRICES:
        config = load_matrix_config(TRIGGERS_DIR / filename)
        phases[filename] = {cron.schedule: cron.params["labels_selector"] for cron in config.cron_triggers}

    for schedule, tier1_selector in phases["tier1.yaml"].items():
        rolling_selector = phases["rolling-upgrade.yaml"][schedule]
        assert tier1_selector != rolling_selector, (
            f"Both matrices select '{tier1_selector}' on '{schedule}' — phases should be opposite"
        )
