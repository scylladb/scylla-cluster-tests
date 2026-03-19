#!/usr/bin/env python3
"""Trend aggregation script for PR review classification data.

Reads all classified PR JSON files (output of classify.py) and produces
report_data.json with 5 aggregation types:
  1. quarterly_counts    — per-quarter comment counts per category
  2. reviewer_matrix     — per-reviewer, per-category count
  3. author_breakdown    — human vs AI PR statistics
  4. rolling_averages    — 4-quarter rolling average per category
  5. inflection_points   — emergence and peak detection per category

Usage:
    python analyze.py \\
        --input ./cache/classified \\
        --output ./cache/report_data.json \\
        --taxonomy scripts/review_analysis/taxonomy.yaml
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

ROLLING_WINDOW = 4


# ---------------------------------------------------------------------------
# Quarter utilities
# ---------------------------------------------------------------------------


def get_quarter(created_at: str) -> str | None:
    """Return 'YYYY-Qn' string from ISO datetime. Returns None on parse error."""
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        q = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{q}"
    except (ValueError, AttributeError, TypeError):
        return None


def quarter_to_sort_key(quarter: str) -> tuple[int, int]:
    """Return a sortable tuple (year, quarter_num) for a quarter string like '2024-Q3'."""
    year_str, q_str = quarter.split("-Q")
    return int(year_str), int(q_str)


def all_quarters_in_range(from_quarter: str, to_quarter: str) -> list[str]:
    """Generate the complete ordered sequence of quarters from from_quarter to to_quarter.

    Ensures no gaps in the timeline (important for charts).
    """
    start_year, start_q = quarter_to_sort_key(from_quarter)
    end_year, end_q = quarter_to_sort_key(to_quarter)

    quarters: list[str] = []
    year, q = start_year, start_q
    while (year, q) <= (end_year, end_q):
        quarters.append(f"{year}-Q{q}")
        q += 1
        if q > 4:
            q = 1
            year += 1
    return quarters


def current_quarter() -> str:
    """Return the current quarter as 'YYYY-Qn'."""
    now = datetime.now(tz=timezone.utc)
    q = (now.month - 1) // 3 + 1
    return f"{now.year}-Q{q}"


# ---------------------------------------------------------------------------
# Taxonomy helpers
# ---------------------------------------------------------------------------


def load_taxonomy(taxonomy_path: Path) -> list[dict]:
    """Load and return categories from taxonomy.yaml."""
    with taxonomy_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data["categories"]


def get_all_category_ids(categories: list[dict]) -> list[str]:
    """Return all category IDs from the taxonomy."""
    return [cat["id"] for cat in categories]


def get_category_trajectory(categories: list[dict]) -> dict[str, str]:
    """Return a dict mapping category_id -> trajectory string."""
    return {cat["id"]: cat.get("trajectory", "PERSISTENT") for cat in categories}


# ---------------------------------------------------------------------------
# Shared iteration helpers
# ---------------------------------------------------------------------------


def iter_classifiable_comments(pr_data: dict):
    """Yield all comment/review body items that have non-empty bodies.

    Yields individual item dicts from both 'comments' and non-empty 'reviews'.
    """
    for item in pr_data.get("comments", []):
        if (item.get("body") or "").strip():
            yield item
    for item in pr_data.get("reviews", []):
        if (item.get("body") or "").strip():
            yield item


def load_pr_file(pr_file: Path) -> dict | None:
    """Load and return a PR JSON file, or None on error."""
    try:
        with pr_file.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Skipping malformed JSON file %s: %s", pr_file, exc)
        return None


# ---------------------------------------------------------------------------
# Aggregation 1: quarterly_counts
# ---------------------------------------------------------------------------


def build_quarterly_counts(
    pr_files: list[Path],
    all_category_ids: list[str],
) -> tuple[dict, str, str, int, int, int]:
    """Build quarterly_counts aggregation.

    Counts per-quarter occurrences of each category across all classifiable
    comments and review bodies. Each comment that matched multiple categories
    increments ALL matching category counters.

    Returns:
        quarterly_counts — {quarter: {category_id: count}} with full range filled
        from_quarter     — earliest quarter seen in data
        to_quarter       — latest quarter seen in data
        prs_analyzed     — total PR files processed without errors
        comments_analyzed — inline comments with valid created_at
        reviews_analyzed  — review bodies with valid created_at
    """
    raw_quarterly: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_quarters_seen: set[str] = set()

    prs_analyzed = 0
    comments_analyzed = 0
    reviews_analyzed = 0

    for pr_file in pr_files:
        pr_data = load_pr_file(pr_file)
        if pr_data is None:
            continue

        prs_analyzed += 1

        # Inline PR comments
        for item in pr_data.get("comments", []):
            body = (item.get("body") or "").strip()
            if not body:
                continue
            created_at = item.get("created_at")
            if not created_at:
                log.debug("Skipping comment missing created_at in %s", pr_file.name)
                continue
            quarter = get_quarter(created_at)
            if not quarter:
                log.debug("Unparseable created_at %r in %s", created_at, pr_file.name)
                continue
            categories = item.get("categories", ["T0_UNCATEGORIZED"])
            for cat_id in categories:
                raw_quarterly[quarter][cat_id] += 1
            all_quarters_seen.add(quarter)
            comments_analyzed += 1

        # Review body comments (non-empty only)
        for item in pr_data.get("reviews", []):
            body = (item.get("body") or "").strip()
            if not body:
                continue
            created_at = item.get("created_at")
            if not created_at:
                log.debug("Skipping review missing created_at in %s", pr_file.name)
                continue
            quarter = get_quarter(created_at)
            if not quarter:
                log.debug("Unparseable created_at %r in %s", created_at, pr_file.name)
                continue
            categories = item.get("categories", ["T0_UNCATEGORIZED"])
            for cat_id in categories:
                raw_quarterly[quarter][cat_id] += 1
            all_quarters_seen.add(quarter)
            reviews_analyzed += 1

    # Determine full range (default to 2019-Q1 → current if no data)
    if all_quarters_seen:
        sorted_quarters = sorted(all_quarters_seen, key=quarter_to_sort_key)
        from_q = sorted_quarters[0]
        to_q = sorted_quarters[-1]
    else:
        from_q = "2019-Q1"
        to_q = current_quarter()

    # Fill complete range with zero-values for all categories
    complete_range = all_quarters_in_range(from_q, to_q)
    quarterly_counts: dict[str, dict[str, int]] = {}
    for q in complete_range:
        quarterly_counts[q] = {cat_id: raw_quarterly[q].get(cat_id, 0) for cat_id in all_category_ids}

    return quarterly_counts, from_q, to_q, prs_analyzed, comments_analyzed, reviews_analyzed


# ---------------------------------------------------------------------------
# Aggregation 2: reviewer_matrix
# ---------------------------------------------------------------------------


def build_reviewer_matrix(
    pr_files: list[Path],
    all_category_ids: list[str],
    min_comments: int = 5,
) -> dict:
    """Build reviewer_matrix aggregation.

    Returns {reviewer_login: {category_id: count}} for human reviewers with
    at least min_comments total comments. Bot users (is_bot == true) are
    excluded.
    """
    reviewer_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    reviewer_totals: dict[str, int] = defaultdict(int)

    for pr_file in pr_files:
        pr_data = load_pr_file(pr_file)
        if pr_data is None:
            continue

        for item in iter_classifiable_comments(pr_data):
            # Exclude bot accounts
            if item.get("is_bot"):
                continue
            user = (item.get("user") or "").strip()
            if not user:
                continue
            categories = item.get("categories", ["T0_UNCATEGORIZED"])
            for cat_id in categories:
                reviewer_counts[user][cat_id] += 1
            reviewer_totals[user] += 1

    # Filter to reviewers meeting the minimum comment threshold
    result: dict[str, dict[str, int]] = {}
    for user, total in reviewer_totals.items():
        if total >= min_comments:
            result[user] = {cat_id: reviewer_counts[user].get(cat_id, 0) for cat_id in all_category_ids}

    return result


# ---------------------------------------------------------------------------
# Aggregation 3: author_breakdown
# ---------------------------------------------------------------------------


def build_author_breakdown(
    pr_files: list[Path],
    all_category_ids: list[str],
) -> dict:
    """Build author_breakdown aggregation.

    Splits PRs and their review comments into 'human' and 'ai' buckets based
    on the pr['is_ai_author'] flag. A PR with no comments still increments
    its bucket's pr_count.

    Returns:
        {
            "human": {"pr_count": int, "comment_count": int, "category_counts": {...}},
            "ai":    {"pr_count": int, "comment_count": int, "category_counts": {...}},
        }
    """
    bucket_pr_counts = {"human": 0, "ai": 0}
    bucket_comment_counts = {"human": 0, "ai": 0}
    bucket_cat_counts: dict[str, dict[str, int]] = {
        "human": defaultdict(int),
        "ai": defaultdict(int),
    }

    for pr_file in pr_files:
        pr_data = load_pr_file(pr_file)
        if pr_data is None:
            continue

        bucket = "ai" if pr_data.get("is_ai_author") else "human"
        bucket_pr_counts[bucket] += 1

        for item in iter_classifiable_comments(pr_data):
            categories = item.get("categories", ["T0_UNCATEGORIZED"])
            for cat_id in categories:
                bucket_cat_counts[bucket][cat_id] += 1
            bucket_comment_counts[bucket] += 1

    # Materialise with full category list and plain dicts
    return {
        "human": {
            "pr_count": bucket_pr_counts["human"],
            "comment_count": bucket_comment_counts["human"],
            "category_counts": {cat_id: bucket_cat_counts["human"].get(cat_id, 0) for cat_id in all_category_ids},
        },
        "ai": {
            "pr_count": bucket_pr_counts["ai"],
            "comment_count": bucket_comment_counts["ai"],
            "category_counts": {cat_id: bucket_cat_counts["ai"].get(cat_id, 0) for cat_id in all_category_ids},
        },
    }


# ---------------------------------------------------------------------------
# Aggregation 4: rolling_averages
# ---------------------------------------------------------------------------


def build_rolling_averages(
    quarterly_counts: dict,
    all_category_ids: list[str],
    window: int = ROLLING_WINDOW,
) -> dict:
    """Build rolling_averages aggregation.

    For each category, compute the trailing window-quarter moving average.
    The first (window - 1) quarters are null because there is insufficient
    history to form a full window.

    Returns:
        {category_id: {quarter: float | None}}
    """
    quarters = list(quarterly_counts.keys())  # ordered by all_quarters_in_range
    result: dict[str, dict[str, float | None]] = {}

    for cat_id in all_category_ids:
        cat_values = [quarterly_counts[q][cat_id] for q in quarters]
        cat_rolling: dict[str, float | None] = {}
        for i, q in enumerate(quarters):
            if i < window - 1:
                cat_rolling[q] = None
            else:
                window_values = cat_values[i - window + 1 : i + 1]
                cat_rolling[q] = mean(window_values)
        result[cat_id] = cat_rolling

    return result


# ---------------------------------------------------------------------------
# Aggregation 5: inflection_points
# ---------------------------------------------------------------------------


def build_inflection_points(
    quarterly_counts: dict,
    rolling_averages: dict,
    all_category_ids: list[str],
    category_trajectories: dict[str, str],
) -> dict:
    """Build inflection_points aggregation.

    For each category determines:
        first_seen        — first quarter with count > 0
        peak_quarter      — quarter with the highest count
        peak_count        — count in peak_quarter
        emergence_quarter — first quarter where count > 2× previous rolling avg
                            (None if this never occurred)
        status            — trajectory from taxonomy YAML

    Returns:
        {category_id: {first_seen, peak_quarter, peak_count, emergence_quarter, status}}
    """
    quarters = list(quarterly_counts.keys())
    result: dict[str, dict] = {}

    for cat_id in all_category_ids:
        cat_values = [quarterly_counts[q].get(cat_id, 0) for q in quarters]

        # first_seen: first quarter with at least one comment
        first_seen: str | None = next((quarters[i] for i, v in enumerate(cat_values) if v > 0), None)

        # peak_quarter / peak_count
        if any(v > 0 for v in cat_values):
            peak_idx = max(range(len(cat_values)), key=lambda i: cat_values[i])
            peak_quarter: str | None = quarters[peak_idx]
            peak_count: int = cat_values[peak_idx]
        else:
            peak_quarter = None
            peak_count = 0

        # emergence_quarter: first quarter where count > 2× the previous rolling avg.
        # We need at least ROLLING_WINDOW prior quarters to have a meaningful baseline.
        # At index i, the rolling avg for quarters[i-1] covers quarters[i-WINDOW..i-1].
        emergence_quarter: str | None = None
        rolling = rolling_averages.get(cat_id, {})
        for i, q in enumerate(quarters):
            # Need index >= ROLLING_WINDOW so that the previous quarter (i-1) has
            # a non-null rolling average.
            if i < ROLLING_WINDOW:
                continue
            count = cat_values[i]
            prev_rolling = rolling.get(quarters[i - 1])
            if prev_rolling is None or prev_rolling == 0:
                continue
            if count > 2 * prev_rolling:
                emergence_quarter = q
                break

        result[cat_id] = {
            "first_seen": first_seen,
            "peak_quarter": peak_quarter,
            "peak_count": peak_count,
            "emergence_quarter": emergence_quarter,
            "status": category_trajectories.get(cat_id, "PERSISTENT"),
        }

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate classified PR review data into trend statistics.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="DIR",
        help="Directory containing classified PR JSON files (output of classify.py).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="FILE",
        help="Output path for report_data.json.",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to taxonomy.yaml file.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0914
    args = parse_args(argv)

    if not args.input.is_dir():
        log.error("Input directory does not exist: %s", args.input)
        return 1
    if not args.taxonomy.is_file():
        log.error("Taxonomy file not found: %s", args.taxonomy)
        return 1

    log.info("Loading taxonomy from %s", args.taxonomy)
    categories = load_taxonomy(args.taxonomy)
    all_category_ids = get_all_category_ids(categories)
    category_trajectories = get_category_trajectory(categories)
    log.info("Loaded %d categories", len(all_category_ids))

    input_files = sorted(
        args.input.glob("*.json"),
        key=lambda p: int(p.stem) if p.stem.isdigit() else 0,
    )
    if not input_files:
        log.warning("No JSON files found in %s", args.input)
        return 0
    log.info("Analyzing %d classified PR files from %s", len(input_files), args.input)

    # --- 1. Quarterly counts (also yields data range and totals) ---
    log.info("Building quarterly_counts...")
    quarterly_counts, from_quarter, to_quarter, prs_analyzed, comments_analyzed, reviews_analyzed = (
        build_quarterly_counts(input_files, all_category_ids)
    )
    log.info(
        "Data range: %s to %s | PRs: %d | Comments: %d | Reviews: %d",
        from_quarter,
        to_quarter,
        prs_analyzed,
        comments_analyzed,
        reviews_analyzed,
    )

    # --- 2. Reviewer matrix ---
    log.info("Building reviewer_matrix (min 5 comments)...")
    reviewer_matrix = build_reviewer_matrix(input_files, all_category_ids)
    log.info("Reviewer matrix: %d reviewers with ≥5 comments", len(reviewer_matrix))

    # --- 3. Author breakdown ---
    log.info("Building author_breakdown...")
    author_breakdown = build_author_breakdown(input_files, all_category_ids)
    log.info(
        "Author breakdown: %d human PRs, %d AI PRs",
        author_breakdown["human"]["pr_count"],
        author_breakdown["ai"]["pr_count"],
    )

    # --- 4. Rolling averages ---
    log.info("Building rolling_averages (window=%d quarters)...", ROLLING_WINDOW)
    rolling_averages = build_rolling_averages(quarterly_counts, all_category_ids)

    # --- 5. Inflection points ---
    log.info("Building inflection_points...")
    inflection_points = build_inflection_points(
        quarterly_counts, rolling_averages, all_category_ids, category_trajectories
    )
    emergence_count = sum(1 for v in inflection_points.values() if v.get("emergence_quarter") is not None)
    log.info("Inflection points: %d categories with detected emergence", emergence_count)

    # --- Assemble and write output ---
    report_data = {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_range": {"from": from_quarter, "to": to_quarter},
        "totals": {
            "prs_analyzed": prs_analyzed,
            "comments_analyzed": comments_analyzed,
            "reviews_analyzed": reviews_analyzed,
        },
        "quarterly_counts": quarterly_counts,
        "reviewer_matrix": reviewer_matrix,
        "author_breakdown": author_breakdown,
        "rolling_averages": rolling_averages,
        "inflection_points": inflection_points,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with args.output.open("w", encoding="utf-8") as fh:
            json.dump(report_data, fh, indent=2, ensure_ascii=False)
        log.info("Report data written to %s", args.output)
    except OSError as exc:
        log.error("Failed to write output file %s: %s", args.output, exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
