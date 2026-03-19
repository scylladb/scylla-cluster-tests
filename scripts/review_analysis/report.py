#!/usr/bin/env python3
"""Markdown report generator for PR review comment taxonomy analysis.

Reads report_data.json (produced by analyze.py) and generates a fully
formatted Markdown report with Mermaid trend charts, reviewer matrices,
AI/human comparisons, and skill design recommendations.

Usage:
    python report.py \\
        --input ./cache/report_data.json \\
        --output ./reports/pr-review-taxonomy-report.md \\
        --taxonomy scripts/review_analysis/taxonomy.yaml
"""

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

TRAJECTORY_EMOJI = {
    "GROWING": "⬆",
    "DECLINING": "⬇",
    "PERSISTENT": "→",
    "INTERMITTENT": "↔",
}

UNCATEGORIZED_ID = "T0_UNCATEGORIZED"

# Hardcoded pilot quotes per category (from 7-year review data)
REPRESENTATIVE_QUOTES: dict[str, list[str]] = {
    "T1_TEST_QUALITY": [
        "Dont use classes, if you need tests grouping uses module + separate files",
        "This should be a fixture, instead of having to call it in every test",
        "can be made into one test with parametrization probably",
    ],
    "T2_MISSING_DOCUMENTATION": [
        "write the descriptions for those, we shouldn't have empty help values",
        "Please add documentation for this nemesis",
        "why max is 10? what makes it reasonable? please add clear comment about the why",
    ],
    "T3_AI_CODE_QUALITY": [
        "@copilot this is wrong solution",
        "As always with claude generated PRs I think the tests are low quality and should be improved upon",
        "need to bring this back",
    ],
    "T4_CODE_STRUCTURE": [
        "nit: no need for this to be inner method",
        "better to centralize the defaults somewhere so it is easier to change them",
        "don't initialize an argument with a mutable list",
    ],
    "T5_SCOPE_HYGIENE": [
        "This far out of scope for this PR, needs to be removed and put into its own PR",
        "how taking this comment out, is related to this PR?",
        "raise a task/subtask in jira",
    ],
    "T6_ERROR_HANDLING": [
        "if something escaped check_node_health we should raise error event, so it would be visible in Argus",
        "if it is invalid input, you should throw exception",
        "does not use try/finally around the yield, so event_stop will not be called",
    ],
    "T7_MISSING_TESTS": [
        "there isn't a unit test asserting the parsing behavior",
        "Do you think we could unit test this as well?",
        "This test is great for onetime verification, but running it constantly doesn't give us assurances",
    ],
    "T8_PLAN_QUALITY": [
        "remove implementation details from plan, it's in the way and not needed",
        "don't include test code of unittest in the plan",
        "minimize the testing to test ideas, don't write the python code for testing",
    ],
    "T9_UNUSED_DEAD_CODE": [
        "Unused import of 'KernelPanicEvent' is not used",
        "This comment appears to contain commented-out code",
        "Suspicious unused loop iteration variable",
    ],
    "T10_CONSISTENCY": [
        "I would unify the handling...for consistency",
        "Cannot we use nodeallocator to do this more easily?",
        "align all of those into the same format",
    ],
    "T11_CONFIG_SYSTEM": [
        "we shouldn't have empty help values",
        "This should be done as part of validation in configuration",
        "shouldnt this be configurable?",
    ],
    "T12_NAMING": [
        "shouldn't name it ami_id, we need better name if we ever want similar test in other backends",
        "overabundance of protected function/attributes... making this file hard to read",
        "no need to make this protected",
    ],
    "T13_LOGIC_CORRECTNESS": [
        "this code is wrong, seeds need to be a comma separated list of addresses, not an actual list",
        "Zero-token nodes are supported only with consistent topology changes...You should enforce this somehow",
        "If we fail to count them, shouldn't we throw an exception? By returning zero the test might pass",
    ],
}


# ---------------------------------------------------------------------------
# Taxonomy helpers
# ---------------------------------------------------------------------------


def load_taxonomy(taxonomy_path: Path) -> list[dict]:
    """Load and return the category list from taxonomy.yaml."""
    with taxonomy_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data["categories"]


def build_cat_index(categories: list[dict]) -> dict[str, dict]:
    """Build {cat_id: cat_dict} lookup from taxonomy categories."""
    return {cat["id"]: cat for cat in categories}


# ---------------------------------------------------------------------------
# Quarter utilities (mirrors analyze.py — duplicated to avoid cross-script import)
# ---------------------------------------------------------------------------


def _quarter_key(quarter: str) -> tuple[int, int]:
    year_str, q_str = quarter.split("-Q")
    return int(year_str), int(q_str)


def all_quarters_in_range(from_q: str, to_q: str) -> list[str]:
    """Return ordered list of all quarters from from_q to to_q inclusive."""
    start_year, start_q = _quarter_key(from_q)
    end_year, end_q = _quarter_key(to_q)
    quarters: list[str] = []
    year, q = start_year, start_q
    while (year, q) <= (end_year, end_q):
        quarters.append(f"{year}-Q{q}")
        q += 1
        if q > 4:
            q = 1
            year += 1
    return quarters


def sample_quarters(quarters: list[str], max_labels: int = 20) -> list[int]:
    """Return indices into quarters that should appear as x-axis labels.

    Caps at max_labels by evenly sampling; always includes the first and last.
    """
    if len(quarters) <= max_labels:
        return list(range(len(quarters)))
    step = (len(quarters) - 1) / (max_labels - 1)
    indices = sorted({0, len(quarters) - 1} | {round(i * step) for i in range(max_labels)})
    return indices[:max_labels]


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------


def _mermaid_xychart(title: str, quarters: list[str], values: list[int], ylabel: str) -> str:
    """Render a Mermaid xychart-beta bar chart block.

    Handles capping x-axis at 20 labels and computing y-axis range.
    """
    if len(quarters) < 4:
        return f"> ⚠️ Insufficient data for chart '{title}' (fewer than 4 quarters).\n"

    indices = sample_quarters(quarters)
    sampled_q = [quarters[i] for i in indices]
    sampled_v = [values[i] for i in indices]

    x_labels = ", ".join(f'"{q}"' for q in sampled_q)
    bar_values = ", ".join(str(v) for v in sampled_v)
    max_y = max(values) if values else 1

    lines = [
        "```mermaid",
        "xychart-beta",
        f'  title "{title}"',
        f"  x-axis [{x_labels}]",
        f'  y-axis "{ylabel}" 0 --> {max_y}',
        f"  bar [{bar_values}]",
        "```",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------


def _section_header(data: dict, categories: list[dict]) -> str:
    dr = data["data_range"]
    totals = data["totals"]
    total_comments = totals["comments_analyzed"] + totals["reviews_analyzed"]
    return (
        "# SCT PR Review Comment Taxonomy Report\n\n"
        f"Generated: {date.today().isoformat()}  \n"
        f"Data: {dr['from']} to {dr['to']} "
        f"| {totals['prs_analyzed']} PRs "
        f"| {total_comments} comments + reviews\n\n"
    )


def _section_executive_summary(data: dict, cat_index: dict[str, dict]) -> str:
    quarterly_counts: dict[str, dict[str, int]] = data["quarterly_counts"]
    inflection_points: dict[str, dict] = data["inflection_points"]

    # Compute total count per category across all quarters
    cat_totals: dict[str, int] = {}
    for cat_id in inflection_points:
        cat_totals[cat_id] = sum(q_counts.get(cat_id, 0) for q_counts in quarterly_counts.values())

    # Top 3 GROWING/PERSISTENT by total count
    growing_cats = sorted(
        [
            cat_id
            for cat_id, info in inflection_points.items()
            if info.get("status") in ("GROWING", "PERSISTENT") and cat_id != UNCATEGORIZED_ID
        ],
        key=lambda c: cat_totals.get(c, 0),
        reverse=True,
    )[:3]

    # Top 3 DECLINING (automated / improving)
    declining_cats = sorted(
        [
            cat_id
            for cat_id, info in inflection_points.items()
            if info.get("status") == "DECLINING" and cat_id != UNCATEGORIZED_ID
        ],
        key=lambda c: cat_totals.get(c, 0),
        reverse=True,
    )[:3]

    # Check for Feb 2026 T3 inflection (emergence in 2026)
    t3_info = inflection_points.get("T3_AI_CODE_QUALITY", {})
    t3_emergence = t3_info.get("emergence_quarter", "")
    ai_inflection_note = ""
    if t3_emergence and "2026" in str(t3_emergence):
        ai_inflection_note = (
            f"\n> **Feb 2026 inflection**: T3 (AI Code Quality) showed a statistically "
            f"significant emergence in {t3_emergence}, coinciding with broader AI-assisted "
            f"PR authoring adoption.\n"
        )

    lines = ["## Executive Summary\n"]

    lines.append("### 🔴 Persistent / Growing Issues (require skills)\n")
    if growing_cats:
        for cat_id in growing_cats:
            cat = cat_index.get(cat_id, {})
            name = cat.get("name", cat_id)
            traj = TRAJECTORY_EMOJI.get(cat.get("trajectory", ""), "")
            lines.append(f"- **{cat_id}: {name}** — {cat_totals.get(cat_id, 0)} occurrences {traj}")
    else:
        lines.append("- *(no data)*")

    lines.append("\n### 🟢 Declining / Automated Issues (already improving)\n")
    if declining_cats:
        for cat_id in declining_cats:
            cat = cat_index.get(cat_id, {})
            name = cat.get("name", cat_id)
            traj = TRAJECTORY_EMOJI.get(cat.get("trajectory", ""), "")
            lines.append(f"- **{cat_id}: {name}** — {cat_totals.get(cat_id, 0)} occurrences {traj}")
    else:
        lines.append("- *(no declining categories found in data)*")

    if ai_inflection_note:
        lines.append(ai_inflection_note)

    return "\n".join(lines) + "\n\n"


def _section_overall_volume(data: dict) -> str:
    quarterly_counts: dict[str, dict[str, int]] = data["quarterly_counts"]
    dr = data["data_range"]

    quarters = all_quarters_in_range(dr["from"], dr["to"])
    totals_per_q = [sum(quarterly_counts.get(q, {}).values()) for q in quarters]

    lines = ["## Overall Review Comment Volume\n"]
    chart = _mermaid_xychart(
        title="Total Review Comments per Quarter",
        quarters=quarters,
        values=totals_per_q,
        ylabel="Comments",
    )
    lines.append(chart)
    return "\n".join(lines) + "\n"


def _section_per_category_charts(data: dict, cat_index: dict[str, dict]) -> str:  # noqa: PLR0914
    quarterly_counts: dict[str, dict[str, int]] = data["quarterly_counts"]
    inflection_points: dict[str, dict] = data["inflection_points"]
    dr = data["data_range"]
    quarters = all_quarters_in_range(dr["from"], dr["to"])

    lines = ["## Per-Category Trends\n"]

    for cat_id, info in sorted(inflection_points.items(), key=lambda x: x[0]):
        if cat_id == UNCATEGORIZED_ID:
            continue
        cat = cat_index.get(cat_id, {})
        name = cat.get("name", cat_id)
        trajectory = info.get("status", cat.get("trajectory", "PERSISTENT"))
        traj_emoji = TRAJECTORY_EMOJI.get(trajectory, "")

        total = sum(quarterly_counts.get(q, {}).get(cat_id, 0) for q in quarters)

        # Top reviewers for this category
        reviewer_matrix: dict[str, dict[str, int]] = data.get("reviewer_matrix", {})
        reviewer_scores = sorted(
            ((r, counts.get(cat_id, 0)) for r, counts in reviewer_matrix.items()),
            key=lambda x: x[1],
            reverse=True,
        )
        top_reviewers = [(r, n) for r, n in reviewer_scores if n > 0][:5]
        reviewers_str = ", ".join(f"{r} ({n})" for r, n in top_reviewers) or "N/A"

        values = [quarterly_counts.get(q, {}).get(cat_id, 0) for q in quarters]

        lines.append(f"## {cat_id}: {name}\n")
        lines.append(f"**Total occurrences**: {total} | **Trajectory**: {trajectory} {traj_emoji}  ")
        lines.append(f"**Primary reviewers**: {reviewers_str}\n")

        chart = _mermaid_xychart(
            title=f"{cat_id}: {name} — Issues per Quarter",
            quarters=quarters,
            values=values,
            ylabel="Occurrences",
        )
        lines.append(chart)

    return "\n".join(lines) + "\n"


def _section_ai_vs_human(data: dict, cat_index: dict[str, dict]) -> str:
    ab: dict = data.get("author_breakdown", {})
    human = ab.get("human", {"pr_count": 0, "comment_count": 0, "category_counts": {}})
    ai = ab.get("ai", {"pr_count": 0, "comment_count": 0, "category_counts": {}})

    human_total = human["comment_count"] or 1
    ai_total = ai["comment_count"] or 1

    # Rows: one per category (T1–T13) plus header
    rows = [
        "## AI-Authored vs Human-Authored PRs\n",
        "| Metric | Human PRs | AI PRs |",
        "|--------|-----------|--------|",
        f"| PR count | {human['pr_count']} | {ai['pr_count']} |",
        f"| Total comments | {human['comment_count']} | {ai['comment_count']} |",
    ]

    for cat_id, cat in sorted(cat_index.items()):
        if cat_id == UNCATEGORIZED_ID:
            continue
        name = cat.get("name", cat_id)
        h_count = human["category_counts"].get(cat_id, 0)
        a_count = ai["category_counts"].get(cat_id, 0)
        h_rate = h_count / human_total * 100
        a_rate = a_count / ai_total * 100
        rows.append(f"| {cat_id} ({name}) rate | {h_rate:.1f}% | {a_rate:.1f}% |")

    return "\n".join(rows) + "\n\n"


def _section_reviewer_matrix(data: dict, cat_index: dict[str, dict]) -> str:
    reviewer_matrix: dict[str, dict[str, int]] = data.get("reviewer_matrix", {})

    # Compute totals, sort descending, take top 10
    reviewer_totals = {r: sum(counts.values()) for r, counts in reviewer_matrix.items()}
    top_reviewers = sorted(reviewer_totals, key=reviewer_totals.get, reverse=True)[:10]

    lines = [
        "## Reviewer Specialization\n",
        "| Reviewer | Top Category | 2nd Category | 3rd Category | Total Comments |",
        "|----------|-------------|--------------|--------------|----------------|",
    ]

    for reviewer in top_reviewers:
        counts = reviewer_matrix[reviewer]
        # Exclude T0 from top-category ranking
        sorted_cats = sorted(
            ((c, n) for c, n in counts.items() if c != UNCATEGORIZED_ID and n > 0),
            key=lambda x: x[1],
            reverse=True,
        )
        top3 = [(c, n) for c, n in sorted_cats[:3]]
        cells = [f"{c} ({n})" for c, n in top3]
        while len(cells) < 3:
            cells.append("—")
        total = reviewer_totals[reviewer]
        lines.append(f"| {reviewer} | {cells[0]} | {cells[1]} | {cells[2]} | {total} |")

    return "\n".join(lines) + "\n\n"


def _section_deep_dives(data: dict, cat_index: dict[str, dict]) -> str:  # noqa: PLR0914
    quarterly_counts: dict[str, dict[str, int]] = data["quarterly_counts"]
    inflection_points: dict[str, dict] = data["inflection_points"]
    dr = data["data_range"]
    quarters = all_quarters_in_range(dr["from"], dr["to"])

    lines = ["## Category Deep Dives\n"]

    for cat_id in sorted(cat_index):
        if cat_id == UNCATEGORIZED_ID:
            continue
        cat = cat_index[cat_id]
        name = cat.get("name", cat_id)
        info = inflection_points.get(cat_id, {})
        trajectory = info.get("status", cat.get("trajectory", "PERSISTENT"))
        traj_emoji = TRAJECTORY_EMOJI.get(trajectory, "")

        total = sum(quarterly_counts.get(q, {}).get(cat_id, 0) for q in quarters)
        pilot_count = cat.get("pilot_count", 0)
        first_seen = info.get("first_seen", "N/A")
        peak_q = info.get("peak_quarter", "N/A")
        peak_n = info.get("peak_count", 0)
        enforcers = ", ".join(cat.get("enforced_by") or []) or "N/A"

        quotes = REPRESENTATIVE_QUOTES.get(cat_id, [])
        quotes_md = "\n".join(f'> "{q}"' for q in quotes)

        lines.append(f"### {cat_id}: {name}\n")
        lines.append(
            f"**Pilot count**: {pilot_count} | **7-year total**: {total} | **Trajectory**: {trajectory} {traj_emoji}  "
        )
        lines.append(f"**Emerged**: {first_seen} | **Peak**: {peak_q} ({peak_n} occurrences)  ")
        lines.append(f"**Primary enforcers**: {enforcers}\n")
        if quotes_md:
            lines.append("**What reviewers say**:\n")
            lines.append(quotes_md)
        lines.append("")

    return "\n".join(lines) + "\n"


def _section_recommendations(data: dict, cat_index: dict[str, dict]) -> str:  # noqa: PLR0914
    quarterly_counts: dict[str, dict[str, int]] = data["quarterly_counts"]
    dr = data["data_range"]
    quarters = all_quarters_in_range(dr["from"], dr["to"])

    # Compute totals
    cat_totals: dict[str, int] = {}
    for cat_id in cat_index:
        cat_totals[cat_id] = sum(quarterly_counts.get(q, {}).get(cat_id, 0) for q in quarters)

    t1 = cat_totals.get("T1_TEST_QUALITY", 0)
    t2 = cat_totals.get("T2_MISSING_DOCUMENTATION", 0)
    t3 = cat_totals.get("T3_AI_CODE_QUALITY", 0)
    t4 = cat_totals.get("T4_CODE_STRUCTURE", 0)
    t5 = cat_totals.get("T5_SCOPE_HYGIENE", 0)
    t6 = cat_totals.get("T6_ERROR_HANDLING", 0)
    t7 = cat_totals.get("T7_MISSING_TESTS", 0)
    t8 = cat_totals.get("T8_PLAN_QUALITY", 0)
    t13 = cat_totals.get("T13_LOGIC_CORRECTNESS", 0)

    t3_threshold = 50
    t8_threshold = 30
    t3_justified = (
        f"T3 occurrences ({t3}) exceed threshold of {t3_threshold}"
        if t3 >= t3_threshold
        else f"T3 occurrences ({t3}) do not yet exceed threshold of {t3_threshold}"
    )
    t8_priority = "HIGH" if t8 >= t8_threshold else "MEDIUM"
    t8_justified = (
        f"T8 occurrences ({t8}) exceed threshold of {t8_threshold}"
        if t8 >= t8_threshold
        else f"T8 occurrences ({t8}) do not yet exceed threshold of {t8_threshold}"
    )

    # Ordered checklist for code-review skill (by 7-year occurrence count)
    checklist_items = [
        ("T1", "Test Quality", t1, "use pytest functions, fixtures, parametrize"),
        ("T2", "Missing Documentation", t2, "config help, docstrings, 'why' comments"),
        ("T4", "Code Structure", t4, "avoid inner functions, mutable defaults, centralize"),
        ("T5", "Scope Hygiene", t5, "changes must be scoped to the PR"),
        ("T6", "Error Handling", t6, "try/except/finally, raise events, validate inputs"),
        ("T7", "Missing Tests", t7, "unit tests for new parsing / config / utility logic"),
        ("T13", "Logic Correctness", t13, "verify data structures, formats, return values"),
    ]
    checklist_items.sort(key=lambda x: x[2], reverse=True)

    checklist_md = "\n".join(
        f"{i + 1}. [ ] {label} ({tid}): {count} occurrences — {guidance}"
        for i, (tid, label, count, guidance) in enumerate(checklist_items)
    )

    return (
        "## Recommendations for Skill Design\n\n"
        "### Skills to Create (data-backed)\n\n"
        "#### 1. `skills/code-review/` — Priority: HIGH\n"
        "Checklist ordered by 7-year occurrence count:\n\n"
        f"{checklist_md}\n\n"
        f"#### 2. `skills/ai-code-audit/` — Priority: HIGH (T3 count: {t3})\n"
        f"Justified: {t3_justified}.\n\n"
        f"#### 3. `skills/plan-review/` — Priority: {t8_priority} (T8 count: {t8})\n"
        f"Justified: {t8_justified}.\n\n"
        "### Automated Checks Already in Place\n\n"
        "- T9 (Unused/Dead Code): automated by CodeQL and github-code-quality bot\n"
        "- Formatting: handled by pre-commit (ruff, autopep8)\n"
        "- Commit message format: handled by commitlint\n"
    )


# ---------------------------------------------------------------------------
# Report assembler
# ---------------------------------------------------------------------------


def generate_report(data: dict, categories: list[dict]) -> str:
    """Assemble all sections into a single Markdown string.

    Args:
        data: Parsed report_data.json dictionary.
        categories: Loaded taxonomy category list from taxonomy.yaml.

    Returns:
        str: Full Markdown report content.
    """
    cat_index = build_cat_index(categories)

    sections = [
        _section_header(data, categories),
        _section_executive_summary(data, cat_index),
        _section_overall_volume(data),
        _section_per_category_charts(data, cat_index),
        _section_ai_vs_human(data, cat_index),
        _section_reviewer_matrix(data, cat_index),
        _section_deep_dives(data, cat_index),
        _section_recommendations(data, cat_index),
    ]

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Markdown taxonomy report from report_data.json.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to report_data.json (output of analyze.py).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="FILE",
        help="Output path for the Markdown report.",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to taxonomy.yaml file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.input.is_file():
        log.error("Input file not found: %s", args.input)
        return 1
    if not args.taxonomy.is_file():
        log.error("Taxonomy file not found: %s", args.taxonomy)
        return 1

    log.info("Loading report data from %s", args.input)
    try:
        with args.input.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.error("Failed to read input file: %s", exc)
        return 1

    log.info("Loading taxonomy from %s", args.taxonomy)
    categories = load_taxonomy(args.taxonomy)
    log.info("Loaded %d categories", len(categories))

    log.info("Generating report...")
    report_md = generate_report(data, categories)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with args.output.open("w", encoding="utf-8") as fh:
            fh.write(report_md)
        log.info("Report written to %s (%d chars)", args.output, len(report_md))
    except OSError as exc:
        log.error("Failed to write output file: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
