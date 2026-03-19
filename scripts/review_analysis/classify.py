#!/usr/bin/env python3
"""Multi-label comment classifier for PR review analysis.

Loads taxonomy.yaml and classifies comments in cached PR JSON files
using regex pattern matching. Supports bot auto-tagging and multi-label
classification (a single comment can match multiple categories).

Usage:
    python classify.py \\
        --input ./cache/raw \\
        --output ./cache/classified \\
        --taxonomy scripts/review_analysis/taxonomy.yaml
"""

import argparse
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

UNCATEGORIZED_ID = "T0_UNCATEGORIZED"


def load_taxonomy(taxonomy_path: Path) -> list[dict]:
    """Load and return the list of category definitions from taxonomy.yaml."""
    with taxonomy_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data["categories"]


def compile_taxonomy(categories: list[dict]) -> list[dict]:
    """Compile regex patterns for each category at startup.

    Returns a list of dicts with:
        id          – category ID string
        compiled    – list of compiled re.Pattern objects
        bot_auto_tag – set of bot login strings
    """
    compiled_categories = []
    for cat in categories:
        cat_id = cat["id"]
        if cat_id == UNCATEGORIZED_ID:
            # T0 is assigned programmatically, no patterns to compile.
            continue
        raw_patterns = cat.get("patterns") or []
        compiled_patterns = []
        for raw in raw_patterns:
            try:
                compiled_patterns.append(re.compile(raw, re.IGNORECASE))
            except re.error as exc:
                log.warning("Bad regex pattern %r in category %s: %s", raw, cat_id, exc)
        bot_logins = set(cat.get("bot_auto_tag") or [])
        compiled_categories.append(
            {
                "id": cat_id,
                "compiled": compiled_patterns,
                "bot_auto_tag": bot_logins,
            }
        )
    return compiled_categories


def classify_body(body: str, compiled_categories: list[dict], user_login: str) -> list[str]:
    """Return a list of matching category IDs for the given comment body.

    Bot auto-tagging is checked first. If the user matches a bot_auto_tag list,
    that category is assigned immediately (no regex run for that category).

    A comment is multi-label: all matching categories are returned.
    If nothing matches, returns [UNCATEGORIZED_ID].
    """
    matched: list[str] = []

    for cat in compiled_categories:
        # Bot auto-tagging: check if user login is in this category's bot list.
        if user_login and cat["bot_auto_tag"] and user_login in cat["bot_auto_tag"]:
            matched.append(cat["id"])
            continue

        # Regex matching against body text.
        for pattern in cat["compiled"]:
            if pattern.search(body):
                matched.append(cat["id"])
                break  # Already matched this category — no need to check more patterns.

    return matched if matched else [UNCATEGORIZED_ID]


def classify_item(item: dict, compiled_categories: list[dict]) -> list[str]:
    """Classify a single comment/review item and return its category list."""
    body = item.get("body") or ""
    user_login = item.get("user") or ""
    return classify_body(body, compiled_categories, user_login)


def process_pr_file(
    pr_file: Path,
    output_dir: Path,
    compiled_categories: list[dict],
) -> tuple[int, int, int, int]:
    """Classify all comments and review bodies in one PR JSON file.

    Returns (comments_count, reviews_count, bot_tagged_count, pr_number).
    """
    try:
        with pr_file.open("r", encoding="utf-8") as fh:
            pr_data = json.load(fh)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Skipping malformed PR file %s: %s", pr_file, exc)
        return 0, 0, 0, -1

    if not isinstance(pr_data, dict):
        log.debug("Skipping non-PR file %s (got %s)", pr_file.name, type(pr_data).__name__)
        return 0, 0, 0, -1
    pr_number = pr_data.get("pr_number", pr_file.stem)
    output_path = output_dir / f"{pr_number}.json"

    # Idempotency: skip already-classified output files.
    if output_path.exists():
        log.debug("Skipping already-classified PR %s", pr_number)
        return 0, 0, 0, int(pr_number) if str(pr_number).isdigit() else -1

    comments_classified = 0
    reviews_classified = 0
    bot_tagged = 0

    # Classify inline comments.
    for item in pr_data.get("comments", []):
        if not (item.get("body") or "").strip():
            item["categories"] = [UNCATEGORIZED_ID]
            continue
        categories = classify_item(item, compiled_categories)
        item["categories"] = categories
        comments_classified += 1
        # Count bot-tagged items (tagged via bot_auto_tag, not just any T9).
        user_login = item.get("user") or ""
        if any(user_login in cat["bot_auto_tag"] for cat in compiled_categories if cat["bot_auto_tag"]):
            bot_tagged += 1

    # Classify review body comments (only non-empty bodies).
    for item in pr_data.get("reviews", []):
        body = (item.get("body") or "").strip()
        if not body:
            # Skip empty review bodies — don't classify them.
            continue
        categories = classify_item(item, compiled_categories)
        item["categories"] = categories
        reviews_classified += 1
        user_login = item.get("user") or ""
        if any(user_login in cat["bot_auto_tag"] for cat in compiled_categories if cat["bot_auto_tag"]):
            bot_tagged += 1

    # Write augmented PR JSON to output directory.
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        with output_path.open("w", encoding="utf-8") as fh:
            json.dump(pr_data, fh, indent=2, ensure_ascii=False)
    except OSError as exc:
        log.warning("Failed to write output for PR %s: %s", pr_number, exc)

    return comments_classified, reviews_classified, bot_tagged, pr_number


def print_summary(
    total_prs: int,
    total_comments: int,
    total_reviews: int,
    category_counts: dict[str, int],
    bot_tagged_total: int,
    all_categories: list[dict],
) -> None:
    """Print the classification summary report to stdout."""
    grand_total = total_comments + total_reviews
    uncategorized = category_counts.get(UNCATEGORIZED_ID, 0)
    uncategorized_rate = (uncategorized / grand_total * 100) if grand_total else 0.0

    # Build ordered category list (T0 last).
    ordered_ids = [cat["id"] for cat in all_categories if cat["id"] != UNCATEGORIZED_ID]
    ordered_ids.append(UNCATEGORIZED_ID)

    # Max width for formatting.
    max_id_len = max((len(cid) for cid in ordered_ids), default=20)
    col = max(max_id_len, 24)

    print()
    print("=== Classification Summary ===")
    print(f"{'Total PRs processed':<30}: {total_prs:>6}")
    print(f"{'Total comments classified':<30}: {total_comments:>6}")
    print(f"{'Total review bodies':<30}: {total_reviews:>6}")
    print()
    print("Category breakdown (comments + reviews):")
    for cat_id in ordered_ids:
        count = category_counts.get(cat_id, 0)
        pct = (count / grand_total * 100) if grand_total else 0.0
        print(f"  {cat_id:<{col}}: {count:>6} ({pct:>6.1f}%)")
    print()
    print(f"{'Bot auto-tagged':<30}: {bot_tagged_total:>6}")
    print(f"Uncategorized rate        : {uncategorized_rate:>5.1f}%  (target: <30%)")
    print()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify PR review comments using taxonomy patterns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="DIR",
        help="Directory containing raw PR JSON files (output of collect.py).",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="DIR",
        help="Directory to write augmented PR JSON files into.",
    )
    parser.add_argument(
        "--taxonomy",
        required=True,
        type=Path,
        metavar="FILE",
        help="Path to taxonomy.yaml file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0914
    args = parse_args(argv)

    # Validate inputs.
    if not args.input.is_dir():
        log.error("Input directory does not exist: %s", args.input)
        return 1
    if not args.taxonomy.is_file():
        log.error("Taxonomy file not found: %s", args.taxonomy)
        return 1

    # Load and compile taxonomy once at startup.
    log.info("Loading taxonomy from %s", args.taxonomy)
    all_categories = load_taxonomy(args.taxonomy)
    compiled_categories = compile_taxonomy(all_categories)
    log.info(
        "Loaded %d categories (%d with patterns)",
        len(all_categories),
        len(compiled_categories),
    )

    # Gather and sort input files by PR number for reproducible output.
    input_files = sorted(
        args.input.glob("*.json"),
        key=lambda p: int(p.stem) if p.stem.isdigit() else 0,
    )
    if not input_files:
        log.warning("No JSON files found in %s", args.input)
        return 0

    log.info("Processing %d PR files from %s", len(input_files), args.input)
    args.output.mkdir(parents=True, exist_ok=True)

    # Accumulators for summary.
    total_prs = 0
    total_comments = 0
    total_reviews = 0
    bot_tagged_total = 0
    category_counts: dict[str, int] = defaultdict(int)

    for pr_file in input_files:
        comments_n, reviews_n, bot_n, pr_num = process_pr_file(pr_file, args.output, compiled_categories)
        if pr_num == -1:
            continue  # Malformed file — already warned.

        total_prs += 1
        total_comments += comments_n
        total_reviews += reviews_n
        bot_tagged_total += bot_n

        # Re-read the output file to tally category counts.
        out_path = args.output / f"{pr_num}.json"
        if out_path.exists():
            try:
                with out_path.open("r", encoding="utf-8") as fh:
                    pr_out = json.load(fh)
                for item in pr_out.get("comments", []):
                    for cat_id in item.get("categories", []):
                        category_counts[cat_id] += 1
                for item in pr_out.get("reviews", []):
                    body = (item.get("body") or "").strip()
                    if body:
                        for cat_id in item.get("categories", []):
                            category_counts[cat_id] += 1
            except (json.JSONDecodeError, OSError) as exc:
                log.warning("Could not re-read output for PR %s: %s", pr_num, exc)

    print_summary(
        total_prs=total_prs,
        total_comments=total_comments,
        total_reviews=total_reviews,
        category_counts=category_counts,
        bot_tagged_total=bot_tagged_total,
        all_categories=all_categories,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
