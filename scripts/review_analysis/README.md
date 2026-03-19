# PR Review Taxonomy Analysis Pipeline

## Purpose

This pipeline analyzes pull request review patterns in the SCT (Scylla Cluster Tests) repository to build a data-driven taxonomy of review categories. It helps identify:

- **Skill gaps**: Which types of code changes require the most review effort
- **Team patterns**: How review effort is distributed across different change types
- **Improvement opportunities**: Categories with high review frequency that could benefit from automated checks or documentation

The analysis covers all PRs since 2019, examining reviewer comments to classify them into 14 categories (T0_UNCATEGORIZED through T13_LOGIC_CORRECTNESS). Results are visualized in a Markdown report with Mermaid charts showing trends over time.

## Prerequisites

- `gh` CLI installed and authenticated
  ```bash
  gh auth status
  ```
- Python 3.9+ with PyYAML
  ```bash
  pip install pyyaml
  # or with uv:
  uv add pyyaml
  ```
- Run from repo root or `scripts/review_analysis/` directory

## Quick Start

### First Run (takes ~7 hours)

```bash
cd scripts/review_analysis
./run.sh --since 2019-01-01 --delay 2.0
```

**Estimated time**: ~7 hours
- 6,500 PRs × 2 API calls per PR × 2 seconds delay = ~7.2 hours
- The script runs continuously; you can monitor progress in the terminal

**Output**: Report is saved to `./reports/pr-review-taxonomy-report.md`

### Quarterly Update (subsequent runs — ~30 minutes)

```bash
cd scripts/review_analysis
./run.sh --no-collect
```

This skips the collection phase (Phase 1) and only re-classifies, analyzes, and reports. Use this when your cache is recent and you only want to update the report.

Alternatively, fetch only new PRs since a specific date:

```bash
./run.sh --since 2026-01-01
```

The `collect.py` script handles deduplication, so re-running with a new `--since` date will only fetch PRs you haven't already cached.

## Running Individual Phases

If you need to run only specific phases (e.g., to tune the taxonomy without re-collecting):

```bash
# Phase 1: Collect PR comments from GitHub API
python collect.py \
  --repo scylladb/scylla-cluster-tests \
  --since 2019-01-01 \
  --cache-dir ./cache/raw \
  --delay 2.0 \
  --resume

# Phase 2: Classify comments using taxonomy.yaml
python classify.py \
  --input ./cache/raw \
  --output ./cache/classified \
  --taxonomy taxonomy.yaml

# Phase 3: Analyze trends and patterns
python analyze.py \
  --input ./cache/classified \
  --output ./cache/report_data.json \
  --taxonomy taxonomy.yaml

# Phase 4: Generate Markdown report with charts
python report.py \
  --input ./cache/report_data.json \
  --output ./reports/pr-review-taxonomy-report.md \
  --taxonomy taxonomy.yaml
```

## Interpreting the Report

The generated report includes:

- **Category Trends**: Quarterly counts for each category over time
  - **GROWING ⬆**: Category increasing over time — top skill priority
  - **PERSISTENT →**: Category stable at moderate frequency — include in skill checklist
  - **DECLINING ⬇**: Category decreasing — lower priority, may indicate team improvement
  - **AUTOMATED ✓**: Category handled by existing bots — no skill needed

- **Reviewer Matrix**: Which reviewers handle which categories
- **Uncategorized Rate**: Percentage of comments that didn't match any pattern
  - Target: < 30%
  - Higher rates indicate taxonomy patterns need tuning

- **Mermaid Charts**: Visual trends showing category frequency over quarters

## Adding New Taxonomy Categories

1. Edit `taxonomy.yaml`
2. Add a new entry with:
   - `id`: Unique identifier (e.g., `T14_NEW_CATEGORY`)
   - `name`: Human-readable name
   - `description`: What this category covers
   - `patterns`: List of regex patterns to match comments
   - `keywords`: List of keywords for fallback matching
   - `bot_auto_tag`: Whether bots automatically tag this (true/false)
   - `trajectory`: Expected trend (growing/stable/declining)

3. Re-run phases 2–4 (skip collection for speed):
   ```bash
   ./run.sh --no-collect
   ```

4. Update unit tests with test cases for the new category:
   ```bash
   # Edit unit_tests/test_review_classifier.py
   # Add test cases for the new category patterns
   uv run python -m pytest unit_tests/test_review_classifier.py -v
   ```

## Tuning Classification Accuracy

The classifier uses regex patterns in `taxonomy.yaml`. To improve accuracy:

1. Find misclassified comments in the output
   - Look for unexpected T0 (uncategorized) entries in the report
   - Or comments that were classified incorrectly

2. Add matching patterns to the relevant category in `taxonomy.yaml`
   ```yaml
   T1_DOCUMENTATION:
     patterns:
       - "(?i)doc.*missing"
       - "(?i)comment.*unclear"
       - "(?i)readme.*update"
   ```

3. Re-run phases 2–4:
   ```bash
   ./run.sh --no-collect
   ```

4. Verify with unit tests:
   ```bash
   uv run python -m pytest unit_tests/test_review_classifier.py -v
   ```

## Cache Structure

```
cache/
  raw/
    pr_list.json         # Index of all PRs fetched
    100.json             # PR #100 with inline comments + reviews
    101.json             # PR #101
    ...
  classified/
    100.json             # Same as raw but with "categories" field per comment
    101.json
    ...
  report_data.json       # Aggregated quarterly counts, reviewer matrix, etc.

reports/
  pr-review-taxonomy-report.md   # Final report with Mermaid charts
```

## Rate Limits

GitHub API allows 5,000 requests/hour for authenticated users.

With `--delay 2.0`:
- One request every 2 seconds = 1,800 requests/hour
- Well within limits, no risk of hitting the rate limit

To speed up collection, use `--delay 1.5` (still safe):
```bash
./run.sh --since 2019-01-01 --delay 1.5
```

## Repeating the Analysis

The pipeline is designed to be idempotent:

- `collect.py --resume` skips already-cached PRs
- `classify.py` skips already-classified PRs
- Re-running the full pipeline fetches only new PRs since the last run

This means you can safely re-run the pipeline without worrying about duplicate data or wasted API calls.

## Files in This Directory

| File | Purpose |
|------|---------|
| `run.sh` | Pipeline wrapper — runs all 4 phases with a single command |
| `collect.py` | Phase 1: Fetch PR comments from GitHub API |
| `classify.py` | Phase 2: Classify comments using taxonomy.yaml patterns |
| `analyze.py` | Phase 3: Aggregate quarterly trends and reviewer statistics |
| `report.py` | Phase 4: Generate Markdown report with Mermaid charts |
| `taxonomy.yaml` | Category definitions with regex patterns and metadata |
| `README.md` | This file |

## Troubleshooting

### "gh: command not found"
Install the GitHub CLI: https://cli.github.com/

### "ModuleNotFoundError: No module named 'yaml'"
Install PyYAML:
```bash
pip install pyyaml
# or with uv:
uv add pyyaml
```

### "Rate limit exceeded"
The GitHub API rate limit is 5,000 requests/hour. If you hit it:
- Wait 1 hour for the limit to reset
- Or increase `--delay` to slow down collection (e.g., `--delay 3.0`)

### "Cache is stale, want to re-collect?"
If your cache is older than a few months, re-run collection:
```bash
./run.sh --since 2026-01-01
```

This fetches only new PRs since the specified date.

### "Uncategorized rate is too high (> 30%)"
The taxonomy patterns need tuning:
1. Review the report to see which comments are uncategorized
2. Add patterns to `taxonomy.yaml` for the relevant categories
3. Re-run phases 2–4: `./run.sh --no-collect`
4. Check the updated report

## Next Steps

After running the pipeline:

1. **Review the report**: `cat ./reports/pr-review-taxonomy-report.md`
2. **Identify high-priority categories**: Look for GROWING ⬆ trends
3. **Create skills**: Use the taxonomy to guide skill development
4. **Schedule quarterly updates**: Set a reminder to re-run the pipeline every 3 months
5. **Iterate on taxonomy**: Refine patterns based on misclassifications
