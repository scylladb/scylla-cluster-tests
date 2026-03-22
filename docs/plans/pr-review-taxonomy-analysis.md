---
status: in_progress
domain: ai-tooling
created: 2026-03-20
last_updated: 2026-03-22
owner: fruch
---

# PR Review Taxonomy Analysis — 7-Year Deep Dive

## TL;DR

> **Objective**: Mine 7 years of SCT PR review comments (~6,500 PRs with reviews out of ~11,900 total), classify them into a data-driven taxonomy, produce a trend report with charts, and use the findings to design the code-review skill and identify what other skills are needed.
>
> **Deliverables**:
> - Data collection script (throttled, resumable, cached)
> - Classification engine (regex + keyword, ~75% accuracy target)
> - Trend report with per-quarter charts per issue category
> - Updated code-review skill with data-backed priority ordering
> - Repeatable pipeline runnable quarterly
>
> **Estimated Effort**: Large
> **Critical Path**: Collect → Classify → Analyze → Report → Skill updates

---

## Context

### Original Request
Go over all SCT PR review comments across the full project history, classify them into common issues, design changes to the review skill to address the most common ones, document the process for repeatability, and suggest additional skills to improve the review process.

### Pilot Findings (from .sisyphus/drafts/pr-review-taxonomy.md)
A pilot of ~45 PRs (Aug 2024 – Mar 2026, ~600+ comments) identified 13 categories:

| # | Category | Pilot Count | Trajectory |
|---|----------|-------------|------------|
| 1 | Test Quality (pytest, fixtures, parametrize) | 38 | PERSISTENT ⬆ |
| 2 | AI-Generated Code Quality | 28 | EMERGED Jan 2026 ⬆ |
| 3 | Missing Documentation | 19 | PERSISTENT → |
| 4 | Plan Review Issues | 18 | EMERGED Feb 2026 ⬇ |
| 5 | Code Structure / Simplification | 16 | PERSISTENT → |
| 6 | Unused Imports (automated by CodeQL) | 13 | AUTOMATED ✓ |
| 7 | Scope / PR Hygiene | 12 | PERSISTENT → |
| 8 | Error Handling | 9 | INTERMITTENT |
| 9 | Missing Unit Tests | 8 | PERSISTENT → |
| 10 | Consistency / Unification | 8 | PERSISTENT → |
| 11 | Configuration System | 8 | PERSISTENT → |
| 12 | Dead / Commented-Out Code | 7 | DECLINING ⬇ |
| 13 | Naming / Abstraction | 5 | PERSISTENT → |

### Repository Scale

| Year | Total PRs | Est. PRs w/ comments | Notes |
|------|-----------|---------------------|-------|
| 2019 | 757 | ~430 | First substantial year |
| 2020 | 1,245 | ~650 | Growth |
| 2021 | 1,022 | ~550 | Stable |
| 2022 | 1,034 | ~900 | Maturing |
| 2023 | 1,109 | ~950 | Pre-AI era |
| 2024 | 2,177 | ~940 | AI adoption begins |
| 2025 | 2,974 | ~1,624 | Heavy AI PRs |
| 2026 | 989 (Q1) | ~500 | AI review infra live |
| **Total** | **~11,900** | **~6,500** | |

### API Budget Constraints
- **GitHub API rate limit**: 5,000 requests/hour (authenticated via `gh`)
- Each PR needs ~2 API calls (comments + reviews)
- Full 6,500-PR scan ≈ ~13,000 calls
- **At gentle pace (1 req/sec)**: ~3.6 hours wall time
- **At very gentle pace (1 req/2sec)**: ~7.2 hours wall time
- Results MUST be cached so re-runs cost zero for already-fetched PRs

---

## Work Objectives

### Core Objective
Produce a quantitative, data-driven taxonomy of PR review issues across 7 years, with temporal trend analysis and actionable skill design recommendations.

### Concrete Deliverables
- `scripts/review_analysis/collect.py` — Throttled data collector
- `scripts/review_analysis/classify.py` — Comment classifier
- `scripts/review_analysis/analyze.py` — Trend aggregator
- `scripts/review_analysis/report.py` — Report + chart generator
- `scripts/review_analysis/taxonomy.yaml` — Category definitions + regex patterns
- `scripts/review_analysis/run.sh` — One-command pipeline
- `scripts/review_analysis/README.md` — Process documentation
- Report output (markdown + charts)

### Must Have
- Throttled API collection (max 1 request/second, configurable)
- Resume capability (re-running skips already-cached PRs)
- Local JSON cache per PR
- At least 13 categories from pilot taxonomy
- Per-quarter trend data for each category
- Trend charts (Mermaid or matplotlib SVG)
- Reviewer specialization matrix
- AI vs human author comparison

### Must NOT Have
- No real-time dashboard (batch reports only)
- No database dependency (flat JSON files)
- No changes to GitHub workflows or CI
- No external service dependencies beyond `gh` CLI

---

## Verification Strategy

### Test Decision
- **Infrastructure exists**: YES (pytest)
- **Automated tests**: YES (tests-after)
- **Framework**: pytest

### QA Policy
- Unit tests for classifier regex patterns
- Integration test on 50 known-classified PRs from pilot
- Manual accuracy check on 30 random comments per quarter-bin

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (Foundation — can all start immediately):
├── Task 1: Taxonomy YAML definition [quick]
├── Task 2: Data collection script skeleton [deep]
├── Task 3: Classification regex patterns [unspecified-high]

Wave 2 (After Wave 1 — core pipeline):
├── Task 4: Collection script full implementation (depends: 2) [deep]
├── Task 5: Classifier implementation (depends: 1, 3) [deep]
├── Task 6: Unit tests for classifier (depends: 3) [quick]

Wave 3 (After Wave 2 — analysis + reporting):
├── Task 7: Trend aggregation script (depends: 5) [unspecified-high]
├── Task 8: Report generator with charts (depends: 7) [unspecified-high]
├── Task 9: Pipeline wrapper script + README (depends: 4, 8) [quick]

Wave 4 (After data is collected — skill design):
├── Task 10: Run full collection (depends: 4) [NOTE: long-running, ~4-7 hours]
├── Task 11: Run classification + analysis (depends: 5, 7, 10) [quick]
├── Task 12: Generate report (depends: 8, 11) [quick]
├── Task 13: Skill design recommendations document (depends: 12) [writing]

Wave FINAL (Verification):
├── F1: Verify report accuracy against pilot data [unspecified-high]
├── F2: Verify pipeline re-run uses cache [quick]
├── F3: Review report completeness [deep]
```

---

## TODOs

- [x] 1. Define taxonomy YAML with categories and regex patterns

  **What to do**:
  - Create `scripts/review_analysis/taxonomy.yaml` defining all 13 categories
  - Each category: id, name, description, regex patterns (list), keywords (list)
  - Include the 13 categories from pilot + a T0_UNCATEGORIZED catch-all
  - Patterns derived from actual review comments observed in pilot

  **References**:
  - `.sisyphus/drafts/pr-review-taxonomy.md` — Full pilot analysis with examples per category
  - Classification rubric table in that draft (detection keywords per category)

  **Acceptance Criteria**:
  - [ ] YAML file parses without error
  - [ ] All 13 categories defined with ≥ 2 regex patterns each
  - [ ] Patterns tested against 10 known examples from pilot

---

- [x] 2. Build throttled, resumable data collection script

  **What to do**:
  - Create `scripts/review_analysis/collect.py`
  - Uses `subprocess` to call `gh api` (avoids Python GitHub library dependency)
  - **Throttling**: Configurable delay between API calls, default 1.5 seconds
  - **Resume**: Check cache dir for existing `{pr_number}.json`, skip if present
  - **Pagination**: Handle GitHub API pagination for PR listing (100 per page)
  - **Two-pass collection**:
    - Pass 1: List all PRs (paginate through all pages), save PR metadata
    - Pass 2: For each PR, fetch comments + reviews, save to `{pr_number}.json`
  - **Progress reporting**: Print progress every 100 PRs
  - **Error handling**: On API error, wait 60s and retry (max 3 retries)
  - **Rate limit awareness**: Check `X-RateLimit-Remaining` header, pause if < 100

  **CLI interface**:
  ```
  python collect.py \
    --repo scylladb/scylla-cluster-tests \
    --since 2019-01-01 \
    --cache-dir ./cache/raw \
    --delay 1.5 \
    --resume
  ```

  **Data schema per cached file** (`cache/raw/{pr_number}.json`):
  ```json
  {
    "pr_number": 14118,
    "created_at": "2026-03-18T...",
    "merged_at": "2026-03-18T...",
    "state": "merged",
    "author": "roydahan",
    "title": "feat(tags): add JenkinsJob tag...",
    "labels": ["backport/2026.1"],
    "is_ai_author": false,
    "comments": [...],
    "reviews": [...]
  }
  ```

  **AI author detection**: Flag PRs where author is "Copilot", "claude[bot]", or has `ai-assisted` label.

  **Must NOT do**:
  - Do NOT make more than 1 request per `--delay` seconds
  - Do NOT skip the cache check (must be resumable)
  - Do NOT store GitHub tokens in the script

  **Acceptance Criteria**:
  - [ ] `--delay 1.5` results in ≤ 1 request per 1.5 seconds (verified by timing 20 requests)
  - [ ] Re-running with `--resume` on a populated cache makes 0 API calls for cached PRs
  - [ ] Handles API 403 rate limit response by waiting and retrying
  - [ ] Progress output shows `[423/6500] PR#12345 — cached` or `[423/6500] PR#12345 — fetched (3 comments, 1 review)`

---

- [x] 3. Build comment classifier

  **What to do**:
  - Create `scripts/review_analysis/classify.py`
  - Loads taxonomy from `taxonomy.yaml`
  - For each comment in each cached PR JSON:
    - Run all category regex patterns against comment body
    - Assign matching categories (can be multi-label)
    - If no match → T0_UNCATEGORIZED
  - **Bot detection**: Auto-tag comments from known bots:
    - `github-code-quality[bot]` → T9 (Unused/Dead Code)
    - `copilot-pull-request-reviewer[bot]` → classify normally
  - **Output**: Augmented JSON with `categories` field per comment
  - **Stats output**: Print classification summary (count per category, uncategorized %)

  **CLI interface**:
  ```
  python classify.py \
    --input ./cache/raw \
    --output ./cache/classified \
    --taxonomy taxonomy.yaml
  ```

  **Acceptance Criteria**:
  - [ ] All cached PRs processed
  - [ ] Each comment has `categories: [...]` field (list of category IDs)
  - [ ] Uncategorized comments < 30% of total (excluding bot comments)
  - [ ] Classification summary printed to stdout

---

- [x] 4. Build trend aggregation script

  **What to do**:
  - Create `scripts/review_analysis/analyze.py`
  - Reads classified JSONs
  - Produces aggregations:
    1. **Per-quarter counts by category** (Q1-2019 through Q1-2026)
    2. **Per-reviewer-per-category matrix**
    3. **Human vs AI author breakdown**
    4. **Rolling 4-quarter moving average** per category
    5. **Inflection point detection**: Quarter where count first exceeded 2× prior 4-quarter average
  - Output: `report_data.json` with all aggregations

  **CLI interface**:
  ```
  python analyze.py \
    --input ./cache/classified \
    --output ./cache/report_data.json
  ```

  **Acceptance Criteria**:
  - [ ] `report_data.json` contains all 5 aggregation types
  - [ ] Quarter bins cover 2019-Q1 through current quarter
  - [ ] At least 10 reviewers appear in reviewer matrix
  - [ ] Inflection points identified for ≥ 3 categories

---

- [x] 5. Build report generator with trend charts

  **What to do**:
  - Create `scripts/review_analysis/report.py`
  - Reads `report_data.json`
  - Generates markdown report with:
    - **Executive summary**: Top 3 persistent, conquered, emerging issues
    - **Overall volume chart**: Mermaid xychart of total comments per quarter
    - **Per-category trend charts**: One Mermaid chart per Tier-1 category
    - **AI impact chart**: Stacked bar of human vs AI PR issues
    - **Category deep dives**: Description, count, trend, representative examples
    - **Reviewer specialization table**
    - **Recommendations**: Prioritized list for skill design

  **Chart format**: Mermaid (renders natively in GitHub markdown). Example:
  ```
  ```mermaid
  xychart-beta
    title "Test Quality Issues per Quarter"
    x-axis [Q1-19, Q2-19, ..., Q1-26]
    y-axis "Occurrences" 0 --> 50
    bar [3, 5, 4, 7, ...]
  ```

  **Acceptance Criteria**:
  - [ ] Markdown report generated
  - [ ] At least 3 Mermaid trend charts render correctly
  - [ ] Executive summary present with top 3 per trajectory type
  - [ ] Reviewer table has ≥ 5 reviewers with their top categories
  - [ ] Recommendations section actionable

---

- [x] 6. Unit tests for classifier patterns

  **What to do**:
  - Create `unit_tests/test_review_classifier.py`
  - Parametrized tests: for each category, test 3+ known-matching comments and 2+ non-matching
  - Use actual comment text from pilot as test fixtures
  - Test multi-label assignment (comment matching 2 categories)
  - Test bot detection

  **References**:
  - `.sisyphus/drafts/pr-review-taxonomy.md` — Real comment examples per category

  **Acceptance Criteria**:
  - [ ] ≥ 39 test cases (3 per 13 categories)
  - [ ] All tests pass
  - [ ] Covers multi-label and bot detection edge cases

---

- [x] 7. Pipeline wrapper and README

  **What to do**:
  - Create `scripts/review_analysis/run.sh` — Single command to run full pipeline
  - Create `scripts/review_analysis/README.md` documenting:
    - Purpose and process overview
    - Prerequisites (`gh` CLI authenticated)
    - How to run (`./run.sh`)
    - How to re-run quarterly (just re-run, cache handles dedup)
    - How to add new taxonomy categories
    - How to interpret the report
    - Estimated runtime (~4-7 hours for first run, ~30 min for updates)

  **Acceptance Criteria**:
  - [ ] `run.sh` executes all 4 phases sequentially
  - [ ] README covers all sections listed above
  - [ ] Re-running uses cache correctly

---

- [ ] 8. Run full data collection (LONG-RUNNING) — IN PROGRESS in tmux:pr-review-collection

  **What to do**:
  - Execute `collect.py` with `--delay 2.0` for gentle pace
  - Target: all PRs from 2019-01-01 to present with review comments
  - Expected: ~6,500 PRs, ~13,000 API calls, ~7 hours at 2s delay
  - Can be run in background (`nohup` or `tmux`)
  - Monitor progress via log output

  **Must NOT do**:
  - Do NOT use delay < 1.5 seconds
  - Do NOT run during peak working hours if concerned about rate limits

  **Acceptance Criteria**:
  - [ ] ≥ 5,000 PR JSONs cached
  - [ ] No API 403 errors in log
  - [ ] Collection completed without manual intervention

---

- [ ] 9. Run classification + analysis + report generation

  **What to do**:
  - Run `classify.py` on collected data
  - Run `analyze.py` to produce aggregations
  - Run `report.py` to generate final report
  - Review report for accuracy against pilot findings
  - Verify pilot-era data (2024-2026) matches pilot counts within ±20%

  **Acceptance Criteria**:
  - [ ] Report generated with all required sections
  - [ ] Pilot-era category counts within ±20% of pilot findings
  - [ ] At least 2 conquered issues identified
  - [ ] Mermaid charts render correctly

---

- [x] 10. Write skill design recommendations

  **What to do**:
  - Based on the report, write a recommendations document covering:
    1. **Code-review skill** — priority-ordered checklist based on 7-year data
    2. **AI-code-audit skill** — yes/no based on T3 occurrence count (threshold: 50+)
    3. **Plan-review skill** — yes/no based on T8 occurrence count (threshold: 30+)
    4. **New categories discovered** — any T0 clusters that became new categories
    5. **Automated check opportunities** — issues that could be caught by linters/bots
  - Save to `.sisyphus/drafts/skill-design-recommendations.md`

  **Acceptance Criteria**:
  - [ ] Recommendations backed by specific occurrence counts
  - [ ] Priority ordering matches data
  - [ ] Clear yes/no for each proposed skill with threshold justification

---

## Commit Strategy

- **Commit 1**: `feature(review-analysis): add taxonomy definition and collection script`
  - Files: `scripts/review_analysis/taxonomy.yaml`, `scripts/review_analysis/collect.py`
- **Commit 2**: `feature(review-analysis): add classifier and unit tests`
  - Files: `scripts/review_analysis/classify.py`, `unit_tests/test_review_classifier.py`
- **Commit 3**: `feature(review-analysis): add analysis, report generator, and pipeline`
  - Files: `scripts/review_analysis/analyze.py`, `scripts/review_analysis/report.py`, `scripts/review_analysis/run.sh`, `scripts/review_analysis/README.md`
- **Commit 4**: `docs(review-analysis): add generated report and skill recommendations`
  - Files: report output, recommendations

## Success Criteria

### Verification Commands
```bash
# Run classifier unit tests
uv run python -m pytest unit_tests/test_review_classifier.py -v

# Verify cache has data
ls cache/raw/*.json | wc -l  # Expected: ≥ 5000

# Verify report renders
# Open report markdown in GitHub or VS Code preview
```

### Final Checklist
- [ ] ≥ 5,000 PRs collected and cached
- [ ] ≥ 70% of comments classified (not T0)
- [ ] Report has per-quarter trends for all 13 categories
- [ ] At least 3 trend charts render correctly
- [ ] Pipeline re-runnable via `run.sh`
- [ ] Process documented in README.md
- [ ] Skill recommendations backed by data
