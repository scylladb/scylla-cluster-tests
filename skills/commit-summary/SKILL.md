---
name: commit-summary
description: >-
  Generate weekly commit summary reports for SCT repository.
  Use when asked to create a commit summary, weekly report, changelog,
  or "last week in SCT" issue. Applies to summarizing git commits
  from scylla-cluster-tests master branch for developer audiences.
  Covers running sct_commits_summary.py, filtering commits by importance,
  and writing prose summaries with embedded GitHub links.
---

# Weekly Commit Summary

Generate curated weekly commit summaries that highlight changes important to SCT developers and testers.

## Essential Principles

### Curate, Don't Dump

**Select commits that matter to other developers, not every change that landed.**

The raw git log contains dozens of commits. Most are routine fixes or dependency bumps that don't need attention. The summary exists to surface changes that affect how people write tests, use tools, or understand the framework. Including everything defeats the purpose — readers stop reading.

### Embed Links in Context

**Links belong inside the most meaningful phrase of each paragraph, not at the start.**

Readers scan by link text. A link on "was updated" tells nothing; a link on "migrated from dict-based to pydantic configuration" tells the whole story. Vary link placement — don't start every paragraph with a link, because it creates a monotonous list feel.

### Group Related, Separate Unrelated

**Combine commits that touch the same area into one paragraph, but never mix unrelated changes.**

Two commits updating scylla-bench belong together. A scylla-bench update and a new nemesis do not. Grouping related work reduces paragraph count and gives readers a coherent picture. Mixing unrelated changes forces readers to context-switch mid-paragraph.

### Follow the Established Voice

**Match the tone and structure of previous issues — concise, factual, third-person.**

The report has a consistent voice across 100+ issues. Breaking that voice is jarring for regular readers. Study the examples before writing. The opening and closing lines are fixed templates.

## When to Use

- When asked to generate a weekly commit summary or "last week in SCT" report
- When asked to create a new commit summary issue
- When asked to curate or filter commits for a summary report
- When asked to write up recent SCT changes for the team
- When incrementing the commit summary issue number

## When NOT to Use

- For regular git log viewing or commit history exploration
- For writing changelogs for releases (different format and audience)
- For reviewing a specific PR or set of changes
- For generating commit messages (that's a different task)

## What to Include vs. Exclude

| Include | Exclude |
|---------|---------|
| New tests or test categories added | Small bug fixes with narrow scope |
| Stress tool or driver updates (ScyllaDB-maintained: scylla-bench, latte, gemini, cassandra-stress, scylla-driver, argus, YCSB) | General package/dependency bumps (renovate, pip updates) |
| Bigger refactorings that change how code is organized | Minor refactors (rename, move, cleanup) |
| New implementation plans | Hydra/container image updates |
| New framework capabilities or backends | Typo fixes, formatting changes |
| Configuration system changes | CI pipeline tweaks (unless significant) |
| Monitoring or reporting improvements | Merge commits (already excluded by script) |
| Nemesis additions or major nemesis changes | Pre-commit hook minor adjustments |
| Performance test additions or changes | |
| Cloud backend additions (OCI, xcloud, etc.) | |

## Quick Reference

### Determine Next Issue Number and Start SHA

Find the latest `commit_summary_issue_*.md` file. The issue number increments by 1. The start SHA is the end commit from the previous report's second line (after the `...` in the commit range).

### Generate Raw Commit List

The script is bundled at `skills/commit-summary/sct_commits_summary.py`:

```bash
python3 skills/commit-summary/sct_commits_summary.py <start_sha> > commit_summary_issue_<N>.md
```

### Output Template

The final report follows this exact structure:

```
This short report brings to light some interesting commits to [scylla-cluster-tests.git master](https://github.com/scylladb/scylla-cluster-tests) from the last week.
Commits in the <start_8chars>...<end_8chars> range are covered.

There were N non-merge commits from M authors in that period. Some notable commits:

<curated paragraphs with embedded links>

See you in the next issue of last week in scylla-cluster-tests.git master!
```

## Reference Index

| File | Content |
|------|---------|
| [writing-style.md](references/writing-style.md) | Detailed style patterns with good/bad examples extracted from past issues |

| Workflow | Purpose |
|----------|---------|
| [generate-summary.md](workflows/generate-summary.md) | 4-phase process from raw git log to polished summary |

## Success Criteria

- [ ] Issue number increments correctly from previous report
- [ ] Start SHA matches end SHA from previous report
- [ ] Commit count and author count are accurate (from raw output)
- [ ] Unimportant commits are removed (small fixes, renovate, hydra)
- [ ] Related commits are grouped into single paragraphs
- [ ] Every paragraph has at least one embedded GitHub commit link
- [ ] Links are on the most descriptive phrase, not generic words
- [ ] No paragraph starts with a bare link (varied placement)
- [ ] Opening and closing lines match the template exactly
- [ ] Tone matches previous issues — concise, factual, third-person
- [ ] Dropped commits listed in a table with SHA, title, and reason for exclusion
