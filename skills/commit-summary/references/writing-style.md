# Commit Summary Writing Style Guide

Patterns extracted from 10+ published issues. Use these as the reference for tone, structure, and link placement.

## Fixed Template Lines

These lines are identical in every issue — do not modify them:

**Opening:**
```
This short report brings to light some interesting commits to [scylla-cluster-tests.git master](https://github.com/scylladb/scylla-cluster-tests) from the last week.
Commits in the <start>…<end> range are covered.

There were N non-merge commits from M authors in that period. Some notable commits:
```

**Closing:**
```
See you in the next issue of last week in scylla-cluster-tests.git master!
```

Note: The ellipsis in the commit range is the Unicode character `…` (U+2026), not three dots.

## Link Placement Patterns

### Good: Link on the descriptive action

> The SCT configuration system was [migrated from a custom dict-based implementation to pydantic](https://github.com/...), bringing type safety and automatic validation.

The link text "migrated from a custom dict-based implementation to pydantic" tells the reader exactly what happened.

### Good: Link mid-sentence on the key noun

> Repair mechanisms were [unified into a single approach](https://github.com/...). Now all repairs go through `run_repair`.

### Good: Link on a tool name with version

> [`scylla-bench` was updated](https://github.com/...) from version 0.2.4 to 0.2.5, fixing an issue where `-help` was printed to stderr.

### Bad: Link on generic words

> The configuration was [updated](https://github.com/...) to use pydantic.

"updated" tells the reader nothing. Link the specific change instead.

### Bad: Every paragraph starts with a link

> [Added new test](https://github.com/...) for size-based load balancing.
> [Updated scylla-bench](https://github.com/...) to v1.
> [Removed Elasticsearch code](https://github.com/...) from the framework.

This creates a monotonous list. Vary placement.

## Length Discipline

A paragraph is 1-2 sentences. The first states the change. A second is earned only when it names another thing the reader can now use — never when it explains why the change was needed.

### Bad: the investigation leaked in

> Monitoring [moved to the 4.16.x branch](https://github.com/...). The overview dashboard template needed [a new merge point for the SCT rows](https://github.com/...), and the 4.16 image ships an epoch-versioned `scylla-node-exporter` that apt refuses to downgrade, so runs with `use_mgmt` set failed in `SetUp()` — the [exporter is now removed before the manager backend install](https://github.com/...) and pulled back in as a dependency of the matching Scylla package.

Two of the three links are bug fixes that followed the version move. The apt epoch, `use_mgmt` and `SetUp()` are debugging detail from the commit body.

### Good: the change alone

> Monitoring [moved to the 4.16.x branch](https://github.com/...).

### Bad: cause and effect narrated

> Every xcloud DB node used to land in a single rack, which left `RackawareValidator`, rack-aware loader pinning and rack-targeting nemeses unused on that backend. Rack indexes are now [derived from the availability zone the Cloud API reports per node](https://github.com/...), with zones sorted alphabetically so the indexes line up with AWS-side loaders.

### Good: the capability alone

> xcloud DB nodes now [get rack indexes derived from their availability zone](https://github.com/...), so rack-aware validation, loader pinning and rack-targeting nemeses work on that backend.

## Describe the End State, Not the First Commit

A range often contains a feature and its walk-back. Read the **last** commit of a group before writing the sentence.

- A commit that sets a version and a later commit that reverts it are one non-event. Omit both.
- A feature added and then dropped after review inside the same range never happened. Omit it.
- A pin applied broadly and then narrowed after review is a narrow pin, not a broad one.

Review-churn commits ("per review feedback", "drop redundant test per review", comment-only follow-ups) are never link candidates.

## Paragraph Structure

The first sentence states the change; a second sentence, if any, adds a second concrete thing.

### Single-commit paragraph (1-2 sentences)

> Due to reaching the AWS Security Groups limit, the [cloud cleanup process will now periodically remove](https://github.com/...) any unused security groups that aren't tagged with `keep:alive`.

### Multi-commit paragraph (2 sentences)

> YCSB was [updated to 1.2.0](https://github.com/...) with a native load balancer. Alternator load balancing is now [enabled by default on every workload](https://github.com/...), except performance tests.

### Large multi-commit effort (still 2 sentences)

> The constraint-based sizing rollout reached most of the test-case tree. Hardcoded `instance_type_*` parameters were replaced with `sizing_db`/`sizing_loader`/`sizing_monitor` constraint blocks across the [longevity configs](https://github.com/...) and the [alternator cases](https://github.com/...).

Eleven commits, two links, no mention of the review rounds that shaped them.

## Tone

- **Third-person, factual.** No "we're excited" or "this is awesome."
- **Present tense for the change.** "Health check will now skip verification" not "Health check was changed to skip."
- **Concise.** Don't explain implementation details unless they help the reader understand the impact.
- **Use "we" sparingly** — only when describing team decisions ("we've refactored..."), not for describing code changes.

## Ordering

1. Most impactful or broadly relevant change first
2. Related changes near each other (e.g., all nemesis changes together)
3. Smaller but noteworthy changes toward the end
4. Tool version bumps typically go mid-to-end unless they're a headline change

## What NOT to Do

- Don't include commit SHAs in the prose text (they're in the links)
- Don't list commit dates or authors in the prose
- Don't use bullet points — the format is flowing paragraphs
- Don't use headers or sections within the body — it's a flat list of paragraphs
- Don't add commentary about commit quality or code style
- Don't mention commits you excluded — the report file omits them silently, and the tables of unlinked and excluded commits go in the chat response instead
- Don't let a long commit body pull you into explaining the fix — the link is the explanation
