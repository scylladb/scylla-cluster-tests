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

## Paragraph Length and Structure

Most paragraphs are 1-3 sentences. The first sentence states the change; subsequent sentences add context.

### Single-commit paragraph (1-2 sentences)

> Due to reaching the AWS Security Groups limit, the [cloud cleanup process will now periodically remove](https://github.com/...) any unused security groups that aren't tagged with `keep:alive`.

### Multi-commit paragraph (2-3 sentences)

> YCSB was [updated to 1.2.0](https://github.com/...) with a native load balancer. Additionally, alternator load balancing is now [enabled by default on every workload](https://github.com/...), with the exception of performance tests.

### Major change paragraph (2-3 sentences with broader context)

> The nemesis system underwent a major refactoring (phase 2). The old `nemesis.py` file [was replaced with a module structure](https://github.com/...), extracting all monkeys into a separate module and introducing a `gatherer.py` module for autoloading. This included [moving nemesis_registry](https://github.com/...) and [nemesis_utils into the nemesis module](https://github.com/...).

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
- Don't mention commits you excluded — just omit them silently
