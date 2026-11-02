# Intent Template

Replace each paragraph under a heading with content. Keep every heading.
Write `None` under a heading that has no content. Leave out the `Jira` line
when the change has no issue.

```markdown
# <Short title>

**Jira:** <KEY>

## Problem

What goes wrong today. State the behavior, not the fix.

## Who it affects

Which test, which pipeline, which backend, or which team.

## Evidence

A Jenkins or Argus link, a log line, a Jira issue, or a metric. Copy the
text verbatim. Replace each password, token, and key with `<redacted>`.

## Definition of done

The requirements that the solution must meet, and the observable result that
tells us the problem is gone. Write each one as a result, not as a design.

## Out of scope

What this change will not do.
```
