# Mini-Plan Template

Mini-plans are lightweight alternatives to full 7-section plans. Use them for small, single-PR changes under ~1K LOC.

## Key Differences from Full Plans

| Aspect | Full Plan | Mini-Plan |
|--------|-----------|-----------|
| Sections | 7 (Problem, Current State, Goals, Phases, Testing, Success, Risks) | 4 (Problem, Approach, Files, Verification) |
| YAML frontmatter | Required | None |
| MASTER.md registration | Required | None |
| progress.json entry | Required | None |
| Location | `docs/plans/<domain>/` | `docs/plans/mini-plans/` |
| Filename | `kebab-case-name.md` | `YYYY-MM-DD-kebab-case-name.md` |
| Lifecycle | Tracked until archived | Disposable after PR merge or 30 days |

## Template

```markdown
# Mini-Plan: <Title>

**Date:** YYYY-MM-DD
**Estimated LOC:** <number>
**Related PR:** #<number> (if applicable)

## Problem
<1-3 sentences: what needs to change and why>

## Approach
<Bulleted list of steps, in order>

## Files to Modify
- `path/to/file.py` -- <what changes>

## Verification
- [ ] <How to verify the change works>
- [ ] `uv run sct.py pre-commit` passes
```

## Example

```markdown
# Mini-Plan: Add Retry Logic to Health Check REST Client

**Date:** 2026-03-17
**Estimated LOC:** 120
**Related PR:** #14052

## Problem
The health check REST client fails immediately on transient HTTP errors (503, 429),
causing false negatives in cluster health reports during rolling restarts.

## Approach
- Add a retry decorator with exponential backoff to `RestClient.request()`
- Configure max retries (3) and retryable status codes (429, 502, 503, 504)
- Add unit tests for retry behavior with mocked responses

## Files to Modify
- `sdcm/rest/rest_client.py` -- Add retry decorator to `request()` method
- `unit_tests/test_rest_client.py` -- Add tests for retry logic

## Verification
- [ ] Unit tests pass: `uv run python -m pytest unit_tests/test_rest_client.py -v`
- [ ] REST client retries on 503 and succeeds on subsequent 200
- [ ] `uv run sct.py pre-commit` passes
```

## Rules

1. **File paths must be code-verified** -- use file-reading tools to confirm paths exist before listing them.
2. **Verification must be concrete** -- every checkbox should be something a person can actually check or run.
3. **No YAML frontmatter** -- mini-plans are intentionally lightweight.
4. **No MASTER.md or progress.json** -- mini-plans are not tracked in the plan registry.
