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

1. **Describe behaviour, not code** -- the Approach section says what should happen and in what order, not how it is coded. No line numbers, no code snippets of internals, no file-internal detail (private attributes, import order, which helper calls which). Reference code by symbol: `file.py:ClassName`. See PAP-1, PAP-9 and PAP-10 in [anti-patterns.md](anti-patterns.md).
2. **State the flow once, end to end** -- if the change has a lifecycle or a multi-step sequence, write it as a single arrow chain in Approach (e.g. `instance created -> rate calculated -> rate reported -> instance terminated -> cost computed -> cost reported -> run total sent`), not spread over the step bullets. See PAP-11.
3. **File paths must be code-verified** -- use file-reading tools to confirm paths exist before listing them. Module-level paths only; "Files to Modify" is the one place file paths belong.
4. **Keep it under ~150 lines** -- a mini-plan is a single-PR change. If it does not fit, either it is a full plan, or it is carrying code detail and separable efforts that should come out (PAP-10).
5. **One concern per plan** -- if part of it could ship as its own effort, split it into its own mini-plan and reference it in one sentence. A cluster of open questions in one area is the signal that it is separable (PAP-12).
6. **Verification must be concrete** -- every checkbox should be something a person can actually check or run.
7. **No YAML frontmatter** -- mini-plans are intentionally lightweight.
8. **No MASTER.md or progress.json** -- mini-plans are not tracked in the plan registry.
