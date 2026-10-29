# Idea Plan Template

An idea plan builds the spec in the same directory,
`docs/plans/<feature>/<idea>/spec.md`. It names the files, the internals, the
order of work, and the tests. Somebody who never saw the change must be able
to follow it. A small change keeps a short plan.

Name files by path and symbol, never by line number. A plan that names line
numbers goes stale at the first refactor.

```markdown
# <Short title> — plan

**Spec:** `docs/plans/<feature>/<idea>/spec.md`

A decision during the build that changes what the spec states updates the
spec in the same commit. A line in its `Decisions` section records the
decision when the Design section does not state the reason.

## Rules

The rules that every task below follows: the standards in `docs/INDEX.md`
that apply, the backends to test, and the provision labels to request.

## Task 1 — <name>

**Files:**
- Create: `<path>`
- Modify: `<path>` (`<ClassName.method>`)
- Test: `unit_tests/unit/<path>`

**Internals:** the functions, the classes, and the fields that this task adds.
Leave out the line when the task adds none.

- [ ] Write the failing test.
- [ ] Run it with `uv run sct.py unit-tests -t unit/<path>` and confirm that the assertion fails.
- [ ] Write the smallest change that makes it pass.
- [ ] Run `uv run sct.py pre-commit`.
- [ ] Commit, with the boxes of this task checked.
```
