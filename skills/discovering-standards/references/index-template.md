# Index Template

`docs/INDEX.md` indexes the standards. One entry for each file under
`docs/standards/`. The paragraph of an entry names every practice in the
file, not the topic alone. An agent then decides from the index which file
to read.

```markdown
# Documentation Index

This file indexes the coding standards of SCT. Read it before a change, and
follow the standards that the change touches. When a standard conflicts with
the task, ask the maintainers.

For setup, architecture, and the commands, read [AGENTS.md](../AGENTS.md).

## Development flow

### Development flow (`development-flow.md`)
<One paragraph: the optional intent, spec, and plan documents.>

### Review guide (`../REVIEW.md`)
<One paragraph: the three review passes.>

## Standards

### <Domain> standards

Located in `docs/standards/<domain>/`.

#### <Topic> (`standards/<domain>/<topic>.md`)
<One paragraph that names every practice in the file.>

## Updating this documentation

- Add or change a standard with the `updating-standards` skill.
- Discover the standards again, and generate `REVIEW.md`, with the
  `discovering-standards` skill.
- Update this index when you add, remove, or change a file.
```
