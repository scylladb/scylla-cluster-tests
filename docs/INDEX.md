# Documentation Index

This file indexes the coding standards of SCT. Read it before a change, and
follow the standards that the change touches. When a standard conflicts with
the task, ask the maintainers.

For setup, architecture, and the commands, read [AGENTS.md](../AGENTS.md). The
other topics have their own files in this directory, as in
[sct-configuration.md](sct-configuration.md) and
[contribution-handbook.md](contribution-handbook.md).

## Development flow

### Development flow (`development-flow.md`)
The optional intent, spec, and plan documents in `docs/plans/<feature>/<idea>/`:
when to write them, the order of work, the root cause of a bug in the commit
message, and how the review uses the spec.

### Review guide (`../REVIEW.md`)
The three review passes: bugs and logic, security, and compliance against the
spec and these standards.

## Standards

### Global standards

Located in `docs/standards/global/`.

#### Commits and pull requests (`standards/global/commits-and-pull-requests.md`)
Conventional Commits types and lengths, one logical change per commit, the
root cause of a bug in the commit body and the pull request description,
pre-commit before each commit, the generated nemesis and configuration files
that a change causes and the unrelated ones to leave out, a draft pull request with the template filled, and the backport and
provision test labels.

### Python standards

Located in `docs/standards/python/`.

#### Coding style (`standards/python/coding-style.md`)
ruff format and ruff check with line length 120 and the pylint limits,
imports at the top of the module, three sorted import groups, the `LOGGER`
module logger, lazy logging arguments, Google style docstrings, comments that
tell why, and no silent errors.

#### Class hierarchies (`standards/python/class-hierarchies.md`)
Override safety when a base class signature changes, including the test stubs
and `**kwargs`, and the same change on each backend.

#### Remote commands and HTTP (`standards/python/remote-commands.md`)
curl through `curl_with_retry()` and `RETRY_ALL_ERRORS_PROBE`, `requests`
through a retry session with `# no-retry` exceptions, and timeouts, retries,
and non-interactive flags for remote package installation.

### Testing standards

Located in `docs/standards/testing/`.

#### Unit tests (`standards/testing/unit-tests.md`)
Top-level pytest functions with plain asserts, fixtures and parametrize, the
`unit_tests/unit/` and `unit_tests/integration/` locations with the
`integration` marker, running the touched tests, and a test for new logic and
for each bug fix.

### Configuration standards

Located in `docs/standards/config/`.

#### Configuration options (`standards/config/configuration-options.md`)
A default in `defaults/` for each option, an `SctField` description that
feeds the configuration pages in `docs/`, and the nemesis exclusion for a pinned
setting.

## Updating this documentation

- Add or change a standard with the `updating-standards` skill.
- Discover the standards again, and generate `REVIEW.md`, with the
  `discovering-standards` skill.
- Update this index when you add, remove, or change a file.
