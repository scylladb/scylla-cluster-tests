# GitHub Copilot Instructions for Scylla Cluster Tests

This file points Copilot to the sources of the SCT rules. It does not restate
them. Change a rule in its source, not here.

- [AGENTS.md](../AGENTS.md): setup, architecture, commands, and a one-line
  summary of each coding standard.
- [docs/INDEX.md](../docs/INDEX.md): the index of the coding standards in
  [docs/standards/](../docs/standards/).
- [REVIEW.md](../REVIEW.md): the review guide. A code review follows its three
  passes and its volume rule.

When you work on a pull request:

- Run `uv run sct.py pre-commit` before each commit.
- After you change a test in `unit_tests/`, run it:
  `uv run python -m pytest <file> -x -q --no-header --tb=short -n0`.
- In the pull request description, list the tests that you could not run,
  such as a provisioning run on a cloud backend, with the command to run each.
