# CLAUDE.md

Project instructions for Claude Code. This file is loaded at session start.

## Rules
- Do NOT @import skill files — they are loaded on demand when triggered by their frontmatter `description`. Eagerly importing all skills wastes ~20K tokens per turn.
- Always use non-interactive flags when available: --yes, -y, --non-interactive, --no-input. Never use commands with --watch, --interactive, or prompts that wait for input.
- Read the target section of a file before editing. Plan the complete edit before applying — do not make speculative changes you may need to revert.
- After 2 consecutive tool failures, stop and reassess your approach before retrying. Diagnose the root cause instead of repeating the same failing command.

## Repository

Scylla Cluster Tests (SCT) — test framework for ScyllaDB. See below for full repository overview, architecture, and conventions.

For development setup, architecture, code style, and testing guidelines see [AGENTS.md](AGENTS.md).

## Skills

Skills are auto-discovered via the `.claude/skills` symlink pointing to `skills/`.

Available skills (invoke via Skill tool or `/skill-name`):
- `designing-skills` — Create and structure AI agent skills
- `fix-backport-conflicts` — Resolve merge conflicts in backport PRs
- `profiling-sct-code` — Profile Python code for performance bottlenecks
- `writing-intents` — Write the intent of a change: the problem, in `docs/plans/<feature>/<idea>/intent.md`
- `writing-specs` — Write the design spec of a change in `docs/plans/<feature>/<idea>/spec.md`
- `writing-plans` — Write implementation plans (full 7-section, mini, or the plan of a spec)
- `writing-unit-tests` — Write pytest unit tests for SCT
- `writing-integration-tests` — Write integration tests with external services
- `commit-summary` — Generate weekly commit summary reports
- `writing-nemesis` — Create new chaos engineering disruptions
- `code-review` — Review PRs for correctness and convention compliance
- `discovering-standards` — Discover the coding standards, write `docs/standards/` and `docs/INDEX.md`, and generate `REVIEW.md`
- `updating-standards` — Add or change one coding standard
- `labeling-pipelines` — Add test_metadata to test-case YAML files
- `reviewing-pipeline-docs` — Review test_metadata sections for accuracy
- `stack-sync` — Manage stacked PRs via the gh-stack CLI extension (native GitHub stacked PR support)
- `downscaling-for-minicloud` — Shrink a test-case to run against the local minicloud emulator

## Development flow

The intent, the spec, and the plan of a change are optional. They go to
`docs/plans/<feature>/<idea>/`. See [docs/development-flow.md](docs/development-flow.md).

- Propose them when a change is big or touches the SCT architecture. Reviewers value them.
- Offer to write them for the user to review before the implementation starts.
- Ask the user before you commit them.
- A bug fix puts its root cause in the commit message and the PR description. It uses the flow only when it changes a contract or a module boundary.

The coding standards are indexed in [docs/INDEX.md](docs/INDEX.md). The review guide is [REVIEW.md](REVIEW.md).
