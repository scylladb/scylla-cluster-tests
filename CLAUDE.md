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
- `writing-plans` — Write implementation plans (full 7-section or mini)
- `writing-unit-tests` — Write pytest unit tests for SCT
- `writing-integration-tests` — Write integration tests with external services
- `commit-summary` — Generate weekly commit summary reports
- `writing-nemesis` — Create new chaos engineering disruptions
- `code-review` — Review PRs for correctness and convention compliance
