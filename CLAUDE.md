# CLAUDE.md

Project instructions for Claude Code. This file is loaded at session start.

## Repository

Scylla Cluster Tests (SCT) — test framework for ScyllaDB. See below for full repository overview, architecture, and conventions.

@AGENTS.md

## Skills

Skills are auto-discovered via the `.claude/skills` symlink pointing to `skills/`.
Do NOT @import skill files here — they are loaded on demand when triggered by their
frontmatter `description`. Eagerly importing all skills wastes ~20K tokens per turn.

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

## Rules
- Always use non-interactive flags when available: --yes, -y, --non-interactive, --no-input. Never use commands with --watch, --interactive, or prompts that wait for input.
