# AI Skills Framework Plan

## Problem Statement

AI coding assistants (GitHub Copilot, Claude Code) working on the SCT repository currently rely on monolithic instruction files (`AGENTS.md`, `.github/copilot-instructions.md`) that bundle all guidance into single documents. This approach has several limitations:

- **No modular skill activation**: All instructions are loaded regardless of the task at hand, wasting context window and diluting focus
- **No structured workflows**: Complex multi-step tasks (writing plans, refactoring tests, creating new features) lack step-by-step guidance
- **Duplicated guidance**: Python conventions, testing practices, and plan-writing rules are scattered across `AGENTS.md`, `.github/copilot-instructions.md`, and `docs/plans/INSTRUCTIONS.md` with inconsistencies
- **No skill reuse across AI platforms**: Guidance is platform-specific rather than structured to work for both Claude Code and GitHub Copilot

The [coodie](https://github.com/fruch/coodie) project demonstrates a skill-based approach using `.github/skills/` directories with structured `SKILL.md` files, `references/`, and `workflows/` subdirectories. This plan adapts that pattern for SCT.

## Current State

### Existing AI Guidance Files

1. **`AGENTS.md`** (root) — Comprehensive guide for LLM agents covering:
   - Repository overview and architecture
   - Development environment setup (`uv sync`, `hydra bash`)
   - Common commands (`uv run sct.py unit-tests`, `uv run sct.py pre-commit`)
   - High-level architecture of `sdcm/` directory
   - Testing best practices (pytest style, fixtures, parametrize)
   - Code style guidelines (no inline imports, import grouping)
   - Documentation standards (Google docstring format)
   - Environment variables reference

2. **`.github/copilot-instructions.md`** — GitHub Copilot-specific instructions covering:
   - Commit message format (Conventional Commits with SCT-specific types)
   - Pre-commit requirements (`uv run sct.py pre-commit`)
   - Test format and location (`unit_tests/`, pytest markers)
   - Backend-specific provision test labels
   - Manual testing documentation requirements
   - Commit history and squashing guidance

3. **`docs/plans/INSTRUCTIONS.md`** — Plan writing guidelines covering:
   - 7-section plan structure (Problem Statement, Current State, Goals, etc.)
   - Content and plan guidelines
   - Agent rules for plan generation

### Existing Plan Examples

- `docs/plans/health-check-optimization.md` — Performance optimization plan
- `docs/plans/nemesis-rework.md` — Feature refactoring plan
- `docs/plans/docker-cleanup-all-backends.md` — Infrastructure cleanup plan
- `docs/plans/full-version-tag-lookup.md` — Feature implementation plan
- `docs/plans/jenkins-uno-choice-billing-project.md` — CI/CD plan

### What's Missing

- No `skills/` directory exists for modular, task-specific guidance
- No structured workflows for common agent tasks
- No reference documents for deep-dive guidance
- Guidance is not organized by task type (testing, planning, coding)
- No code review skill to guide AI agents during PR reviews

### Platform-Specific Discovery Mechanisms

Each AI platform has its own native way to discover instruction files. Understanding these is key to dual-platform support.

**GitHub Copilot** (per [GitHub docs](https://docs.github.com/en/copilot/customizing-copilot/adding-repository-custom-instructions-for-github-copilot)):
- **`AGENTS.md`** — Agent instructions. Copilot reads the nearest `AGENTS.md` in the directory tree. You can place multiple `AGENTS.md` files at different levels, and the nearest one takes precedence.
- **`.github/copilot-instructions.md`** — Repository-wide custom instructions.
- **`.github/instructions/**/*.instructions.md`** — Path-specific custom instructions that apply to files matching a specified path.
- Copilot Coding Agent, Chat, and Code Review all support `AGENTS.md` files.

**Claude Code** (per [Anthropic docs](https://docs.anthropic.com/en/docs/claude-code/memory)):
- **`CLAUDE.md`** or **`.claude/CLAUDE.md`** — Project memory, loaded at session start.
- **`.claude/rules/*.md`** — Modular rules files, automatically loaded. Supports path-specific scoping via YAML frontmatter `paths:` field.
- **`@path/to/file`** imports in CLAUDE.md — Allows referencing any file in the repo.
- Claude discovers `CLAUDE.md` files recursively up the directory tree from the working directory.

**Implication for skills**: Skills should live in a common `skills/` directory. To make them discoverable:
- **Copilot**: Reference skills from root `AGENTS.md` and/or `.github/copilot-instructions.md`.
- **Claude Code**: Reference skills from `CLAUDE.md` using `@skills/<name>/SKILL.md` imports, or symlink skill files into `.claude/rules/`.

### Reference: coodie Skill Structure

The coodie project (`github.com/fruch/coodie`) provides a proven skill structure:

```
skills/                            # Common location (platform-agnostic)
├── designing-workflow-skills/     # Meta-skill for creating skills
│   ├── SKILL.md                   # Main skill definition with frontmatter
│   ├── references/                # Deep-dive reference docs
│   │   ├── anti-patterns.md
│   │   ├── workflow-patterns.md
│   │   ├── tool-assignment-guide.md
│   │   └── progressive-disclosure-guide.md
│   └── workflows/                 # Step-by-step processes
│       ├── design-a-workflow-skill.md
│       └── review-checklist.md
├── writing-plans/                 # Skill for creating project plans
│   ├── SKILL.md
│   ├── references/
│   │   └── plan-templates.md
│   └── workflows/
│       └── create-a-plan.md
├── modern-python/                 # Python tooling and conventions
│   ├── SKILL.md
│   ├── references/
│   │   ├── pyproject.md
│   │   ├── ruff-config.md
│   │   └── ...
│   └── templates/
│       └── pre-commit-config.yaml
└── test-refactoring/              # Test improvement patterns
    ├── SKILL.md
    ├── references/
    │   ├── parametrize-patterns.md
    │   └── sync-async-dedup.md
    └── workflows/
        └── refactor-test-file.md
```

Key design patterns from coodie:
- **SKILL.md frontmatter**: YAML metadata with `name`, `description`, and `allowed-tools` (Claude Code native)
- **`description` as trigger**: The `description` field controls when Claude Code activates the skill
- **Progressive disclosure**: SKILL.md stays under 500 lines; details go in `references/`
- **Numbered phases**: All workflows use numbered phases with entry/exit criteria
- **Success criteria checklist**: Every skill ends with a validation checklist

## Goals

1. **Create a modular skill framework** in a common `skills/` directory at the repository root, discoverable by all AI platforms through their native mechanisms (Copilot via `AGENTS.md` references, Claude Code via `CLAUDE.md` imports).
2. **Deliver 6 initial skills** covering the requested areas:
   - Designing and creating new skills (meta-skill)
   - Writing implementation plans
   - Python programming guidelines for SCT
   - Unit testing practices
   - Integration testing practices
   - Code review guidance
3. **Ensure dual-platform compatibility** — skills must work for both Claude Code and GitHub Copilot:
   - Claude Code: Discovers skills via `CLAUDE.md` imports (`@skills/<name>/SKILL.md`) and/or `.claude/rules/` symlinks; SKILL.md frontmatter enables automatic activation
   - GitHub Copilot: Discovers skills via references in root `AGENTS.md` and `.github/copilot-instructions.md`; Copilot reads `AGENTS.md` files anywhere in the directory tree
4. **Extract and deduplicate guidance** from existing `AGENTS.md` and `.github/copilot-instructions.md` into skills, keeping the root files as entry points that reference skills
5. **Follow coodie patterns** for skill structure while adapting content to SCT-specific conventions

## Implementation Phases

### Phase 1: Create Skill Framework Foundation (Priority: High)

**Description**: Set up the `skills/` directory at the repository root, configure platform-specific discovery mechanisms, and create the meta-skill for designing new skills.

**Deliverables**:
- `skills/` directory at repository root (common, platform-agnostic location)
- Platform discovery configuration:
  - Update root `AGENTS.md` with a "Skills" section referencing `skills/` directory contents (Copilot discovery)
  - Create/update `CLAUDE.md` with `@skills/<name>/SKILL.md` imports (Claude Code discovery)
  - Optionally symlink skill SKILL.md files into `.claude/rules/` for automatic Claude Code loading
- `skills/designing-skills/SKILL.md` — Meta-skill adapted from coodie's `designing-workflow-skills`
- `skills/designing-skills/references/skill-structure.md` — SCT-specific skill structure reference
- `skills/designing-skills/references/anti-patterns.md` — Common mistakes when creating skills
- `skills/designing-skills/workflows/create-a-skill.md` — Step-by-step process for creating a new skill

**Adaptation Notes**:
- Replace coodie-specific tool references (TodoRead, TodoWrite) with SCT-equivalent tools
- Add SCT-specific context: Copilot Coding Agent tools, Claude Code tools, and common tool overlap
- Include dual-platform compatibility checklist in the skill creation workflow
- Reference SCT conventions from `AGENTS.md` for code-generation skills

**Definition of Done**:
- [ ] `skills/` directory exists at repository root with all files
- [ ] Root `AGENTS.md` references skills (Copilot discovery)
- [ ] `CLAUDE.md` imports skills (Claude Code discovery)
- [ ] SKILL.md has valid frontmatter with `name`, `description`
- [ ] Skill references SCT-specific tool patterns
- [ ] Includes dual-platform (Claude + Copilot) compatibility guidance

---

### Phase 2: Create Writing Plans Skill (Priority: High)

**Description**: Create a skill for writing implementation plans, adapting coodie's writing-plans skill and integrating with the existing `docs/plans/INSTRUCTIONS.md`.

**Deliverables**:
- `skills/writing-plans/SKILL.md` — Plan writing skill adapted for SCT's 7-section structure
- `skills/writing-plans/references/plan-templates.md` — SCT-specific plan templates
- `skills/writing-plans/workflows/create-a-plan.md` — Step-by-step plan creation process

**Adaptation Notes**:
- Use SCT's existing 7-section plan structure from `docs/plans/INSTRUCTIONS.md` (Problem Statement, Current State, Goals, Implementation Phases, Testing Requirements, Success Criteria, Risk Mitigation)
- Reference existing plan examples (`health-check-optimization.md`, `nemesis-rework.md`)
- Include SCT-specific conventions: backend-specific details, Definition of Done per phase, PR-scoped phases
- Merge guidance from `docs/plans/INSTRUCTIONS.md` "Rule for Agents" section into the skill
- Keep `docs/plans/INSTRUCTIONS.md` as the authoritative source; skill references it

**Definition of Done**:
- [ ] `skills/writing-plans/` directory exists with all files
- [ ] SKILL.md references `docs/plans/INSTRUCTIONS.md` as authoritative source
- [ ] Plan templates match SCT's 7-section format
- [ ] Workflow references existing example plans in `docs/plans/`

---

### Phase 3: Create Python Guidelines Skill (Priority: High)

**Description**: Create a skill for Python programming conventions specific to SCT, extracting and organizing guidance currently scattered across `AGENTS.md` and `.github/copilot-instructions.md`.

**Deliverables**:
- `skills/python-guidelines/SKILL.md` — Python conventions for SCT development
- `skills/python-guidelines/references/import-conventions.md` — Import ordering and anti-patterns
- `skills/python-guidelines/references/error-handling.md` — Error handling patterns (`silence` context manager, event system)
- `skills/python-guidelines/references/code-style.md` — Style guide (Google docstrings, no inline imports, etc.)

**Adaptation Notes**:
- Extract Python-specific guidance from `AGENTS.md` sections: "Code Style Guidelines", "Documentation Standards"
- Include SCT-specific patterns: `silence` context manager for error handling, `sct_events` system, `LOGGER` usage
- Reference SCT tools: `ruff`, `autopep8`, `pyright` configuration from `pyproject.toml`
- Include pre-commit workflow: `uv run sct.py pre-commit`
- Reference SCT's Python version and typing configuration

**Definition of Done**:
- [ ] `skills/python-guidelines/` directory exists with all files
- [ ] Covers import conventions (3 groups, no inline imports)
- [ ] Covers error handling (`silence` context manager)
- [ ] Covers code style (Google docstrings, typing)
- [ ] References SCT linting tools (`ruff`, `autopep8`, `pyright`)

---

### Phase 4: Create Unit Testing Skill (Priority: High)

**Description**: Create a skill for writing unit tests in SCT, covering pytest conventions, fixture patterns, parametrization, and SCT-specific test infrastructure.

**Deliverables**:
- `skills/unit-testing/SKILL.md` — Unit testing skill for SCT
- `skills/unit-testing/references/pytest-patterns.md` — Pytest patterns used in SCT (fixtures, parametrize, markers)
- `skills/unit-testing/references/test-examples.md` — Concrete before/after examples from `unit_tests/`
- `skills/unit-testing/workflows/write-unit-test.md` — Step-by-step process for writing a unit test

**Adaptation Notes**:
- Extract testing guidance from `AGENTS.md` "Unit Testing Guidelines" and "Testing Best Practices" sections
- Include SCT-specific patterns: pytest (NOT unittest.TestCase), `@pytest.fixture`, `@pytest.mark.parametrize`
- Reference test location conventions: all tests in `unit_tests/`, naming convention `test_*.py`
- Include test runner command: `uv run sct.py unit-tests -t <test_file>`
- Cover SCT-specific markers: `@pytest.mark.integration`, `@pytest.mark.provisioning`, `@pytest.mark.need_network`
- Include mock patterns commonly used in SCT (mocking cluster nodes, remote commands, etc.)

**Definition of Done**:
- [ ] `skills/unit-testing/` directory exists with all files
- [ ] Covers pytest conventions (no unittest.TestCase)
- [ ] Includes fixture and parametrize patterns
- [ ] References SCT test runner commands
- [ ] Includes concrete examples from existing `unit_tests/`

---

### Phase 5: Create Integration Testing Skill (Priority: Medium)

**Description**: Create a skill for writing and running integration tests in SCT, covering backend-specific testing, provision test labels, and manual testing documentation.

**Deliverables**:
- `skills/integration-testing/SKILL.md` — Integration testing skill for SCT
- `skills/integration-testing/references/backend-testing.md` — Backend-specific testing guide (AWS, GCE, Azure, Docker, K8S)
- `skills/integration-testing/references/manual-testing-template.md` — Template for PR manual testing sections

**Adaptation Notes**:
- Extract from `.github/copilot-instructions.md`: "Backend-Specific Provision Tests" and "Manual Testing Notes in PRs"
- Include SCT integration test runner: `uv run sct.py integration-tests`
- Cover provision test labels: `provision-aws`, `provision-gce`, `provision-azure`, `provision-docker`, `provision-k8s`, `provision-baremetal`
- Include backend files mapping (which files trigger which provision labels)
- Include Docker-based local testing: `--backend docker` workflow
- Cover cluster reuse pattern: `SCT_REUSE_CLUSTER` for faster iteration
- Include manual testing template from `.github/copilot-instructions.md`

**Definition of Done**:
- [ ] `skills/integration-testing/` directory exists with all files
- [ ] Covers backend-specific testing with provision labels
- [ ] Includes manual testing documentation template
- [ ] References Docker-based local testing workflow
- [ ] Covers cluster reuse pattern

---

### Phase 6: Create Code Review Skill (Priority: Medium)

**Description**: Investigate and implement a skill for AI-assisted code review on SCT pull requests. This skill should guide agents on how to review code changes, what to look for, and how to provide actionable feedback following SCT conventions.

**Investigation Areas**:
- Study existing code review patterns in the SCT repository (PR review comments, common feedback themes)
- Evaluate what makes a good AI code review for a test framework (correctness, style, test coverage, backend impact)
- Research how Claude Code and Copilot handle code review workflows (PR diff analysis, inline comments, suggestions)
- Determine which SCT-specific checks should be automated (import conventions, error handling patterns, test presence)

**Deliverables**:
- `skills/code-review/SKILL.md` — Code review skill for SCT pull requests
- `skills/code-review/references/review-checklist.md` — Checklist of what to verify in SCT code reviews (style, imports, error handling, test coverage, backend impact, pre-commit compliance)
- `skills/code-review/references/common-issues.md` — Catalog of frequently caught issues with before/after examples
- `skills/code-review/workflows/review-a-pr.md` — Step-by-step process for reviewing a PR

**Adaptation Notes**:
- Include SCT-specific review criteria: no inline imports, `silence` over bare try/except, pytest not unittest, correct provision labels
- Cover backend impact analysis: which files trigger which provision test labels
- Include guidance on reviewing nemesis operations, cluster configurations, and test case YAML files
- Reference commit message format validation (Conventional Commits with SCT types/scopes)
- Cover pre-commit compliance check: `uv run sct.py pre-commit`
- Include guidance on identifying missing test coverage for code changes
- Consider integration with existing PR template (`.github/pull_request_template.md`)

**Definition of Done**:
- [ ] `skills/code-review/` directory exists with all files
- [ ] Covers SCT-specific review criteria (imports, error handling, test patterns)
- [ ] Includes backend impact analysis guidance
- [ ] Includes review checklist with common issues catalog
- [ ] Workflow covers end-to-end PR review process

---

### Phase 7: Update Root Guidance Files (Priority: Medium)

**Description**: Update `AGENTS.md` and `.github/copilot-instructions.md` to reference the new skills, reducing duplication while keeping these files as concise entry points.

**Deliverables**:
- Updated `AGENTS.md` with skill references
- Updated `.github/copilot-instructions.md` with skill references

**Adaptation Notes**:
- Add a "Skills Reference" section to `AGENTS.md` listing all skills with descriptions (enables Copilot discovery)
- Create or update `CLAUDE.md` with `@skills/<name>/SKILL.md` imports (enables Claude Code discovery)
- Keep essential quick-reference content in root files (commit format, basic commands)
- Move detailed guidance to skills, replacing duplicated sections with references
- Ensure backward compatibility — agents that don't understand skills still get basic guidance from root files

**Definition of Done**:
- [ ] `AGENTS.md` references `skills/` directory with skill listing
- [ ] `CLAUDE.md` imports skill files for Claude Code discovery
- [ ] `.github/copilot-instructions.md` references `skills/` directory
- [ ] No essential guidance is lost during the transition
- [ ] Root files remain functional for agents that don't support skills

## Testing Requirements

### Skill Structure Validation

Since skills are documentation/instruction files (Markdown), testing focuses on structural validation:

1. **File structure tests**: Verify all skill directories contain required files (`SKILL.md`, `references/`, `workflows/`)
2. **Frontmatter validation**: Verify SKILL.md files have valid YAML frontmatter with `name` and `description` fields
3. **Link validation**: Verify all internal file references in SKILL.md resolve to existing files
4. **Line count check**: Verify SKILL.md files stay under 500 lines (progressive disclosure principle)

### Functional Validation

1. **Claude Code activation test**: Manually verify skills activate correctly based on `description` field keywords
2. **Copilot reference test**: Manually verify Copilot agents can find and follow skill guidance when directed
3. **Plan generation test**: Ask an AI agent to "generate an implementation plan" and verify it follows the writing-plans skill
4. **Unit test generation test**: Ask an AI agent to "write a unit test for X" and verify it follows the unit-testing skill
5. **Integration test guidance test**: Ask an AI agent about backend testing and verify it provides correct provision label guidance
6. **Code review test**: Ask an AI agent to review a PR and verify it follows the code-review skill checklist

### Manual Testing Procedures

- Run `uv run sct.py pre-commit` to verify no formatting issues in new Markdown files
- Review each skill against the success criteria checklist in the designing-skills meta-skill
- Verify dual-platform compatibility by testing with both Claude Code and GitHub Copilot

## Success Criteria

1. **6 skills created** in `skills/` following consistent structure:
   - `designing-skills/` — Meta-skill for creating new skills
   - `writing-plans/` — Skill for implementation plans
   - `python-guidelines/` — Python coding conventions
   - `unit-testing/` — Unit testing practices
   - `integration-testing/` — Integration testing practices
   - `code-review/` — Code review guidance for PRs

2. **Each skill has**:
   - `SKILL.md` with valid frontmatter (`name`, `description`)
   - At least one file in `references/` with detailed guidance
   - At least one file in `workflows/` with step-by-step process (where applicable)
   - "When to Use" and "When NOT to Use" sections
   - Success criteria checklist

3. **Dual-platform compatibility**:
   - Skills live in `skills/` at the repository root (common, platform-agnostic location)
   - Copilot discovers skills via `AGENTS.md` references (natively supported)
   - Claude Code discovers skills via `CLAUDE.md` imports and/or `.claude/rules/` symlinks
   - Skill content is platform-agnostic (no Claude-only or Copilot-only instructions in skill body)

4. **Root files updated**:
   - `AGENTS.md` lists skills with descriptions for Copilot discovery
   - `CLAUDE.md` imports skill files for Claude Code discovery
   - `.github/copilot-instructions.md` references `skills/` directory
   - No essential guidance lost

## Risk Mitigation

### Risk: Skills Not Activated by AI Agents

**Likelihood**: Medium
**Impact**: Skills exist but are never used by agents
**Mitigation**:
- Write clear, keyword-rich `description` fields in SKILL.md frontmatter
- Reference skills from root guidance files (`AGENTS.md`, `.github/copilot-instructions.md`)
- Test activation with both Claude Code and GitHub Copilot before finalizing

### Risk: Content Drift Between Skills and Root Files

**Likelihood**: Medium
**Impact**: Contradictory guidance between skills and `AGENTS.md`/`.github/copilot-instructions.md`
**Mitigation**:
- Skills are authoritative for their domain; root files reference but don't duplicate
- Include a cross-reference check in the pre-commit or PR review process
- Add a "Skills Reference" section to root files that explicitly delegates to skills

### Risk: Too Many Files Overwhelm Context Windows

**Likelihood**: Low
**Impact**: Agent loads too many skill files and runs out of context
**Mitigation**:
- Follow the 500-line limit for SKILL.md (progressive disclosure)
- Use `references/` and `workflows/` for detail — agents load these only when needed
- Keep skill descriptions specific to avoid spurious activations

### Risk: Auto-generated Files from Pre-commit Hooks

**Likelihood**: Medium
**Impact**: Pre-commit hooks may generate nemesis configurations or Jenkins pipelines when Markdown files change
**Mitigation**:
- Run `uv run sct.py pre-commit` before committing to catch any auto-generated files
- If auto-generated files appear, remove them with `git rm` before pushing
- Skill files are pure Markdown and should not trigger code-generation hooks
