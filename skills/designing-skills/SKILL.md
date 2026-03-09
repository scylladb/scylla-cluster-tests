---
name: designing-skills
description: >-
  Guides the design and structuring of AI agent skills for the SCT
  repository with multi-step phases, progressive disclosure, and
  dual-platform compatibility (GitHub Copilot and Claude Code).
  Use when creating new skills, reviewing existing skills, or
  restructuring AI guidance into modular skill directories.
---

# Designing Skills for SCT

Build modular, task-specific AI agent skills that work across both GitHub Copilot and Claude Code.

## Essential Principles

### Description Is the Trigger

**The `description` field controls when Claude Code activates the skill.**

Claude decides whether to load a skill based solely on its frontmatter `description`. The body of SKILL.md — including "When to Use" and "When NOT to Use" sections — is only read AFTER the skill is already active. Put trigger keywords, use cases, and exclusions in the description. A bad description means wrong activations or missed activations regardless of what the body says.

GitHub Copilot discovers skills through explicit references in `AGENTS.md` — the description field still helps Copilot agents understand the skill's purpose.

### Numbered Phases

**Phases must be numbered with entry and exit criteria.**

Unnumbered prose instructions produce unreliable execution order. Every phase needs:
- A number (Phase 1, Phase 2, ...)
- Entry criteria (what must be true before starting)
- Numbered actions (what to do)
- Exit criteria (how to know it's done)

### Progressive Disclosure

**Progressive disclosure is structural, not optional.**

SKILL.md stays under 500 lines. It contains only what the LLM needs for every invocation: principles, routing, quick references, and links. Detailed patterns go in `references/`. Step-by-step processes go in `workflows/`. One level deep — no reference chains.

### Dual-Platform Compatibility

**Skills must work for both GitHub Copilot and Claude Code.**

Skills live in a common `skills/` directory at the repository root. Discovery mechanisms differ by platform:
- **Copilot**: Reads `AGENTS.md` references and `.github/skills/` symlink. Skills are listed in the "Skills" section of root `AGENTS.md`.
- **Claude Code**: Imports skills via `@skills/<name>/SKILL.md` in `CLAUDE.md` and `.claude/skills/` symlink. Frontmatter `description` triggers automatic activation.

Skill content must be platform-agnostic — no Claude-only or Copilot-only instructions in the skill body.

### SCT Conventions First

**Skills must follow SCT repository conventions.**

All skills should be consistent with conventions in `AGENTS.md` and `.github/copilot-instructions.md`:
- Python code examples use pytest (not unittest), Google docstrings, no inline imports
- Test examples go in `unit_tests/`, use `@pytest.fixture` and `@pytest.mark.parametrize`
- Commands reference SCT tools: `uv run sct.py unit-tests`, `uv run sct.py pre-commit`
- Configuration examples reference `sdcm/sct_config.py`, `test-cases/`, `defaults/`

## When to Use

- Creating a new skill directory under `skills/`
- Reviewing or refactoring an existing skill for quality
- Deciding how to split content between SKILL.md, references/, and workflows/
- Structuring a skill that covers a specific SCT task domain (testing, planning, coding, reviewing)
- Adding dual-platform discovery configuration for a new skill

## When NOT to Use

- Writing the actual domain content of a skill (this teaches structure, not domain expertise)
- Simple one-off documentation updates — edit the relevant file directly
- Updating `AGENTS.md` or `.github/copilot-instructions.md` without creating a skill

## Skill Directory Structure

Every skill follows this structure:

```
skills/
└── <skill-name>/             # kebab-case directory name
    ├── SKILL.md              # Main skill definition with YAML frontmatter
    ├── references/           # Deep-dive reference documents
    │   ├── <topic>.md        # Detailed guidance on a specific topic
    │   └── ...
    └── workflows/            # Step-by-step processes
        ├── <process>.md      # Numbered phases with entry/exit criteria
        └── ...
```

### SKILL.md Template

```markdown
---
name: <skill-name>
description: >-
  <Third-person description with trigger keywords.
  Use when <specific scenarios>. Applies to <specific domains>.>
---

# <Skill Title>

<One-line purpose statement.>

## Essential Principles
<3-5 non-negotiable rules, each explaining WHY>

## When to Use
<4-6 specific scenarios>

## When NOT to Use
<3-5 scenarios naming alternatives>

## <Domain-Specific Sections>
<Quick references, routing tables, decision trees>

## Reference Index
<Links to all supporting files in references/ and workflows/>

## Success Criteria
<Checklist for output validation>
```

## Platform Discovery Configuration

When adding a new skill, the skill is automatically discoverable via symlinks:
- `.github/skills` → `skills/` (Copilot native discovery)
- `.claude/skills` → `skills/` (Claude Code native discovery)

Additionally, update both platform reference files:

### Copilot Discovery (AGENTS.md)

Add an entry to the "Skills" section in `AGENTS.md`:

```markdown
## Skills

| Skill | Description | Path |
|-------|-------------|------|
| designing-skills | Meta-skill for creating new AI agent skills | `skills/designing-skills/SKILL.md` |
| <new-skill> | <description> | `skills/<new-skill>/SKILL.md` |
```

### Claude Code Discovery (CLAUDE.md)

Add an import to `CLAUDE.md`:

```markdown
@skills/<new-skill>/SKILL.md
```

## Anti-Pattern Quick Reference

The most common mistakes. Full catalog in [anti-patterns.md](references/anti-patterns.md).

| ID | Anti-Pattern | One-Line Fix |
|----|-------------|-------------|
| AP-1 | Vague description / missing scope | Add trigger keywords to description; add When to Use / When NOT to Use |
| AP-2 | Monolithic SKILL.md (>500 lines) | Split into references/ and workflows/ |
| AP-3 | Reference chains (A → B → C) | All files one hop from SKILL.md |
| AP-4 | Unnumbered phases | Number every phase with entry/exit criteria |
| AP-5 | Missing exit criteria | Define what "done" means for every phase |
| AP-6 | No verification step | Add validation at the end of every workflow |
| AP-7 | Broken file references | Verify every path resolves before submitting |
| AP-8 | Platform-specific content in skill body | Keep skill content platform-agnostic |
| AP-9 | Missing discovery configuration | Update both AGENTS.md and CLAUDE.md |
| AP-10 | No concrete examples | Show input → output for key instructions |
| AP-11 | Description summarizes workflow | Description = triggering conditions only, not workflow steps |
| AP-12 | SCT convention violations in examples | Verify examples use pytest, Google docstrings, `silence`, no inline imports |
| AP-13 | Reference dump instead of guidance | Teach decision criteria, not raw documentation |

## Reference Index

| File | Content |
|------|---------|
| [skill-structure.md](references/skill-structure.md) | SCT-specific skill structure with file organization and frontmatter rules |
| [anti-patterns.md](references/anti-patterns.md) | Common mistakes when creating skills with before/after fixes |

| Workflow | Purpose |
|----------|---------|
| [create-a-skill.md](workflows/create-a-skill.md) | 5-phase process for creating a new skill from scratch |

## Success Criteria

A well-designed SCT skill:

- [ ] Has valid YAML frontmatter with `name` and `description` fields
- [ ] Has When to Use (4+ scenarios) AND When NOT to Use (3+ scenarios) sections
- [ ] Numbers all phases with entry and exit criteria
- [ ] Keeps SKILL.md under 500 lines with details in references/workflows
- [ ] Has no broken file references (all paths resolve)
- [ ] Has no reference chains (all links one hop from SKILL.md)
- [ ] Includes a verification step at the end of workflows
- [ ] Has a description that triggers correctly (third-person, specific keywords)
- [ ] Includes concrete examples for key instructions
- [ ] Is registered in both `AGENTS.md` (Copilot) and `CLAUDE.md` (Claude Code)
- [ ] Contains no platform-specific instructions in the skill body
- [ ] Follows SCT conventions from `AGENTS.md`
