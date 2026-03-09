# SCT Skill Structure Reference

Detailed guide to the file organization, frontmatter format, and content rules for SCT skills.

---

## Directory Layout

```
skills/
└── <skill-name>/                 # kebab-case, descriptive name
    ├── SKILL.md                  # Main skill definition (REQUIRED)
    ├── references/               # Deep-dive reference documents (OPTIONAL)
    │   ├── <topic-a>.md
    │   └── <topic-b>.md
    └── workflows/                # Step-by-step processes (OPTIONAL)
        ├── <process-a>.md
        └── <process-b>.md
```

### Naming Conventions

- **Skill directory**: kebab-case, max 64 characters (e.g., `unit-testing`, `python-guidelines`)
- **Files**: kebab-case `.md` files (e.g., `skill-structure.md`, `create-a-skill.md`)
- **SKILL.md**: Always uppercase — this is the entry point for the skill

### Required vs Optional

| Component | Required | Purpose |
|-----------|----------|---------|
| `SKILL.md` | Yes | Main skill definition with frontmatter |
| `references/` | No | Detailed guidance that SKILL.md summarizes |
| `workflows/` | No | Step-by-step processes with numbered phases |

Simple skills (pure guidance, no workflows) may have only SKILL.md. Complex skills should split content into references/ and workflows/.

---

## SKILL.md Frontmatter

Every SKILL.md starts with YAML frontmatter between `---` markers:

```yaml
---
name: skill-name
description: >-
  Third-person description with trigger keywords.
  Use when <specific scenarios>. Applies to <specific domains>.
---
```

### Frontmatter Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | kebab-case, matches directory name, max 64 characters |
| `description` | Yes | Max 1024 characters, no angle brackets, third-person voice |

### Validation Rules

These constraints ensure skills work reliably across platforms:

| Rule | Constraint | Reason |
|------|-----------|--------|
| Name format | kebab-case only | Consistent directory naming across platforms |
| Name length | Max 64 characters | Filesystem and tool compatibility |
| Description length | Max 1024 characters | Context window efficiency; forces conciseness |
| No angle brackets | `<` and `>` forbidden in description | Breaks YAML parsing in some tool chains |
| Valid YAML | Frontmatter must parse as valid YAML | Both platforms read frontmatter programmatically |

### Writing Effective Descriptions

The `description` field is the most important part of the skill for Claude Code — it determines when the skill activates. Think of it as a search index: it must contain the right keywords for Claude to match against user requests.

**Rules:**
1. Use third-person voice: "Guides the creation of..." not "I help with..."
2. Include trigger keywords that match user requests — add synonyms users might use
3. List specific scenarios: "Use when creating unit tests, refactoring test files, or adding pytest fixtures"
4. Stay under 1024 characters (hard limit) — aim for 200-400 characters
5. Do NOT include workflow steps — only triggering conditions
6. Include exclusions where the boundary is ambiguous: "Use when X. Not for Y."

**Good example:**
```yaml
description: >-
  Guides writing unit tests for the SCT framework using pytest
  conventions. Use when creating new test files, adding test cases,
  refactoring tests from unittest to pytest, or reviewing test
  coverage for SCT components. Not for integration tests or
  performance benchmarks.
```

**Bad example:**
```yaml
description: >-
  First reads the file, then creates a test class, then adds
  fixtures and assertions. Outputs a complete test file.
```

**Why this matters:** Claude reads every skill's description at session start and decides which to activate. A description that summarizes the workflow (bad example) causes Claude to follow the description as instructions and skip the actual SKILL.md body. A description that lists triggering conditions (good example) lets Claude correctly match user intent.

### Optimizing Descriptions Iteratively

If a skill isn't triggering correctly:

1. **Write trigger eval queries** — 5-10 prompts that should trigger, 3-5 that shouldn't
2. **Test the description in isolation** — read only the description, check each query
3. **Fix false negatives** — add missing keywords, synonyms, or scenario phrases
4. **Fix false positives** — narrow scope with exclusions or more specific terms
5. **Re-test** — repeat until all queries pass

See [test-and-iterate.md](../workflows/test-and-iterate.md) for the full process.

---

## Line Count Limits

Progressive disclosure keeps context windows focused:

| File Type | Max Lines | Rationale |
|-----------|-----------|-----------|
| SKILL.md | 500 | Contains only always-needed content |
| Reference files | 400 | Detailed but focused on one topic |
| Workflow files | 300 | Step-by-step processes stay concise |

If a file exceeds its limit, split it into multiple files. SKILL.md should summarize and link; details go in references/ and workflows/.

---

## Content Organization Rules

### What Goes in SKILL.md

- Essential principles (3-5 non-negotiable rules)
- When to Use / When NOT to Use sections
- Quick reference tables (summarize detailed content)
- Reference index (links to all supporting files)
- Success criteria checklist

### What Goes in references/

- Detailed pattern catalogs (e.g., anti-patterns with before/after examples)
- Deep-dive technical guides (e.g., import conventions, error handling)
- Templates and format specifications
- Extended examples that would bloat SKILL.md

### What Goes in workflows/

- Multi-step processes with numbered phases
- Each phase has entry criteria, numbered actions, and exit criteria
- Review checklists
- Creation guides (e.g., "create-a-skill.md", "write-a-plan.md")

### The One-Hop Rule

All files are one hop from SKILL.md:

```
SKILL.md → references/topic.md      ✅ (one hop)
SKILL.md → workflows/process.md     ✅ (one hop)
references/a.md → references/b.md   ❌ (reference chain)
```

Reference files do not link to other reference files. If two reference topics are related, SKILL.md should link to both independently.

---

## Platform Discovery

Skills need to be registered for each AI platform. Additionally, symlinks at `.github/skills` and `.claude/skills` both point to `skills/` for native platform discovery.

### GitHub Copilot

Add the skill to the "Skills" table in the root `AGENTS.md`:

```markdown
| <skill-name> | <description> | `skills/<skill-name>/SKILL.md` |
```

Copilot reads the nearest `AGENTS.md` in the directory tree. Since skills are referenced from the root `AGENTS.md`, Copilot agents will find them when working anywhere in the repository.

### Claude Code

Add an import line to `CLAUDE.md` at the repository root:

```markdown
@skills/<skill-name>/SKILL.md
```

Claude Code loads `CLAUDE.md` at session start. The `@` import syntax causes Claude to read the referenced file. The `description` field in SKILL.md frontmatter then controls whether Claude activates the skill for a given task.

---

## SCT-Specific Conventions

Skills that generate code examples or instructions must follow SCT conventions:

| Convention | Source | Rule |
|-----------|--------|------|
| Import style | `AGENTS.md` | No inline imports; 3 groups (stdlib, third-party, internal) |
| Test style | `AGENTS.md` | pytest, not unittest.TestCase |
| Docstrings | `AGENTS.md` | Google Python docstring format |
| Error handling | `AGENTS.md` | `silence` context manager over bare try/except |
| Pre-commit | `.github/copilot-instructions.md` | Always run `uv run sct.py pre-commit` |
| Commit format | `.github/copilot-instructions.md` | Conventional Commits with SCT types/scopes |
| Test location | `.github/copilot-instructions.md` | All tests in `unit_tests/`, named `test_*.py` |
