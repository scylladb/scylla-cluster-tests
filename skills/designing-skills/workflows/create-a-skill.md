# Creating a New SCT Skill

A 5-phase process for creating a skill from scratch in the SCT repository.

---

## Phase 1: Define Scope

**Entry:** You have a task domain in mind (e.g., "unit testing", "code review", "writing plans").

**Actions:**

1. **Draft the `description` first.** This is the most important field. Claude Code uses it to decide activation. Use third-person voice, include trigger keywords and exclusions. Test it: would this description activate for the right requests and stay silent for wrong ones?

2. **Write the "When to Use" section.** List 4-6 specific scenarios where the skill applies. Be concrete: "when writing a new unit test file in `unit_tests/`" not "when doing testing."

3. **Write the "When NOT to Use" section.** List 3-5 scenarios where a different approach is better. Name the alternative: "use integration-testing skill for backend-specific tests" not "not for complex tests."

4. **Define 3-5 essential principles.** These are non-negotiable rules for every invocation. Ask: "What mistake would ruin the output if the LLM made it?" Each principle guards against a specific failure mode.

5. **Verify SCT alignment.** Check that your scope aligns with SCT conventions in `AGENTS.md` and `.github/copilot-instructions.md`.

**Exit:** Draft description, When to Use, When NOT to Use, and essential principles documented.

---

## Phase 2: Plan Content Structure

**Entry:** Phase 1 complete. Scope defined.

**Actions:**

1. **List all content the skill needs.** What guidance, examples, references, and processes are required?

2. **Apply the 500-line test.** Can everything fit in SKILL.md under 500 lines? If not, identify what moves to references/ and workflows/.

3. **Identify reference topics.** Each reference file covers one focused topic (e.g., "import-conventions.md", "error-handling.md"). Keep each under 400 lines.

4. **Identify workflow processes.** Each workflow file is a numbered-phase process (e.g., "create-a-plan.md", "write-unit-test.md"). Keep each under 300 lines.

5. **Plan the directory structure:**
   ```
   skills/<skill-name>/
   ├── SKILL.md
   ├── references/
   │   ├── <topic-a>.md
   │   └── <topic-b>.md
   └── workflows/
       └── <process>.md
   ```

6. **Verify the one-hop rule.** All files must be reachable directly from SKILL.md. No reference-to-reference links.

**Exit:** Complete file list with content assignments for each file.

---

## Phase 3: Write Content

**Entry:** Phase 2 complete. File structure planned.

**Actions:**

1. **Write reference files first.** These are the foundation that SKILL.md summarizes and links to.

2. **Write workflow files.** Each workflow follows numbered phases:
   ```markdown
   ### Phase N: <Name>
   **Entry:** <What must be true>
   **Actions:**
   1. <Specific step>
   2. <Specific step>
   **Exit:** <How to know it's done>
   ```

3. **Write SKILL.md last.** It summarizes and routes to the reference and workflow files. Follow the SKILL.md template structure (see SKILL.md → Reference Index → skill-structure.md):
   - Frontmatter (name, description)
   - Essential Principles
   - When to Use / When NOT to Use
   - Domain-specific quick references
   - Reference Index (links to all supporting files)
   - Success Criteria checklist

4. **Include concrete examples.** Every key instruction should have an SCT-specific input → output example.

5. **Check line counts:**
   - SKILL.md: under 500 lines
   - Reference files: under 400 lines each
   - Workflow files: under 300 lines each

**Exit:** All content files written, line counts within limits.

---

## Phase 4: Configure Discovery

**Entry:** Phase 3 complete. All skill files written.

**Actions:**

1. **Update `AGENTS.md`** (Copilot discovery). Add a row to the Skills table:
   ```markdown
   | <skill-name> | <description> | `skills/<skill-name>/SKILL.md` |
   ```

2. **Update `CLAUDE.md`** (Claude Code discovery). Add an import line:
   ```markdown
   @skills/<skill-name>/SKILL.md
   ```

3. **Verify no other files need updating.** Check if `.github/copilot-instructions.md` references the skill's domain — if so, add a cross-reference to the skill.

**Exit:** Skill is discoverable by both Copilot and Claude Code.

---

## Phase 5: Validate

**Entry:** Phase 4 complete. Skill is created and registered.

**Actions:**

1. **Verify all file references.** Every path in SKILL.md must resolve to an existing file.

2. **Check frontmatter.** Valid YAML with `name` (kebab-case, matches directory) and `description` (third-person, trigger keywords).

3. **Read the description in isolation.** Would it activate for the right requests and stay silent for wrong ones?

4. **Read SKILL.md as a fresh reader.** Is the structure clear? Could an LLM follow it without prior context?

5. **Scan for anti-patterns.** Check against the anti-pattern catalog (see SKILL.md → Reference Index → anti-patterns.md):
   - No monolithic SKILL.md (AP-2)
   - No reference chains (AP-3)
   - No unnumbered phases (AP-4)
   - No missing exit criteria (AP-5)
   - No broken file references (AP-7)
   - No platform-specific content (AP-8)
   - No missing discovery configuration (AP-9)
   - No description summarizing workflow steps (AP-11)
   - No SCT convention violations in examples (AP-12)

6. **Run pre-commit (if environment supports it).** Execute `uv run sct.py pre-commit` to verify no formatting issues. This requires a working Python environment with dependencies installed; if unavailable, manually verify trailing whitespace, end-of-file newlines, and YAML validity.

7. **Verify dual-platform registration:**
   - `AGENTS.md` has the skill in its Skills table
   - `CLAUDE.md` has the `@skills/<name>/SKILL.md` import

**Exit:** All checks pass. Skill is ready for use.
