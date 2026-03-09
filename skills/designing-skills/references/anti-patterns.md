# Anti-Patterns Catalog

Common mistakes when creating SCT skills. Each entry includes the symptom, why it's wrong, and a fix.

---

## Structure Anti-Patterns

### AP-1: Vague Description and Missing Scope

**Symptom:** Skill has a vague `description` and no "When to Use" / "When NOT to Use" sections.

**Why it's wrong:** Claude Code activates skills based solely on `description`. Vague descriptions cause wrong activations or missed activations. Once active, the scope sections prevent the LLM from attempting tasks outside the skill's competence.

**Fix:** Write the description with triggering conditions ("Use when..."), third-person voice ("Guides X" not "I help with X"), and specific keywords. Add When to Use (4+ concrete scenarios) and When NOT to Use (3+ scenarios naming alternatives).

---

### AP-2: Monolithic SKILL.md

**Symptom:** SKILL.md exceeds 500 lines with everything inlined.

**Why it's wrong:** Oversized files dilute LLM attention. Critical instructions get buried in reference material.

**Fix:** SKILL.md under 500 lines with core principles and routing. Detailed reference material in `references/`. Step-by-step processes in `workflows/`.

---

### AP-3: Reference Chains

**Symptom:** SKILL.md links to file A, which links to file B, which links to file C.

**Why it's wrong:** Each hop degrades context. By the time the LLM reaches file C, context from SKILL.md has degraded.

**Fix:** All files one hop from SKILL.md. Reference files do not link to other reference files.

---

### AP-4: Unnumbered Phases

**Symptom:** Workflow uses prose paragraphs instead of numbered phases.

**Why it's wrong:** The LLM cannot reliably determine ordering from prose.

**Fix:** Number every phase. Add entry criteria, numbered actions, and exit criteria to each phase:

```markdown
### Phase 1: Setup
**Entry:** User has provided [input]
**Actions:**
1. Validate input
2. Check prerequisites
**Exit:** [Specific artifact] exists and is valid
```

---

### AP-5: Missing Exit Criteria

**Symptom:** Phases say what to do but not how to know when it's done.

**Why it's wrong:** Without exit criteria, the LLM may produce incomplete work and move on, or loop endlessly.

**Fix:** Define what "done" means for every phase. Use concrete checks: "SKILL.md exists and has valid frontmatter" not "write the skill."

---

### AP-6: No Verification Step

**Symptom:** The workflow ends with "output the results" and no validation.

**Why it's wrong:** LLMs can produce plausible but incorrect output. A verification step catches errors before the user acts on bad results.

**Fix:** Add a final phase: verify all referenced paths exist, no placeholder text remains, frontmatter is valid YAML.

---

### AP-7: Broken File References

**Symptom:** SKILL.md references `workflows/advanced.md` but the file doesn't exist.

**Why it's wrong:** The LLM either hallucinates the content or stops. Silent failures with unpredictable behavior.

**Fix:** Before submitting, verify every path referenced in SKILL.md exists.

---

## Platform Anti-Patterns

### AP-8: Platform-Specific Content in Skill Body

**Symptom:** SKILL.md contains instructions like "If you're Claude Code, do X" or "Copilot should do Y."

**Why it's wrong:** Skills should be platform-agnostic. Platform-specific discovery is handled by `AGENTS.md` (Copilot) and `CLAUDE.md` (Claude Code), not by skill content.

**Fix:** Write skill content that works identically for any AI agent. Platform-specific configuration goes in the discovery files.

---

### AP-9: Missing Discovery Configuration

**Symptom:** New skill is created but not registered in `AGENTS.md` or `CLAUDE.md`.

**Why it's wrong:** Copilot agents won't find the skill (no reference in `AGENTS.md`). Claude Code won't import it (no entry in `CLAUDE.md`).

**Fix:** Always update both discovery files when adding a new skill:
1. Add a row to the Skills table in `AGENTS.md`
2. Add an `@skills/<name>/SKILL.md` import to `CLAUDE.md`

---

## Content Anti-Patterns

### AP-10: No Concrete Examples

**Symptom:** Skill describes rules in abstract terms without showing input → output.

**Why it's wrong:** Abstract rules are ambiguous. Concrete examples anchor the LLM's understanding and reduce interpretation drift.

**Fix:** Show the exact output format with a realistic SCT-specific example.

**Before:**
```markdown
Write tests following pytest conventions.
```

**After:**
````markdown
Write tests following pytest conventions:

```python
import pytest
from sdcm.utils.common import get_data_dir_path

@pytest.fixture
def data_dir(tmp_path):
    return tmp_path / "data"

def test_get_data_dir_path_returns_existing_dir(data_dir):
    data_dir.mkdir()
    assert data_dir.exists()
```
````

---

### AP-11: Description Summarizes Workflow

**Symptom:** The `description` field summarizes the skill's workflow steps instead of listing triggering conditions.

**Why it's wrong:** When the description contains workflow steps, Claude follows the description and shortcuts past the actual SKILL.md body.

**Before:**
```yaml
description: >-
  First reads the test file, then identifies patterns,
  then rewrites using pytest fixtures and parametrize.
```

**After:**
```yaml
description: >-
  Guides writing unit tests for the SCT framework using
  pytest conventions. Use when creating new test files,
  refactoring tests, or reviewing test coverage.
```

---

### AP-12: SCT Convention Violations in Examples

**Symptom:** Skill examples use patterns that violate SCT conventions (inline imports, unittest.TestCase, bare try/except).

**Why it's wrong:** LLMs learn from examples. Bad examples in skills propagate convention violations across the codebase.

**Fix:** Verify all code examples follow SCT conventions:
- Imports at top of file, three groups (stdlib, third-party, internal)
- pytest style (not unittest.TestCase)
- Google docstring format
- `silence` context manager for error handling (not bare try/except):
  ```python
  from sdcm.tester import silence
  with silence(parent=self, name="optional operation", verbose=True):
      # code that may fail non-critically
  ```

---

### AP-13: Reference Dump Instead of Guidance

**Symptom:** Skill pastes a full specification or API reference instead of teaching when and how to apply it.

**Why it's wrong:** The LLM already has general knowledge. What it needs is judgment: when to apply technique A vs B, what tradeoffs to consider.

**Fix:** Teach decision criteria, not raw documentation. Show when to use X vs Y and why, with SCT-specific context.

---

### AP-14: No Trigger Test Queries

**Symptom:** Skill is created and registered but never tested to verify the description triggers correctly.

**Why it's wrong:** Descriptions that look reasonable can still cause false positives (activating for wrong requests) or false negatives (missing correct requests). Without testing, these bugs only surface when a user encounters unexpected behavior.

**Fix:** Write 5-10 should-trigger queries and 3-5 should-NOT-trigger queries. Test the description against each query in isolation. See [test-and-iterate.md](../workflows/test-and-iterate.md) for the full process.

---

### AP-15: Description Exceeds Constraints

**Symptom:** Description is over 1024 characters, contains angle brackets (`<`, `>`), or uses first-person voice.

**Why it's wrong:**
- Overly long descriptions waste context window space and force Claude to process unnecessary text at session start.
- Angle brackets break YAML parsing in some tool chains.
- First-person voice ("I help with...") causes Claude to role-play the description instead of using it as a matching criterion.

**Fix:** Keep description under 1024 characters (aim for 200-400). Remove all angle brackets — use plain text or quoted examples instead. Use third-person voice: "Guides..." not "I help with..."

---

### AP-16: No Intent Capture

**Symptom:** Skill was written without collecting example user prompts, expected outputs, or failure modes.

**Why it's wrong:** Skills written without concrete intent produce generic guidance that doesn't match real usage patterns. The description misses keywords users actually use, the "When to Use" section is too abstract, and the skill body covers hypothetical scenarios instead of real ones.

**Fix:** Before writing any skill files, gather: 5-10 example user prompts (should-trigger), 3-5 boundary prompts (should-NOT-trigger), 2-3 examples of expected good output, and known failure modes. This front-loads the hardest design decisions.

---

### AP-17: Instructions Without Reasoning

**Symptom:** Skill rules say WHAT to do but not WHY.

**Before:**
```markdown
Use numbered phases for all workflows.
```

**After:**
```markdown
Use numbered phases for all workflows — unnumbered prose produces unreliable
execution order because the LLM cannot determine sequence from paragraph structure.
```

**Why it's wrong:** Instructions without reasoning are brittle. The LLM follows them literally for exact matches but can't generalize to novel situations. When the WHY is included, the LLM can apply the principle's intent even in cases the instruction didn't explicitly cover.

**Fix:** For every instruction, add a clause or sentence explaining the reasoning. Use "because", "since", "this prevents", or em-dashes to connect the rule to its purpose.
