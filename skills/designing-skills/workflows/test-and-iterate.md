# Testing and Iterating on a Skill

A 4-phase process for verifying that a skill triggers correctly and improving it based on results.

---

## Phase 1: Write Trigger Eval Queries

**Entry:** Skill has a complete SKILL.md with frontmatter `description`.

**Actions:**

1. **Write 5-10 should-trigger queries.** These are user prompts where the skill MUST activate. Draw from the "When to Use" scenarios and real user requests gathered during intent capture.

   Example for `designing-skills`:
   ```
   - "Create a new skill for code review"
   - "Help me structure a skill directory"
   - "Review this skill for anti-patterns"
   - "I want to add a new skill to the SCT repo"
   - "How should I split content between SKILL.md and references?"
   ```

2. **Write 3-5 should-NOT-trigger queries.** These are prompts for related but out-of-scope tasks. Draw from the "When NOT to Use" scenarios.

   Example for `designing-skills`:
   ```
   - "Fix the unit test for config parsing"
   - "Update the README with new installation steps"
   - "Profile the longevity test for memory leaks"
   ```

3. **Include edge cases.** Write 2-3 ambiguous queries that test the boundary of the skill's scope. Decide whether each should trigger or not.

   Example:
   ```
   - "Update AGENTS.md with a new skill entry" → should-trigger (part of skill creation)
   - "Update AGENTS.md with new architecture docs" → should-NOT-trigger (general docs)
   ```

**Exit:** A list of eval queries, each labeled as should-trigger or should-NOT-trigger.

---

## Phase 2: Test the Description

**Entry:** Phase 1 complete. Eval queries written.

**Actions:**

1. **Read the description in isolation.** Copy only the `description` field value. For each eval query, ask: "Given ONLY this description, would an LLM activate this skill for this query?"

2. **Score each query.** Mark as:
   - **Pass** — correctly triggers (or correctly stays silent)
   - **Fail** — wrong behavior (false positive or false negative)

3. **Calculate accuracy.** Count passes / total queries. Target: 100% for should-trigger, 100% for should-NOT-trigger.

4. **Record failures.** For each failure, note:
   - The query
   - Expected behavior (trigger / no trigger)
   - Actual behavior
   - Likely cause (missing keyword, overly broad term, etc.)

**Exit:** Accuracy scores and a list of failures with likely causes.

---

## Phase 3: Improve the Description

**Entry:** Phase 2 complete. Failures identified.

**Actions:**

1. **Fix false negatives** (should-trigger but didn't). Add missing trigger keywords or scenarios to the description. Common fixes:
   - Add synonyms the user might use ("structure" + "organize" + "design")
   - Add specific task verbs ("creating", "reviewing", "refactoring")
   - Add domain nouns that appear in the failing queries

2. **Fix false positives** (should-NOT-trigger but did). Narrow the description scope. Common fixes:
   - Add exclusion phrases ("Use when X, not for Y")
   - Replace broad terms with specific ones ("SCT skills" instead of "documentation")
   - Add qualifying context ("for the skills/ directory" not "for any directory")

3. **Generalize fixes.** When fixing one query, ask: "Does this fix apply to a category of queries?" Apply the fix broadly rather than patching individual cases.

4. **Check constraints.** After edits, verify:
   - Under 1024 characters
   - No angle brackets (`<`, `>`)
   - Third-person voice maintained
   - No workflow steps leaked into description

5. **Re-run Phase 2.** Test the updated description against all eval queries.

**Exit:** All eval queries pass. Description is within constraints.

---

## Phase 4: Validate Skill Quality

**Entry:** Phase 3 complete. Description triggers correctly.

**Actions:**

1. **Review every instruction for WHY.** Read each rule or instruction in SKILL.md. If it only says WHAT to do without explaining WHY, add the reasoning. Instructions with reasoning produce more reliable LLM behavior.

2. **Apply the lean test.** For each instruction, ask: "If I removed this, would the skill output get worse?" Remove instructions that don't improve outcomes — they dilute attention.

3. **Check for generalized guidance.** Each instruction should apply broadly, not just to one specific edge case. If an instruction only fixes one scenario, generalize it or remove it.

4. **Verify concrete examples.** Key instructions should have input → output examples. Abstract rules without examples cause interpretation drift.

5. **Final read-through.** Read the entire skill (SKILL.md + references + workflows) as if encountering it for the first time. Flag anything confusing, redundant, or contradictory.

**Exit:** Skill is lean, well-reasoned, and triggers correctly. Ready for use.
