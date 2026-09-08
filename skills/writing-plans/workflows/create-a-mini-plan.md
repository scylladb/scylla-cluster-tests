# Creating a Mini-Plan

A 3-phase lightweight workflow for writing a mini-plan for small, single-PR changes.

---

## Phase 1: Route

**Entry:** You have a task that needs a plan and need to confirm it qualifies as a mini-plan.

**Actions:**

1. **Check if user specified plan size.** If the user explicitly said "small plan" or "mini-plan", proceed. If they said "big plan", use the full plan workflow in [create-a-plan.md](create-a-plan.md) instead.

2. **Check PR label (if working on a PR).** Run:
   ```bash
   gh pr view <number> --json labels --jq '.labels[].name' | grep -q '^plans$'
   ```
   - PR has `plans` label -> redirect to full plan workflow
   - PR does NOT have `plans` label -> proceed with mini-plan

3. **Estimate scope (if no user answer and no PR context).** Count files likely to be modified and estimate LOC:
   - Multiple new files, new config parameters, multiple backends, CI changes -> likely needs a full plan
   - Single file change, bug fix, test addition, refactoring one module -> mini-plan is appropriate
   - When in doubt, ask the user

**Exit:** Confirmed this is a mini-plan. If not, redirected to the full plan workflow.

---

## Phase 2: Write the 4 Sections

**Entry:** Phase 1 complete. Task qualifies as a mini-plan.

**Actions:**

1. **Set the header and metadata:**
   - Title: `# Mini-Plan: <descriptive title>`
   - Date: today's date in YYYY-MM-DD format
   - Estimated LOC: rough estimate based on scope
   - Related PR: include PR number if applicable

2. **Write the Problem section.** 1-3 sentences explaining what needs to change and why. Be specific about the pain point.

3. **Write the Approach section.** Bulleted list of steps in execution order. Each bullet should be a concrete action, not a vague goal.

   Describe **behaviour, not code**: what should happen and why, not how it is written. No line numbers, no snippets of internal logic, no file-internal detail — reference code by symbol (`file.py:ClassName`) and leave the "how" to the PR (PAP-1, PAP-9). If the change has a lifecycle, state the whole sequence once as an arrow chain rather than spreading it over bullets (PAP-11).

4. **Write the Files to Modify section.** For each file:
   - **Verify the path exists** using file-reading tools before listing it
   - Describe what changes in that file
   - If creating a new file, note it as "(new file)"

5. **Write the Verification section.** Include:
   - At least one specific verification step for the change
   - `uv run sct.py pre-commit` passes (always include this)
   - Unit test commands if tests are added

6. **Save the file** as `docs/plans/mini-plans/YYYY-MM-DD-kebab-case-name.md`.

**Exit:** All 4 sections written with verified file paths.

---

## Phase 3: Validate

**Entry:** Phase 2 complete. Mini-plan written.

**Actions:**

1. **Verify all file paths resolve.** Re-check that every path in "Files to Modify" points to a real file (or is marked as new).

2. **Verify the verification checklist is concrete.** Each checkbox should be something a person can actually run or check -- no vague items like "it works" or "code is clean".

3. **Confirm no full-plan artifacts.** The mini-plan should NOT have:
   - YAML frontmatter
   - MASTER.md registration
   - progress.json entry
   - More than 4 sections

4. **Check reviewability.** Run `wc -l` on the file:
   - Over ~150 lines -> cut code detail first (PAP-1, PAP-9), then split out any separable effort into its own mini-plan (PAP-12), then drop investigation narrative and restated rules (PAP-10)
   - `grep -nE ':[0-9]+' <file>` -> any hit is a line-number reference; replace it with a symbol reference (PAP-9)
   - Any fenced code block that is not an interface, config format, or API contract -> replace with a prose description of the behaviour

5. **Run pre-commit (if available).** Execute `uv run sct.py pre-commit` to check formatting.

**Exit:** Mini-plan is validated, saved in `docs/plans/mini-plans/`, and ready to use.
