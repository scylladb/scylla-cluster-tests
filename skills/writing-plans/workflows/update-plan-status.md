# Updating Plan Status

A 3-phase workflow for updating a plan's status across all tracking files.

---

## Phase 1: Update the Plan File

**Entry:** A plan's status has changed (e.g., implementation started, phase completed, plan blocked).

**Actions:**

1. **Read the plan file** and locate the YAML frontmatter at the top.

2. **Update the `status` field** to the new value. Valid values: `draft`, `approved`, `in_progress`, `blocked`, `complete`.

3. **Update `last_updated`** to today's date (`YYYY-MM-DD`).

4. **If completing a plan**, check all Definition of Done items in the Implementation Phases section.

5. **If archiving a completed plan**, move the file to `docs/plans/archive/`.

**Exit:** Plan file frontmatter reflects the new status.

---

## Phase 2: Update MASTER.md

**Entry:** Phase 1 complete. Plan file updated.

**Actions:**

1. **Read `docs/plans/MASTER.md`** and locate the plan's row in its domain table.

2. **Update the status badge** to match the new frontmatter status.

3. **If the plan was archived**, update the file link to point to `archive/<filename>`.

4. **If a pending_pr plan was merged**, change status from `pending_pr` to `draft` and add the file link.

**Exit:** MASTER.md status matches the plan file.

---

## Phase 3: Update progress.json

**Entry:** Phase 2 complete. MASTER.md updated.

**Actions:**

1. **Read `docs/plans/progress.json`** and locate the plan's entry by `id`.

2. **Update the `status` field** to match the new frontmatter status.

3. **Update `phases_done`** if a phase was completed. Count phases marked as done in the plan file.

4. **Update `phases_total`** if it was previously `null` and the plan now has a defined number of phases.

5. **Validate JSON syntax** — ensure the file is valid JSON after editing.

**Exit:** progress.json entry matches the plan file and MASTER.md. All three tracking locations are consistent.
