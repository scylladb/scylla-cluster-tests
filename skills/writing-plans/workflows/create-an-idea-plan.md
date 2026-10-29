# Creating an Idea Plan

A 3-phase workflow for writing `plan.md`, the build plan of a spec in
`docs/plans/<feature>/<idea>/`. The development flow in
`docs/development-flow.md` describes the intent, the spec, and the plan. The
plan is optional: the diff can carry the files and the tests.

---

## Phase 1: Read the Spec

**Entry:** `docs/plans/<feature>/<idea>/spec.md` exists.

**Actions:**

1. Read `spec.md`, and `intent.md` when it exists.
2. Read `docs/INDEX.md` and the standards that the change touches.
3. Read the code that the spec names. Confirm that each file and class exists,
   or is new in this change.
4. When `plan.md` exists, update it. Do not write a second one.

**Exit:** You know the components and the contracts that the plan must build.

---

## Phase 2: Write the Tasks

**Entry:** Phase 1 complete.

**Actions:**

1. Read [idea-plan-template.md](../references/idea-plan-template.md).
2. Write one task for each unit of work, in the order of dependency. Each
   task names its files, its internals, and its checkboxes, the failing test
   first.
3. Build only what the spec designs. When a task needs a design change, stop
   and update the spec first.
4. Write the prose in ASD-STE100 Simplified Technical English.

**Exit:** Each component and contract of the spec has a task that builds it.

---

## Phase 3: Review With the User

**Entry:** Phase 2 complete.

**Actions:**

1. Show the file to the user.
2. Ask whether to commit it. On a yes, commit it with a subject such as
   `docs(plans): plan <short title>`.
3. During the build, check each box when its step is done. Commit the checked
   boxes with the task.

**Exit:** The user has read the plan and decided about the commit.
