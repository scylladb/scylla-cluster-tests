---
name: writing-specs
description: >-
  Writes the design spec of an SCT change in docs/plans/FEATURE/IDEA/spec.md:
  the overview, the constraints, the components, the contracts, the risks,
  and the decisions, with code and Mermaid diagrams first. Use when the user
  asks for a spec, a design, or a design review document for one change, or
  when an intent.md exists and the design comes next. Also use to write a
  spec from code that already exists. Not for the problem statement (use
  writing-intents), the build plan, a multi-phase roadmap, or a mini-plan
  (use writing-plans).
argument-hint: "[feature/idea] [--from-code]"
---

# Writing a Spec

Write `docs/plans/<feature>/<idea>/spec.md`, the design document of the
development flow in `docs/development-flow.md`. The flow is optional. Write a
spec only when the user asks for one, or agrees to one.

## Essential Principles

### The Spec Is the Design Review Document

A reviewer learns the solution from the spec before reading the diff. The
overview states the solution in a few lines. The constraints, the diagrams,
and the contracts carry the design. The change builds what the spec designs,
and nothing more.

### Code and Diagrams First

After the overview, show the design as code and diagrams. A data shape is a
dataclass or a pydantic model. A configuration parameter is its entry and its
default. A module API is its signature. A flow is a Mermaid diagram. Prose
carries only what code does not state: a constraint, a decision, a risk.

### Files and Tests Go to the Plan

File lists, internal functions, and tests belong to `plan.md`. Line numbers
belong to neither. A spec that names them changes with every refactor and
hides the design.

### Record Each Decision

An answer of the user to a design question gets a line in `Decisions`. A
question for the reviewer gets an `(open)` line. Name no rejected option.

## Phase 1: Resolve the Directory

**Entry criteria**: the user wants a spec.

1. Take `<feature>/<idea>` from the arguments when they give it.
2. Otherwise run `ls docs/plans/*/*/intent.md docs/plans/*/*/spec.md` and
   find the idea directory that holds an `intent.md` and no `spec.md`. When none or several fit, propose a path as
   in `writing-intents` and ask the user.
3. When `spec.md` exists, read it and update it. Do not write a second one.

**Exit criteria**: the directory path is known and the user agrees to it.

## Phase 2: Work Out the Design

**Entry criteria**: Phase 1 complete.

1. Read `intent.md` when it exists. Otherwise ask the user for the problem in
   a few lines.
2. Read the code that the change touches. Every component, class, and
   configuration key that the spec names must exist, or be new in this
   change. Read `docs/INDEX.md` and the standards that apply.
3. When the arguments hold `--from-code`, read the diff with `git diff` and
   `git diff --cached`. State the design that the code carries.
4. Otherwise work out the constraints and the design with the user. Ask one
   question at a time. Each answer that shapes the design becomes a
   `Decisions` line. A question that the user leaves to the reviewer becomes
   an `(open)` line.
5. Check the SCT impact areas. They are the backends, the configuration in
   `sdcm/sct_config/` and `defaults/`, the nemesis, the monitoring and Argus
   reporting, and the Jenkins pipelines.

**Exit criteria**: you can fill the overview, the constraints, and the
design.

## Phase 3: Write

**Entry criteria**: Phase 2 complete.

1. Read [spec-template.md](references/spec-template.md).
2. Write the file in the shape of the template. Follow its section rule.
3. A spec can near 200 lines, with design areas that can ship alone. Then
   propose a split into more idea directories.
4. Write the prose in ASD-STE100 Simplified Technical English.
5. Show the file to the user.
6. Ask whether to commit it. On a yes, commit it with a subject such as
   `docs(plans): design <short title>`.

**Exit criteria**: the user has read the file and decided about the commit.

## When NOT to Use

- A small change that one commit message explains.
- A bug fix that changes no contract and no module boundary. Its root cause
  goes in the commit message and the pull request description.
- A multi-phase roadmap with tracking in `docs/plans/MASTER.md`. Use the full
  plan format of `writing-plans`.

## Next Step

After the spec, offer the plan through `writing-plans`. The plan is optional:
the diff can carry the files and the tests.
