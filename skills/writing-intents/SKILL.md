---
name: writing-intents
description: >-
  Writes the intent of an SCT change: the problem, who it affects, the
  evidence, and what good looks like, in docs/plans/FEATURE/IDEA/intent.md.
  Use when the user asks to state the problem, write an intent, start an
  idea, or describe a change before its design. Also use to write an intent
  from code that already exists. The intent is the first, optional document
  of the development flow, before the spec and the plan. Not for the design
  (use writing-specs) or the build plan (use writing-plans).
argument-hint: "[feature/idea] [--from-code]"
---

# Writing an Intent

Write `docs/plans/<feature>/<idea>/intent.md`, the first document of the
development flow in `docs/development-flow.md`. The flow is optional. Write
an intent only when the user asks for one, or agrees to one.

## Essential Principles

### State the Problem, Not the Fix

The intent says what goes wrong and why it matters. The spec holds the
design. A fix in the intent anchors the reviewer on one solution before
anybody checks the problem.

### Copy Evidence Verbatim

A log line, a traceback, a Jenkins or Argus link, or a Jira issue goes in as
it is. A paraphrase loses the detail that a reviewer needs to confirm the
problem.

Remove each secret before you copy. SCT logs can hold passwords, tokens,
keys, and stress commands with credentials. Replace each one with
`<redacted>`, because a committed secret stays in the git history.

### The Author Approves

The author knows the problem best. Show the file and ask before you commit
it.

## Phase 1: Resolve the Directory

**Entry criteria**: the user wants an intent.

1. Take `<feature>/<idea>` from the arguments when they give it.
2. Otherwise propose one. `<feature>` is the area of SCT, as in `config`,
   `nemesis`, `cleanup`, or `costs`. Run `ls docs/plans/` and reuse a
   directory that fits. `<idea>` is a short kebab-case name for the change.
3. When `docs/plans/<feature>/<idea>/intent.md` exists, read it and update it.
   Do not write a second one.

**Exit criteria**: the directory path is known and the user agrees to it.

## Phase 2: Gather the Problem

**Entry criteria**: Phase 1 complete.

1. Start from what the conversation gives: the request, a Jira issue, a
   failing run, or a pull request.
2. When the arguments hold `--from-code`, read the working tree diff with
   `git diff` and `git diff --cached`. State the problem that the code solves.
3. Ask the user for what is missing, one question at a time. Ask about the
   evidence and the outcome first, because the reviewer needs them most.

**Exit criteria**: you can fill every section of the template.

## Phase 3: Write

**Entry criteria**: Phase 2 complete.

1. Read [intent-template.md](references/intent-template.md).
2. Write the file in the shape of the template. Keep every heading. Write
   `None` under a heading that has no content.
3. Write the prose in ASD-STE100 Simplified Technical English: short
   sentences, active voice, one topic per paragraph.
4. Show the file to the user.
5. Ask whether to commit it. On a yes, commit it with a subject such as
   `docs(plans): state the intent for <short title>`.

**Exit criteria**: the user has read the file and decided about the commit.

## When NOT to Use

- A small fix, a configuration tweak, or a test addition. The commit message
  carries the reason.
- A bug fix that changes no contract and no module boundary. Its root cause
  goes in the commit message and the pull request description.
- A full 7-section plan or a mini-plan. Use `writing-plans`.

## Next Step

After the intent, offer the spec through `writing-specs` when the change is
big or touches the SCT architecture.
