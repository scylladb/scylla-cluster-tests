# Development flow

An idea can go through three written documents before the code: the intent,
the spec, and the plan. The flow is optional. The author decides for each
change.

The documents help the reviewers most. A reviewer reads the problem and the
design first, and then checks the code against them.

## When to use the flow

Write the documents when the change is big, or when it touches the
architecture of SCT. Examples:

- A new subsystem, a new backend, or a new kind of nemesis.
- A change to a contract between modules, such as a base class that many
  backends override, the configuration schema, or the events format.
- A change that spans more than one pull request.

A small fix, a configuration tweak, or a test addition does not need them.

## The documents

All the documents of one idea go to one directory:

```
docs/plans/<feature>/<idea>/
├── intent.md   # the problem
├── spec.md     # the design
└── plan.md     # the files, the order of work, and the tests
```

`<feature>` is the area of SCT, as in `config`, `nemesis`, `cleanup`, or
`costs`. Use a directory that exists under `docs/plans/` when one fits.
`<idea>` is a short kebab-case name for the specific change.

| Document | Holds | Skill |
|---|---|---|
| `intent.md` | The problem, who it affects, the evidence, and the outcome | `writing-intents` |
| `spec.md` | The overview, the constraints, the design, the contracts, the risks, and the decisions | `writing-specs` |
| `plan.md` | One task per unit of work, with its files and its tests | `writing-plans` |

Each document is optional. A change can have an intent and a spec, and let
the diff carry the plan. A spec reads the intent when it exists.

These documents do not get frontmatter. They are not registered in
`docs/plans/MASTER.md` or `docs/plans/progress.json`. The full 7-section plans
and the mini-plans in [plans/INSTRUCTIONS.md](plans/INSTRUCTIONS.md) keep
their own rules.

## Order of work

1. Write the intent. State the problem, not the fix.
2. Write the spec from the intent.
3. Write the plan from the spec.
4. Write the code. Follow the standards in [INDEX.md](INDEX.md).
5. Open the pull request. Link the idea directory in the description.

The author reads each document before the next one starts. An agent shows the
document to the author, and asks before it commits the document.

Commit the documents before the code on the branch. The reviewer then reads
the problem, the design, and the code in that order.

When the code exists first, write the intent and the spec from the diff. The
spec then states the design that the code carries. The diff carries the plan.

## Bug fixes

A bug fix does not get a separate root cause document. Write the root cause
in the commit message and in the pull request description. Give the
mechanism, the code path, and the evidence that ties the cause to the
symptom.

A bug fix that changes a contract or a module boundary can use the flow like
any other big change.

## Changes during the build

A decision during the build that changes the design updates `spec.md` in the
same commit. Add a line to its `Decisions` section when the design does not
state the reason.

## Review

[REVIEW.md](../REVIEW.md) holds the review guide. When the pull request
carries an idea directory, the reviewer checks the code against its spec.
