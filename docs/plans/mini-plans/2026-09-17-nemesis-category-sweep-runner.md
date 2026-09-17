# Mini-Plan: Category Sweep Nemesis Runner

**Date:** 2026-09-17
**Estimated LOC:** ~250 (runner ~90, tests ~130, base-loop signal ~15, docs ~20)

## Problem

Every runner available today either shuffles the whole nemesis set and cycles it forever
(`SisyphusMonkey`) or draws from it at random by weight (`CategoricalMonkey`). There is no way to
ask for a bounded, ordered coverage run — "execute every nemesis exactly once, schema changes
first, then topology changes, then the remaining disruptive ones, then everything else, then
stop". Answering "did nemesis X run in this job, and what kind of disruption surrounded it" today
means reconstructing a random order out of the event log, and a job that is meant as a coverage
sweep has no signal that the sweep finished.

## Approach

A new runner, `CategorySweepMonkey`, in `sdcm/nemesis/monkey/runners.py`. End to end:

runner constructed -> four category selectors resolved against the nemesis registry -> categories
concatenated into one ordered list -> base `run()` prunes infeasible members through the existing
precheck -> each surviving nemesis executed once in list order -> an InfoEvent announces each
category as its first member starts -> after the last nemesis the runner ends its own thread with
a completion event.

- **Categories** are ordered, and disjoint by first match, so every nemesis lands in exactly one:

  | # | Label | Selector |
  |---|-------|----------|
  | 1 | `schema-changes` | `schema_changes` |
  | 2 | `topology-changes` | `topology_changes and not schema_changes` |
  | 3 | `other-disruptive` | `disruptive and not topology_changes and not schema_changes` |
  | 4 | `rest` | `not disruptive and not topology_changes and not schema_changes` |

  Against the current tree (112 discovered nemesis) this partitions as 25 / 13 / 46 / 28. One
  class carries both `schema_changes` and `topology_changes`, so the precedence above is a real
  decision rather than a formality, and belongs in the class docstring.

- The category table is a class attribute (label plus selector phrase), not literals buried in the
  build step, so unit tests can substitute a table built from the test-only flag tree. It is
  deliberately **not** configurable from a test yaml: a test narrows the sweep with the existing
  `nemesis_selector`, which intersects every category so the sweep stays inside the requested
  subset and keeps its category ordering.

- **Order inside a category** is alphabetical by class name. A sweep exists for coverage and
  reproducibility, so nothing is shuffled and `nemesis_multiply_factor` is ignored — multiplying
  the list contradicts "each nemesis once". Both are stated in the docstring, since both differ
  from what `SisyphusMonkey` does with the same config.

- **Category boundaries survive precheck.** The base run loop prunes infeasible nemesis from the
  list in place and preserves order, so the runner needs only one flat ordered list plus each
  entry's category label, and announces a category when the label changes between consecutive
  entries. Nothing is re-derived after pruning — unlike `CategoricalMonkey`, which has to resync
  its weights because its candidates live outside `disruptions_list`. A category emptied entirely
  by precheck is skipped silently; its exclusions are already reported by precheck itself.

- **Stopping after one pass.** The base loop ends only on the shared nemesis termination event
  (shared across all nemesis threads, so a runner must not set it), on an exhausted `cycles_count`
  supplied by the caller that starts the thread, or on `KillNemesis`, which reporting reads as
  "killed by tearDown". None of the three expresses "this runner is finite and has finished", so
  add a dedicated sentinel exception in `sdcm/exceptions.py`: the runner raises it once the sweep
  is exhausted, and the base loop catches it, publishes a completion InfoEvent naming the number
  of nemesis executed, and leaves the thread. Other runners are unaffected — none raises it.

- A job whose `test_duration` runs out mid-sweep simply stops where it is. With the default
  5-minute interval a full 112-nemesis sweep needs a multi-day job, so most runs will be partial;
  the category events make the reached point explicit, and the completion event is what
  distinguishes a finished sweep from a truncated one.

- **Naming** is the one decision worth settling before implementation: `CategorySweepMonkey` is
  used throughout this plan; `AllNemesisSweepMonkey` is the literal alternative. `CategoricalMonkey`
  is already taken by the weighted-random runner and must not be confused with it.

## Files to Modify

- `sdcm/nemesis/monkey/runners.py` — new `CategorySweepMonkey`: category table, ordered build,
  category announcement, end-of-sweep signal
- `sdcm/nemesis/__init__.py` — `NemesisRunner.run()` recognises the sweep-complete signal and exits
  the loop with a completion event
- `sdcm/exceptions.py` — sentinel exception for a finite runner that completed its pass
- `docs/nemesis.md` — row in the "Available Runners" table, plus a short subsection on sweep
  semantics (category order, one pass, self-stop, ignored `nemesis_multiply_factor`)
- `unit_tests/unit/nemesis/monkey/test_category_sweep.py` — new test module
- `unit_tests/unit/nemesis/__init__.py` — extend the test flag tree only if the real flag names are
  needed there; prefer a test-local category table over touching the shared tree

## Verification

- [x] Partition test against the real registry: the four selectors together cover every discovered
      nemesis and no class appears in two categories, so a future flag combination that escapes the
      partition fails here. Exact per-category counts (25 / 13 / 46 / 28 today) are deliberately not
      asserted - they would churn on every added nemesis without guaranteeing anything more
- [x] Ordering test on the test flag tree: the built list is grouped by category in the declared
      order and alphabetical within each group
- [x] Selector test: a `nemesis_selector` narrows every category and leaves ordering intact
- [x] Precheck test: excluded members disappear, the remaining order and category boundaries hold,
      and each surviving category is announced exactly once
- [x] Stop test: `run()` returns after every nemesis has executed once, the shared termination event
      is left unset, and no CRITICAL event is published
- [x] Empty-category test: a category whose members are all filtered out produces no announcement
      and does not end the sweep early
- [x] `uv run python -m pytest unit_tests/unit/nemesis -v`
- [x] `uv run sct.py pre-commit` passes, and `data_dir/nemesis.yml`, `data_dir/nemesis_classes.yml`
      and the generated nemesis pipelines come back unchanged — runners are not disruptions
- [ ] Manual smoke run: a short longevity with `nemesis_class_name: 'CategorySweepMonkey'` and a
      narrowing `nemesis_selector` (e.g. `limited`), checking the log for categories announced in
      the declared order and for the completion event after the last nemesis
