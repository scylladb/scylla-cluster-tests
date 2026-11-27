# Typed access to the SCT configuration

**Jira:** SCT-950

## Problem

`SCTConfiguration` is a Pydantic model. Each option is a typed attribute.
Most of the code still reads the options by a string key:
`self.params.get("n_db_nodes")`. String-key access has these problems:

- The IDE gives no completion, no type, and no go-to-definition. The caller
  gets `Any`.
- A typo in the key does not fail. `get()` returns `None` for an unknown key,
  so `params.get("tset_duration")` reads as an unset option. No linter and no
  type checker can find the typo.
- `get()` looks like `dict.get()`, but it takes no fallback argument. People
  and agents write `params.get("key", fallback)`. The call fails with a
  `TypeError` only when the code path runs, which can be late in a long test.
- `get()` hides special behavior. For the multi-region options it joins a
  list into one space-separated string. For a dotted key such as
  `stress_image.ycsb` it walks into a dict option. A reader cannot see either
  behavior at the call site.

The code also uses the dict-style forms `params["key"]` and
`params["key"] = value`, with the same lack of typing.

## Who it affects

Every SCT contributor and every agent that reads or writes framework code or
test code. The configuration owners, who cannot rename or restructure an
option safely while its readers are string literals.

## Evidence

Counts on `upstream/master`, 2026-11-05:

- About 1,280 `params.get("...")` calls in `sdcm/`, and 360 in the top-level
  `*_test.py` files. The heaviest files are `sdcm/tester.py` (338),
  `sdcm/cluster.py` (153), and `sdcm/cluster_k8s/__init__.py` (72).
- About 35 `self.params["..."]` reads and writes in `sdcm/` and the test files.
- Seven dotted-key calls, all under `stress_image`: `cassandra-stress`,
  `scylla-bench`, `ycsb`, `alternator-dns`, and `minicloud`.
- The multi-region options that `get()` joins into a string:
  `region_name`, `ami_id_db_scylla`, `ami_id_loader` (AWS), and
  `gce_datacenter` (GCE).
- `get()` ends with `getattr(self, key, None)`, so an unknown key returns
  `None` and does not raise.

The plan `docs/plans/config/sct-config-nested-sections.md` moves groups of
options into section models, for example `config.nemesis.interval`. The
string keys of a moved group stop working. Typed access makes those moves
safe to find and to review.

## Definition of done

- No literal-key `params.get("...")` call and no `params["..."]` access
  remains in `sdcm/` or in the test files. Each one reads or writes the
  attribute.
- A key that is not a literal (a variable or an f-string) is read through an
  explicit attribute lookup at the call site.
- Each multi-region call site states whether it wants the list or the joined
  string. No call site depends on hidden joining.
- A check in pre-commit or in the unit tests fails when new code adds a
  literal-key `params.get("...")` call.
- Each step leaves the values that tests read unchanged. The unit tests and
  pre-commit pass after each pull request.
- `get()` keeps no dotted-key walk and no multi-region joining.

## Out of scope

- Moving options into nested section models. The nested-sections plan owns
  that.
- Renaming, removing, or retyping any option, or changing a default.
- Plain dicts that some code also calls `params`, such as the trigger-matrix
  job parameters and the OCI builder options. They are not
  `SCTConfiguration`.
- The dict-merge methods `update()` and `__contains__`, which the loading
  code uses.
