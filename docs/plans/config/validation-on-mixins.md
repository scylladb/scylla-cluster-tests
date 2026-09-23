---
status: draft
domain: config
created: 2026-09-15
last_updated: 2026-09-15
owner: fruch
---

# Config Validation on Mixins

## Problem Statement

`SCTConfiguration` validates itself in two places that have nothing to do with the options they
check. Roughly 140 lines of inline cross-field checks sit at the tail of the constructor, and about
25 `_validate_*` / `_verify_*` methods sit in `sdcm/sct_config/config.py` behind
`SCTConfiguration.verify_configuration`. Both are far from the option definitions they enforce, which
now live in 29 domain mixins under `sdcm/sct_config/mixins/`.

The concrete costs:

- **Nothing is discoverable from the option.** Someone adding an option to `ScyllaConfigMixin` has no
  reason to look in `config.py` for the rule that constrains it, and no reason to add one there.
  Rules get written inline in `__init__` because that is where the existing ones are.
- **`__init__` is ~560 lines** of loading, cloud image resolution and validation interleaved, carrying
  three separate lint suppressions for branch, local and statement counts.
- **Checks run at the wrong time or not at all.** Nothing enforces which phase a check belongs to, so
  the zero-token-node rules are duplicated in both `__init__` and `verify_configuration`, and the
  gradual-throughput check runs twice per config.
- **Failures are stripped under `python -O`.** Seven checks — including the per-backend required
  parameters, seed count and node-per-AZ divisibility — are bare `assert` statements.
- **The first attempt failed.** [PR #14124](https://github.com/scylladb/scylla-cluster-tests/pull/14124)
  moved ten of the inline blocks to Pydantic validators on the central class. It is still open, red,
  and now conflicting. Its failure is the most useful input this plan has, and is analysed below.

`SCTConfiguration`'s own class docstring already names this work as SCT-525 phase 4.

## Current State

### How the configuration is assembled

`sdcm/sct_config/config.py:SCTConfiguration` is a Pydantic model assembled from the mixins listed in
`sdcm/sct_config/mixins/__init__.py:CONFIG_GROUPS`. Each mixin is a plain `BaseModel` owning one
domain's options; `CONFIG_GROUPS` fixes both the field order on the assembled model and the section
order in the generated documentation. Every option lives in exactly one mixin. Fields stay flat, so
`config.nemesis_class_name` and the YAML keys are unchanged.

**No mixin currently declares any validator.** They hold a docstring, a `config_group` marker and
field definitions, nothing else. The only Pydantic validator anywhere in the package is on the
`AdaptiveTimeoutMultipliers` type in `sdcm/sct_config/types.py`; coercion elsewhere is done by
`BeforeValidator` functions attached to the type aliases in that module.

`SCTConfiguration.__init__` is a hand-written loader. It builds an all-defaults model and then merges
backend defaults, user YAML, region data, environment variables and computed values into it, one
field assignment at a time. The model sets `validate_assignment=True`, so **every one of those
assignments re-runs every model validator on the class** — measured at 242 assignments for a minimal
docker configuration.

### What validates where today

| Where | What | Character |
|---|---|---|
| Tail of `__init__` | instance provisioning type, authenticator credentials, alternator authorization, stress durations, fullscan parameters, docker simulated racks, endpoint snitch forcing, DNS names per backend, Scylla network interfaces, K8s TLS/SNI, zero-token nodes, gradual throughput | pure data checks, plus one value-forcing rule and one random pick |
| `verify_configuration` | unexpected and invalid `SCT_*` variables, per-backend required options, version supplied, multi-region parameters, seeds, nemesis placement and parallelism, nodes per AZ, placement groups, partition ranges, data volumes, safepoint logging, scylla-bench workloads, docker parameters, rack-aware teardown, xcloud parameters, instance type availability | mixed: some pure, some needing resolved images, some hitting cloud APIs or the filesystem |
| `verify_configuration_urls_validity` | image tags, repository URLs | network only |

`sdcm/sct_config/defaults.py` holds `REQUIRED_PARAMS`, `BACKEND_REQUIRED_PARAMS`,
`XCLOUD_PER_PROVIDER_REQUIRED_PARAMS` and `PER_PROVIDER_MULTI_REGION_PARAMS`. These tables exist only
to drive validation, and `verify_configuration` appends to the backend table in place on every call.

`init_and_verify_sct_config` is the one entry point that runs the whole chain. Many callers — most of
`sct.py`, and `sdcm/utils/lint/validator.py` — construct a configuration and deliberately never call
`verify_configuration`, because they only need the values.

### What PR #14124 established, by failing

The PR guarded its validators with a private `_config_loaded` attribute defaulting to off, flipping
it at the end of `__init__` and then invoking the validators as a manual chain. Four consequences:

1. **The decorators bought nothing during the load** — the guard was off for its whole duration, and
   the rules then ran as the same imperative chain the refactor was meant to remove.
2. **Every other construction path silently skipped validation.** `model_validate`, `model_copy`,
   `model_construct` and direct keyword construction never run `__init__`, so the guard stayed off
   forever and the rules never fired.
3. **Multi-key updates broke.** `unit_tests/conftest.py`'s `params` fixture sets four authenticator
   options in one `update` call; once the guard was on, the first key raised because the other three
   were not set yet. Around fifteen other test modules use the same construct-then-update shape.
4. **A value-forcing rule needed a hack.** Assigning inside a model validator under
   `validate_assignment=True` re-enters the validator chain, so the PR wrote straight into the
   instance dictionary to dodge the recursion, bypassing coercion and the assigned-fields bookkeeping.

Master has since moved on: four of the ten blocks the PR touched have changed behaviour, and one of
its three claimed fixes has already landed independently.

### What is already proven to work

`unit_tests/unit/config/test_mixin_validators.py` was written as the bridgehead for this work. Using
throwaway mixins it establishes that field and model validators declared on a mixin do run on the
assembled model, that validators from several mixins all run without multiple inheritance shadowing
them, that a mixin's model validator can read fields owned by other mixins, and that
`model_construct` bypasses the lot. The mechanism is not in question — only its integration with the
loading constructor.

## Goals

1. **Every validation rule lives on the mixin that owns the options it checks.** `config.py` keeps
   only genuinely cross-domain rules and the dispatch machinery.
2. **Each rule runs exactly once per configuration load**, down from up to 242 times.
3. **Rules run on every construction path**, not only through `__init__` — the default is validation,
   not silence.
4. **A multi-key update stays possible**, and a failed one leaves the configuration unchanged, so the
   existing construct-then-update call sites keep working without modification.
5. **No check is a bare `assert`** — all seven become explicit raises, so `python -O` cannot strip
   them.
6. **`__init__` carries no validation**, and sheds its lint suppressions for branch and statement
   counts.
7. **The per-backend requirement tables move out of `defaults.py`** onto the mixins whose options they
   describe, and stop being mutated in place.

## Implementation Phases

Each phase is one PR, stacked on its predecessor.

### Phase 1: Validation plumbing, proven on three rules

**Importance: Critical** — everything else depends on this being right.

Introduce a small validation module in the `sct_config` package providing the contract the mixins
will be written against:

- A **deferral window** used by any operation that mutates several options before the result is
  meaningful. Cross-field rules are suspended inside the window and run once on exit. The flag
  backing it defaults to *validation enabled*, so a construction path nobody anticipated gets the
  checks rather than silently skipping them — the single thing PR #14124 had inverted.
- A **decorator for mixin authors**, used in place of a raw model validator, so a rule is one method
  with no guard boilerplate.
- A **single-shot re-validation entry point** that fires every model validator on an existing
  instance in place, without copying it and without disturbing its private attributes or its record
  of which fields were explicitly set.

`__init__` runs its entire load inside the deferral window and validates once on exit.
`SCTConfiguration.update` becomes **transactional**: it defers, applies every key, validates once,
and on any failure restores the configuration to exactly its prior state. Without the rollback, a
failed update leaves partial state and the *next*, unrelated assignment raises a confusing error —
PR #14124's failure relocated rather than fixed.

The resulting contract, which belongs verbatim in the module docstring:

> Every mutation of an `SCTConfiguration` leaves it satisfying every cross-field rule, or raises and
> changes nothing. A single assignment validates immediately. `update` applies all keys and validates
> once at the end, so options that are only jointly valid can be set together.

Three rules move in this phase, chosen to exercise every part of the mechanism rather than for their
size: **K8s SNI requires TLS** to `KubernetesConfigMixin` (the trivial case); **DNS names are only
supported on some backends** to `CommonConfigMixin` (reads an option another mixin owns); and
**`PasswordAuthenticator` requires a user and password** to `ScyllaConfigMixin` (the rule that broke
PR #14124, and the one that proves transactional `update`).

**Definition of Done:**

- [ ] Validation module added, with the contract stated in its docstring
- [ ] `__init__` defers during its load and validates once on exit
- [ ] `update` is transactional — validates once, rolls back fully on failure
- [ ] The three rules live on their mixins; the corresponding inline blocks are gone from `__init__`
- [ ] `unit_tests/conftest.py` and every other construct-then-update call site pass **unmodified**
- [ ] Guard tests added to `unit_tests/unit/config/test_mixin_validators.py` (see Testing)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** none.

### Phase 2: The rest of the inline constructor validation

**Importance: High**

Move the remaining pure checks out of `__init__` onto their owning mixins: alternator authorization
to `AlternatorConfigMixin`; stress and prepare durations to `StressConfigMixin` as field validators,
since they coerce rather than only check; fullscan parameters to `LongevityConfigMixin`; the docker
simulated-racks image requirement to `DockerConfigMixin`; the Scylla network interface rules — the
largest single block, and the biggest readability win — and the zero-token node rules to
`ScyllaConfigMixin` and `CommonConfigMixin` respectively. Moving zero-token also removes its
duplication between `__init__` and `verify_configuration`.

Two rules need care. **Endpoint snitch forcing writes a value**, so it cannot use the deferrable
decorator: it must run even while deferred and must be idempotent, so that the assignment it makes
re-enters the chain once and then settles. Its "an explicit snitch must be Gossiping" half is a pure
check and splits out. **Instance provisioning type** stops being a runtime check and becomes a
constrained annotation on the option itself.

The two `assert` statements in this set become explicit raises.

Staying in `__init__` and explicitly out of scope: cloud image resolution, capacity reservation and
dedicated host reservation, `user_prefix` construction — which depends on image metadata resolved
earlier in the constructor — and the random cassandra-stress driver version pick. A validator may
re-run; something that rolls a random value must not.

**Definition of Done:**

- [ ] No validation remains in `__init__`; its branch and statement lint suppressions are removed
- [ ] Every moved rule raises the same message it raises today
- [ ] Value-forcing rules are idempotent and documented as such
- [ ] `assert` replaced by explicit raises in the moved set
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 1.

### Phase 3: Normalisation split, and the construction-time checks in `verify_configuration`

**Importance: Medium**

The gradual-throughput check looks pure and is not — it rewrites throttle steps and thread counts in
place. Split it: normalisation becomes a field validator on `PerformanceConfigMixin`, and the residual
pure check becomes a cross-field rule. This also removes the duplicate invocation, since the check
currently runs both from `__init__` and from `verify_configuration`.

Four sites in `unit_tests/test_perf_gradual_config.py` expect the raise to come from an explicit
`verify_configuration` call rather than from the update that sets the bad value. They must be
restructured in the same commit.

Two checks currently in `verify_configuration` are genuinely construction-time and move now: the
backtrace-decoding disable pattern to `MonitoringConfigMixin`, and the NVMe self-test type to
`LogsConfigMixin` as a constrained annotation.

**Definition of Done:**

- [ ] Gradual-throughput normalisation and checking are separate, and the check runs once
- [ ] The affected perf-gradual tests are restructured in the same commit
- [ ] The two construction-time checks live on their mixins
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 2.

### Phase 4: `verify_configuration` checks move onto their mixins

**Importance: High**

The remaining `_validate_*` / `_verify_*` methods move to the mixin that owns their options, and
`verify_configuration` becomes a dispatcher that collects and runs them.

**These stay plain methods, not Pydantic validators.** The deciding question is *when a rule becomes
true*. A rule that holds as soon as the configuration is loaded belongs in Pydantic. A rule that only
holds for a configuration someone intends to actually *run* — one that needs resolved images, cloud
API answers, or files on disk — belongs in this phase. `verify_configuration` exists precisely
because its checks are not satisfied at the end of `__init__`, which is why several callers construct
a configuration and never call it. Turning those into construction-time rules would make every later
assignment on such a configuration raise.

Network- and filesystem-touching checks move too — instance type availability, xcloud availability
zones, rack-aware teardown, Scylla.d override files. They keep their phase; they stop living in the
middle of `config.py`.

All remaining `assert` statements become explicit raises. Six tests currently pin `AssertionError`,
two of them on exact message strings; they are updated in the same commit.

**Definition of Done:**

- [ ] `verify_configuration` is a dispatcher; no check bodies remain in `config.py` except
      genuinely cross-domain ones
- [ ] The phase boundary is documented on the dispatcher: what belongs here versus in a mixin validator
- [ ] No `assert` remains in any configuration check
- [ ] A freshly loaded minimal configuration passes `verify_configuration` with no rule firing
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 3.

### Phase 5: Per-backend requirement tables move onto the mixins

**Importance: Medium**

`BACKEND_REQUIRED_PARAMS`, `XCLOUD_PER_PROVIDER_REQUIRED_PARAMS` and
`PER_PROVIDER_MULTI_REGION_PARAMS` in `sdcm/sct_config/defaults.py` exist only to drive validation, so
they follow their validators. Each mixin declares which of *its own* options a given backend requires,
and the per-backend view is assembled from the mixins the same way `CONFIG_GROUPS` assembles the
field order.

This also fixes the current in-place append to the backend requirement table, which mutates shared
state on every `verify_configuration` call.

**Needs Investigation:** whether the `xcloud` requirements, which vary by cloud provider *and* by
cluster type, fit the same per-mixin declaration or need a second dimension. Resolve before starting
the phase.

**Definition of Done:**

- [ ] Each mixin declares its own per-backend required options
- [ ] The tables are assembled from the mixins; the `defaults.py` copies are removed
- [ ] No requirement table is mutated at validation time
- [ ] The per-backend error messages are unchanged
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 4.

### Phase 6: Documentation and close-out

**Importance: Medium**

Rewrite the `SCTConfiguration` class docstring — its "still to come" list is done by this point — and
document the two-mechanism rule where mixin authors will meet it: in the mixins package docstring and
in the validation module. Regenerate the option reference, since constrained annotations replaced
runtime checks for at least three options and the generated accepted-values text changes with them.

Close PR #14124 as superseded, and mark Phase 1 of
`docs/plans/sct-config-validation-and-lazy-images.md` superseded by this plan, leaving its Phase 2
(lazy image resolution) intact and unblocked.

**Definition of Done:**

- [ ] `SCTConfiguration` docstring reflects the finished state
- [ ] The two-mechanism rule is documented for mixin authors
- [ ] Generated option reference regenerated and committed
- [ ] PR #14124 closed with a pointer to this plan
- [ ] The superseded plan phase is marked, MASTER.md and progress.json updated

**Dependencies:** Phase 5.

## Testing Requirements

### Mechanism guard tests (Phase 1, extended thereafter)

`unit_tests/unit/config/test_mixin_validators.py` currently proves the mechanism with throwaway
mixins. It grows to guard the integration, against the real assembled model:

- Each rule runs **exactly once** per configuration load — the regression test for the 242× problem.
- Rules run on plain construction and on `model_validate`, not only through `__init__`, so the
  PR #14124 silent-skip failure cannot recur.
- A failed `update` leaves the configuration byte-for-byte unchanged.
- **No two mixins declare a validator with the same method name.** Pydantic keys validators by
  attribute name, so a collision across 29 mixins silently drops one of them. This is a guard test,
  not a naming convention.
- **Instance revalidation is not enabled** anywhere in the model configuration — enabling it turns the
  single-shot re-validation into a deep re-validation and the mechanism collapses.
- Cross-mixin validator ordering is not relied upon by any rule. Ordering is deterministic but
  undocumented; where a rule genuinely must run after another, it belongs on `SCTConfiguration`
  itself, whose validators run last.

### Per-rule tests (Phases 2–5)

Each moved rule gets positive and negative cases driven the way the existing config tests are — via
`SCT_*` environment variables and the YAML fixtures in `unit_tests/test_configs/`, extending that
directory's existing scenario-named convention. Negative cases assert on the message, not only the
exception type. Three network-interface fixtures already exist in that directory but are unreferenced;
Phase 2 gives them their tests.

### Regression coverage

- `unit_tests/unit/test_config.py` and `unit_tests/unit/config/` pass unchanged, except where a phase
  explicitly restructures a test and says so.
- The construct-then-update modules — the `params` fixture in `unit_tests/conftest.py`, plus the
  cluster-cloud, teardown-validator, seed-selector, KMS and version-utils tests — pass unmodified
  after Phase 1.
- `unit_tests/test_gce_use_dns_names.py` keeps passing: it asserts on a message, and a Pydantic
  validation error is still a `ValueError` carrying that text.

### Integration and manual

- `unit_tests/integration/test_config.py` continues to pass where credentials are available.
- For at least one test case per backend (docker, aws, gce), `sct.py conf` produces a configuration
  dump **identical** to the one master produces. This is the strongest end-to-end signal that no rule
  changed behaviour, and it is worth running at every phase boundary.
- The pipeline linter, which constructs and verifies every test case, reports the same set of failures
  as on master.

## Success Criteria

All Definition of Done items across the six phases, plus:

- **Rule locality:** every option's validation is reachable from the mixin that defines it.
- **No behaviour change:** the configuration dump for the sampled test cases is byte-identical to
  master's at every phase boundary, and every preserved rule raises its existing message.
- **No silent skips:** the guard tests fail if a validator is shadowed, skipped, or never runs.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A rule that mutates is written as a deferrable check, causing recursion or a double run | Medium | High | The decorator's docstring forbids mutation; the gradual-throughput rule — which looks pure and is not — is called out by name in Phase 3 and split before it moves |
| Two mixins declare a validator with the same name, silently dropping one | Medium | High | Guard test in Phase 1, not a naming convention; the failure is otherwise invisible |
| A check moved to construction time is not actually true at construction, breaking callers that never verify | Medium | High | The Phase 4 rule is explicit about which mechanism a rule gets; Phase 4 adds a test that a freshly loaded configuration passes `verify_configuration` with nothing firing |
| A behaviour change slips in while moving a rule, silently altering which configurations are accepted | Medium | High | Configuration dumps compared against master per backend at every phase boundary; messages preserved verbatim |
| Instance revalidation gets enabled later by someone tuning the model, breaking the mechanism invisibly | Low | High | Pinned by a guard test in Phase 1 with the reason in its docstring |
| The stack grows stale against a fast-moving `config.py` | Medium | Medium | Phases are ordered so each is independently mergeable; Phases 1–3 touch the constructor, Phases 4–5 touch verification, so the surfaces barely overlap |
| In-place mutation of a list-valued option bypasses validation entirely, leaving a rule stale | Low | Medium | Known and out of scope; tracked as a follow-up rather than silently relied upon |

## Out of Scope

Tracked separately, not addressed here:

- Roughly a quarter of the model's options are annotated as non-optional while holding no value, so
  re-assigning an option its own current value raises. This constrains the implementation but is a
  pre-existing defect with its own fix.
- A dict-era `keys()` call survives in the region-data branch of `__init__` and would raise on the
  path that reaches it.
- Options mutated in place rather than reassigned bypass assignment validation entirely.
- Lazy cloud image resolution — already Phase 2 of
  `docs/plans/sct-config-validation-and-lazy-images.md`, unblocked by but independent of this plan.
