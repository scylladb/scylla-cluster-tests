---
status: draft
domain: config
created: 2026-03-01
last_updated: 2026-10-22
owner: pehala
---
# SCT Configuration: Initial Support for Nested Sections

## Problem Statement

`SCTConfiguration` is one flat Pydantic model with 535 user-facing options. [PR #15972](https://github.com/scylladb/scylla-cluster-tests/pull/15972) split its *source* into 29 domain mixins. At runtime, though, it is still one object. Splitting the files did not change three things:

1. **Prefixes stand in for structure.** `nemesis_*`, `mgmt_*`, `oci_*` and `k8s_*` encode grouping as a naming convention. `unit_tests/unit/config/test_option_groups.py` has to enforce that convention, and it already carries an exception list. Several options (`manager`'s `use_mgmt`, `nemesis`'s `sla`) cannot be identified by name at all.
2. **Group logic is tied to the whole configuration.** A method that works on one group of options reads flat prefixed options such as `self.nemesis_interval`. It cannot be called or tested without building the whole configuration.
3. **One namespace for everything.** The 29 mixins and the root merge into one class through multiple inheritance, so every field and method of the configuration lives in one namespace of 549 fields plus methods. A method name has to be unique across all mixins, because under multiple inheritance the first class in the resolution order silently overrides the others. A field can shadow a method of the same name, and neither can reuse a name that `BaseModel` or the root already defines (`get`, `update`, `copy`). No check catches such a collision, and every group method needs the group prefix to stay unambiguous.
4. **Validation has no owner.** Every rule runs against the whole object, so it all sits in `sdcm/sct_config/config.py`: `verify_configuration()`, 29 `_validate_*`/`_verify_*`/`_check_*` methods, and the inline checks at the end of `__init__`. That file is still 3,320 lines. A mixin can hold validators, but it cannot be built or validated without the other 28 mixins, so nothing stops a rule from drifting back into the assembler. Not every rule can move to a group: some read several groups, or need backend, regions and cloud API calls, and those stay on the root. A clearer structure still lets more rules move over time, because a rule that reads one section's fields can be placed on that section.

The [PR #13845 review](https://github.com/scylladb/scylla-cluster-tests/pull/13845) settled the direction: a deep structure is what makes the split real. Once options live in a section model (for example, `config.nemesis.interval`), the section owns its fields and its validation. It can then be built, validated and mocked independently, and the file split follows from that. This plan replaces the mixin-first plan from that PR. It covers the first step: the tooling that makes sections possible, and the `nemesis` section as the pilot that proves it. Converting the remaining groups is left to a future plan. Each group moves completely in one PR: code, tests, YAML and Jenkins. There is never a period where a group can be configured or read in two ways.

## Benefits

Sections are an enabler. Moving validators is a side effect of them, not the reason for them. The benefits, in order of weight:

1. **Related settings live together.** `config.nemesis.interval` sits next to every other nemesis setting, in YAML (`nemesis: {...}`), in env vars and in code. A test config reads as a set of topics instead of 535 flat names that have to be grepped by prefix. This is the main benefit, and it holds for every section.
2. **Each section is redesigned, not just regrouped.** Regrouping existing options is a small gain. The section PR is where the group is revisited and reshaped into something that makes sense as a unit. Two examples:
   - `nemesis` (this plan's pilot): every nemesis runner carries its own distinct config values, fully validated against its runner class.
   - `stress` (a future section, outside this plan): an abstraction on top of the stress tools, so switching tool does not require knowing every tool-specific option and how it interacts with the others.
3. **Sections unify and resolve config, so consumers get simpler.** A section can hold basic logic next to its data: merging a runner's options with the section defaults, computed values, conversion between input forms. Consumers then read a finished value (`config.nemesis.resolve_runners()`) instead of repeating that logic. The `nemesis` pilot does this for per-runner resolution. Mixins can hold the same logic, since they are Pydantic models, but it fits less cleanly: the methods share one namespace with the other 500+ fields and need the group prefix to stay unambiguous, they read flat prefixed names, and they cannot be called or tested without building the whole configuration. On a section the same logic uses short names, reads its own fields as attributes, and runs standalone.
4. **Code reads shorter and clearer.** Section logic uses short names (`interval`, not `nemesis_interval`) and reads its own fields as attributes. Each group's methods live in their own namespace, so they need no group prefix and cannot override another group's methods. Consumers read `config.nemesis.interval` instead of a flat prefixed `get("nemesis_interval")`.
5. **Sections can be tested individually.** A section is built and validated on its own, without the full configuration.
6. **Validation lives with its data.** Rules that read one section move onto it. The root keeps the rules that span sections.

[Nemesis section redesign](#nemesis-section-redesign) shows these benefits on the pilot, with before and after examples.

The initial plan does not depend on moving validators onto the mixins first. Converting a mixin to a section after that would only be a rename plus merge work, which is the small gain item 2 avoids.

## Current State

### Already shipped ([PR #15972](https://github.com/scylladb/scylla-cluster-tests/pull/15972), SCT-525 phases 1–3)

- `sdcm/sct_config/` is a package:
  - `config.py` holds the assembler, loading, validation and doc generation.
  - `types.py` holds the annotated aliases and `SctField`.
  - `helpers.py` holds merging and env sub-key parsing.
  - `defaults.py` holds the backend tables.
  - `mixins/` holds 29 files, one per group.
- `sdcm/sct_config/mixins/__init__.py:CONFIG_GROUPS` fixes the group order. Every option belongs to exactly one mixin, and `test_option_groups.py` guards that.
- Each group has its own generated docs page under `docs/configuration_options/`. The page opens with the mixin's docstring.
- `unit_tests/unit/config/test_mixin_validators.py` proves two things about Pydantic validators declared on a mixin: they run on the assembled model, and a mixin's `model_validator` can read fields owned by another mixin.

The mixins are the starting point for this plan, and they stay in place. A mixin is the group's declaration unit: it carries the group's `config_group` title and docstring, and today its flat fields. When a group converts, its mixin stays in `CONFIG_GROUPS` and declares one field, the section (`nemesis: NemesisConfig`), while the flat fields move into the section model under `sdcm/sct_config/sections/`.

### How a configuration is loaded

`SCTConfiguration.__init__` builds the model empty, then applies layers in this order:

1. Docker image defaults
2. `defaults/test_default.yaml` and the per-backend defaults file
3. The user files from `SCT_CONFIG_FILES`
4. `SCT_*` environment variables

`helpers.py:merge_dicts_append_strings` applies each layer by handing the model itself to `anyconfig.merge`, so every top-level key becomes one call to `__contains__`, `__getitem__` or `__setitem__`, and through those to `setattr` with `validate_assignment=True`. Image resolution, region and placement handling, and the inline validation run after all layers are applied. `init_and_verify_sct_config()` then calls `verify_configuration()`.

Behaviours of this pipeline that nesting has to work with:

- **Model-typed values are replaced, not merged.** `anyconfig` recurses only into existing values that are Mappings. Dict-typed options (`stress_image`, `sizing_*`) are therefore deep-merged across layers. Model-typed options are not: `test_metadata`, `mgmt_agent_backup_config` and `adaptive_timeout_multipliers` are replaced wholesale by any later layer that sets them. A naive `nemesis: NemesisConfig` field would make a user file containing `nemesis: {interval: 5}` wipe every nemesis default.
- **`++` appending is top-level only.** `merge_dicts_append_strings` looks only at the top-level keys of the incoming layer. `String`/`StringOrList` fields are appendable by default, which covers 265 options including `nemesis_class_name` and `nemesis_selector`.
- **Env vars are derived from field names.** `_load_environment_variables` maps `SCT_<FIELD>` from `model_fields`. It already reads one level of nested sub-keys in two forms, `SCT_<FIELD>.<key>` and `SCT_<FIELD>__<KEY>`; the `__` form lowercases the key. `SCT_STRESS_IMAGE.cassandra-stress` (set from groovy), `SCT_SIZING_DB__VCPU` and `SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.<op>` use this today. `_check_unexpected_sct_variables` rejects any `SCT_*` name that does not match a field.
- **Dotted lookup stops at models.** `get("a.b")` walks dicts only (`_dotted_get`). Code uses it for `stress_image.<tool>` and `teardown_validators.<name>`.

### Code that assumes a flat namespace

- **Consumers.** There are 1,738 `params.get("<literal>")` calls over 522 distinct keys, plus 152 `params["…"]` subscripts. Plain attribute access to options is close to nonexistent. The typed-access plan ([PR #13878](https://github.com/scylladb/scylla-cluster-tests/pull/13878)) has not started.
- **Name tables.** Option names are stored as strings and resolved at runtime:
  - `defaults.py`: `REQUIRED_PARAMS`, `BACKEND_REQUIRED_PARAMS`, `STRESS_CMD_PARAMS`, `AMI_ID_PARAMS`, `BACKEND_IMAGE_FIELD`, `PER_PROVIDER_MULTI_REGION_PARAMS`
  - `config.py`: `SIZING_ROLE_PARAMS`
  - `sdcm/sct_provision/region_definition_builder.py:ConfigParamsMap` and its OCI/Azure/GCE instances
  - `sdcm/provision/common/oracle.py`
  - `sdcm/utils/cloud_catalog/cost.py`
  - the AWS/GCE zone resolvers
- **Silent-failure trap.** `_check_backend_defaults` keeps only names that exist in `model_fields`. A required-param name that moves into a section would be dropped without an error, and the backend's required-param check would quietly stop running.
- **Parallel loaders.** Three tools re-read YAML with flat keys instead of going through the model:
  - `sct_sizing.py`
  - `sdcm/utils/trigger_matrix/audit.py`
  - `sdcm/utils/lint/docs_linter.py`

  Nested keys in test-case YAML would be invisible to them.
- **Generated YAML.** All 116 files in `configurations/nemesis/` are rendered from `templates/test_config/template-nemesis-config.yaml.j2`, and that template hard-codes flat keys.
- **Output.**
  - `sdcm/tester.py` sends the config to Argus twice: `self.params.model_dump()` through `submit_sct_run` and `self.params.model_dump_json()` through `sct_submit_config`. The Argus backend reads only `n_db_nodes`, `instance_type_db`, `ami_id_*`, `gce_*`, `post_behavior_*`, `cluster_backend`, `stress_duration`, `config_files` and `test_method` out of `sct_config` (`plugins/sct/testrun.py`, `resource_setup.py`), and no nemesis keys. `RunConfigParam` indexes `sct_config.<key>[.<sub>]`, so a nested option shows up as `sct_config.nemesis.interval`.
  - Resolved-placement persistence (`tester.py`, `sct.py`) writes and reads AMI params by flat name.
  - `dump_config()` feeds `sct.py conf` and the run log.
- **Docs.** The generator keys headings, anchors, `SCT_*` names and the alphabetical index by flat field name, and has no notion of sections, so nested options would not appear with their YAML path or nested env name. `unit_tests/unit/config/test_help.py` asserts the heading format, and the `update-conf-docs` pre-commit hook regenerates the pages whenever `sdcm/sct_config/*.py` changes.
- **Jenkins.** `vars/*.groovy` exports 122 distinct flat `SCT_*` names one by one. Of the planned early groups:
  - Nemesis: none.
  - Manager: `SCT_USE_MGMT` plus 14 other manager vars.
  - OCI: `SCT_OCI_REGION_NAME`, `SCT_OCI_INSTANCE_TYPE_DB`, `SCT_OCI_IMAGE_DB`.

  `sdcm/utils/lint/env_builder.py` keeps its own map from pipeline params to `SCT_*` names.

### Precedent: existing nested options

Several options already hold nested values. In YAML they look like a section, but each is one option of its group.

| Option | Type | Merge across layers today |
|---|---|---|
| `stress_image`, `teardown_validators`, `agent` | `DictOrStr` | Key by key |
| `sizing_db`, `sizing_loader`, `sizing_monitor` | `dict` | Key by key |
| `append_scylla_yaml` | `DictOrStrOrPydantic` | Key by key |
| `adaptive_timeout_multipliers` (`AdaptiveTimeoutMultipliers`), `test_metadata` (`sdcm/test_metadata.py:TestMetadata`), `mgmt_agent_backup_config` (`sdcm/mgmt/common.py:AgentBackupParameters`) | Pydantic model | Replaced whole |
| `scylla_network_config`, `kafka_connectors`, `grafana_screenshot_panels` | List of dicts or models | Replaced whole |

Model-typed options convert on assignment through a `BeforeValidator`, which shows that Pydantic sub-models fit the current loader for a single value. None of these options is a *section*, a group of options with defaults spread across layers, which is what this plan adds.

These options keep their form in this plan. Because they are already nested, they are prime candidates for follow-up conversions: each can become a section, or a field of one (for example `sizing_*` under a nodes section), in its own group's PR. The merge rules in the Design section cover all of them, so such a conversion changes no merge behaviour.

## Goals

1. **Sections are supported end to end.** The root `SCTConfiguration` can hold section models next to its flat options. A section is declared once and works in YAML, env vars, attribute access, validation, docs and output. The root holds the cross-cutting `common` options, the runtime and computed fields, and one attribute per section, named after its mixin module (`config.nemesis`). The section attribute is declared on the group's mixin, so `CONFIG_GROUPS` and the root assembly do not change, and each later group decides for itself whether it keeps flat fields or declares a section. Fields inside a section drop the group prefix (`nemesis_interval` becomes `config.nemesis.interval`), unless the section's PR restructures them.
2. **`nemesis` is the first section, migrated completely and redesigned.** Runners and their options are declared under one `nemesis:` key, and each runner can carry its own validated config. Its code, validators, tests, YAML and docs move in one PR, which proves the tooling on a real group.
3. **Sections stand alone.** Every section can be instantiated and validated with no env vars, no YAML, no network access and no other section present (a test that exercises code reading other groups supplies those separately), for example `NemesisConfig(interval=5, runners=[{"SisyphusMonkey": {}}])`. A standalone section holds only the values it is given; defaults still come from `defaults/test_default.yaml` (see Section contract).
5. **Validation lives with its data.** A rule that reads only one section's fields is a validator on that section. The root keeps only rules that span sections.
5. **One form only.** A converted option has exactly one YAML form (`nemesis: {interval: 5}`), one env form (`SCT_NEMESIS__INTERVAL`) and one way to read it (`config.nemesis.interval`). There are no aliases. An old flat key or `SCT_*` name is an unknown option and fails loudly at load time. The root `get()` behaves as today: it returns `None` for an unknown name. Sections are read through attributes only.
6. **A group migrates completely in one PR.** That PR converts the section and moves its validators. It also migrates every user of the old names in the repo:
   - consumers;
   - unit tests and their fixtures;
   - YAML under `defaults/`, `test-cases/`, `configurations/` and `unit_tests/test_configs/`, plus the templates that generate YAML;
   - `vars/*.groovy` and jenkinsfiles that export the group's `SCT_*` variables;
   - lint tooling and docs.

   `uv run sct.py lint-pipelines` is the gate. It loads every pipeline's configuration through the real loader with the env that Jenkins would set, so a missed key or env var fails the PR.

   Two more checks cover what the gate cannot see:
   - **Other repos.** `scylla-pkg` and `qa-internal` are searched for the group's old `SCT_*` names and YAML keys. Any hit is migrated in a companion PR that merges together with the group's PR.
   - **Local run.** A test case that uses the group runs end to end on minicloud, through teardown, log collection and resource cleanup, so a stage that sets an old name fails before the PR merges.

**Non-goals:**
- Converting any group other than `nemesis`. That is left to a future plan.
- Lazy or optional image resolution. That is covered by [sct-config-validation-and-lazy-images.md](../sct-config-validation-and-lazy-images.md).
- Composing a partial configuration from a subset of mixins, for unit tests or for stages such as duration calculation, provisioning, log collection and cleanup. Keeping the mixins leaves that path open, but it is not part of this plan.
- Migrating root `.get()` calls to attribute access and removing `get()` from the root. That is covered by the typed-access plan ([PR #13878](https://github.com/scylladb/scylla-cluster-tests/pull/13878)). This plan does not wait for it: `get()` stays on the root unchanged, and sections are typed from the start, read through attributes only. A converted group therefore needs no later typed-access migration, and the typed-access plan shrinks with every group converted. Every existing `nemesis` usage moves to typed access in Phase 2.

## Design

### Section contract

- **Declaration.** The group's mixin stays in `CONFIG_GROUPS` and declares the section as its only field: `nemesis: NemesisConfig = Field(default_factory=NemesisConfig)`. The mixin keeps its docstring and `config_group` title. The docs generator reads both per class from `CONFIG_GROUPS` (`_fields_by_group`, `_group_blurb`), never from the assembled model, which is why all 29 mixins already carry the same `config_group` ClassVar without clashing. The section model lives in `sdcm/sct_config/sections/<group>.py`.
- **Base class.** A section is a Pydantic model deriving from a shared `ConfigSection` base in `sdcm/sct_config/sections/`. That base enables `validate_assignment`, so `config.nemesis.interval = "x"` fails the same way a root assignment does. It also forbids unknown keys, so a misspelled or outdated key inside a section fails loudly.
- **Fields.** Section fields keep their `SctField` metadata: description, `appendable`, and `BeforeValidator` converters. They keep the same types they have today.
- **Default.** The root declares each section with a default instance, so an empty section is always present and never `None`. Section fields default to unset, as root fields do today. `defaults/test_default.yaml` and the per-backend defaults files stay the source of truth for default values; they declare the section in its nested form and merge field by field like any other layer.
- **Access.** A section is read and written through attributes only. It has no `get()`, `[]` or `in`.
- **Names.** Field names are the old names with the group prefix stripped. Fields that never had the prefix (`sla`, `use_mgmt`) are named in their group's PR. A section PR may restructure its fields (the `nemesis` pilot replaces three parallel lists with `runners` and nests the grow/shrink knobs).

### Inputs and reads

| | Form |
|---|---|
| YAML | `nemesis: {interval: 5}` |
| Env | `SCT_NEMESIS__INTERVAL=5`. This is the existing one-level nested syntax that `SCT_SIZING_DB__VCPU` already uses. |
| Append | `nemesis: {selector: "++ …"}` |
| Read | `config.nemesis.interval` |
| Write | `config.nemesis.interval = 5` (validated) |
| Flat names in YAML and env (`nemesis_interval`, `SCT_NEMESIS_INTERVAL`) | Rejected with an unknown-option error |
| `get("nemesis_interval")` | Returns `None`, as `get()` does for any unknown name. The migration removes every such call; see the Phase 2 DoD |
| `get("nemesis.interval")`, `config.nemesis.get("interval")` | Not supported: dotted `get()` does not descend into sections, and sections have no `get()` |

**`get()` on the root.** `get()`, `[]`, `in` and `update()` stay on the root with today's behaviour: `get()` returns `None` for an unknown name, `[]` raises. Dotted lookups into dict-typed root options keep working: `get("stress_image.<tool>")` and `get("teardown_validators.<name>")` walk the dict, and a missing sub-key returns `None`. Moving consumers off `get()` is left to the typed-access plan, except for the options of a converted group.

### Merge rules

One rule set applies to every value, at every depth: root options, sections, fields inside sections and the existing nested options listed under Precedent.

1. **Mappings merge key by key, recursively.** A dict, a Pydantic model or a section set in a later layer merges into the earlier value one key at a time. Keys the later layer does not set keep their earlier value.
2. **Lists and scalars are replaced.** A later layer that sets a list or a scalar replaces the earlier value whole.
3. **`++` appends.** On an appendable field, a string starting with `++` appends to the earlier string, and a list whose first item is `++` extends the earlier list. This works at any depth, so `nemesis: {selector: "++ …"}` and `SCT_NEMESIS__SELECTOR="++ …"` behave like a top-level `++` today.
4. **String forms are parsed first.** A value given as a JSON or YAML string (env vars, `DictOrStr` fields) is converted to its dict, list or model, then merged under rules 1–3.

| Value | Result |
|---|---|
| `stress_image`, `sizing_*`, `teardown_validators`, `agent`, `append_scylla_yaml` | Key by key (rule 1), unchanged |
| `nemesis:` section, `nemesis.grow_shrink` | Field by field (rule 1) |
| `adaptive_timeout_multipliers`, `test_metadata`, `mgmt_agent_backup_config` | Field by field (rule 1). **Changed:** today replaced whole |
| `nemesis.runners`, `scylla_network_config`, `kafka_connectors`, `grafana_screenshot_panels` | Replaced whole (rule 2), unchanged |
| `nemesis.runners: ["++", …]` | Appended (rule 3) |

The one behaviour change is for model-typed options. Their defaults are empty (`adaptive_timeout_multipliers: {}`, `test_metadata: null`) or unset, so a change shows only where two layers set the same model, for example `configurations/adaptive_timeout_multipliers.yaml` over a test case that also sets it. The Phase 1 `sct.py conf` comparison with `master` finds every such case.

**Where merging happens.** The merge helper, which every layer already passes through, recurses into mappings and models. The alternative is to collect all layers into one nested dict and build the model once. That would move *when* the `BeforeValidator`s run, and it would break the hooks that rewrite the env dict before merging (`_resolve_instance_sizes`, `_constrain_loader_arch_to_stress_tools`).

### Output

Every output shows converted groups only in the nested form. There is no flat view.

- **`dump_config()`.** `sct.py conf` and the run log show the nested form.
- **Argus payload.** The `model_dump()` sent to Argus is nested for converted groups, in the same PR that converts the group. The Argus backend reads none of the nemesis keys (see Current State), so the pilot needs no Argus change. A later group whose keys Argus reads ships the Argus-side change together with that group's PR.

### Migration tooling

A script under `utils/` rewrites one group's flat keys into its section across all YAML files. It keeps comments, key order and `++` values. The same script verifies the result: every test-case file loads to the same values on `master` and on the branch, with old names mapped to new paths. Each group PR runs it; it is deleted once the last group is converted.

### Nemesis section redesign

The `nemesis` group is not only moved under a `nemesis:` key. Its options are reshaped around the thing they configure: nemesis runners.

#### What is wrong today

- **Runners are index-aligned lists.** Running several nemesis threads (runners) takes `nemesis_class_name`, `nemesis_selector` and `nemesis_seed` as parallel lists. Entry *i* of each list belongs to runner *i*, a list of one value is copied to every runner, and only a length-parity validator ties them together.
- **Every other option is global.** `nemesis_interval`, `nemesis_multiply_factor` and `nemesis_filter_seeds` apply to all runners, so a test cannot run a fast non-disruptive runner next to a slow disruptive one.
- **Runner-specific options have no place of their own.** `CategoricalMonkey` takes its weight distribution only from Python, so `cdc_replication_test.py` builds that runner by hand, outside the config. `FixedOrderMonkey` reuses `nemesis_selector` for a different meaning: for it, the selector is a comma-separated list of nemesis class names to run in order, not a selector expression, and `nemesis_multiply_factor` is silently ignored.
- **Resolution lives in the consumer.** `sdcm/tester.py:get_nemesis_class` reads the lists through `params.get()`, rejects old string syntaxes, broadcasts or indexes each list, and looks every class up by name with `getattr(nemesis, …)`.

#### Before and after

**Test-case YAML.** Before, from `longevity-multidc-parallel-topology-schema-changes-12h.yaml`: two runners are spread over three lists, and `interval` is shared by both, so the schema-changes runner cannot run more often than the topology one.

```yaml
nemesis_class_name: ['SisyphusMonkey', 'SisyphusMonkey']
nemesis_selector: ["topology_changes", "schema_changes and not disruptive and not ModifyTableCompactionMonkey and not ModifyTableCompressionMonkey"]
nemesis_seed: [253, 328]
nemesis_interval: 10
nemesis_during_prepare: false
```

After: each runner is one entry that carries its own settings, and the shared settings sit next to it.

```yaml
nemesis:
  interval: 10
  during_prepare: false
  runners:
    - SisyphusMonkey:
        selector: "topology_changes"
        seed: 253
    - SisyphusMonkey:
        selector: "schema_changes and not disruptive and not ModifyTableCompactionMonkey and not ModifyTableCompressionMonkey"
        seed: 328
        interval: 5  # not expressible before
```

**Runner resolution.** Before, `get_nemesis_class` does the broadcasting, indexing and class lookup itself, and `_validate_nemesis_parallel_config` in `config.py` checks the list lengths separately. After, the section has already validated every entry against its runner class at load time, and the tester takes the result:

```python
for runner_class, runner_config in self.params.nemesis.resolve_runners():
    ...
```

**Runner-specific options.** Before, a test that runs a random topology runner next to a fixed reproduction sequence and a weighted runner cannot say so in YAML. The fixed sequence hides in the second selector, whose meaning depends on the class at the same index, and the weighted runner must be built in Python, as `cdc_replication_test.py` does with method-style keys (`"nodetool_decommission": 5`) that no config check ever sees:

```yaml
nemesis_class_name: ['SisyphusMonkey', 'FixedOrderMonkey']
nemesis_selector: ["topology_changes", "DecommissionMonkey, NoCorruptRepairMonkey, DecommissionMonkey"]
nemesis_multiply_factor: 2  # silently ignored by FixedOrderMonkey
# CategoricalMonkey: built in the test's Python code
```

After, each runner declares its own options under its own name, and a misspelled disruption or option fails at config load:

```yaml
nemesis:
  multiply_factor: 2
  runners:
    - SisyphusMonkey:
        selector: "topology_changes"
    - FixedOrderMonkey:
        order: [DecommissionMonkey, NoCorruptRepairMonkey, DecommissionMonkey]
    - CategoricalMonkey:
        dist: {DecommissionMonkey: 5, TerminateAndRemoveNodeMonkey: 5, GrowShrinkClusterNemesis: 5}
        default_weight: 0
```

**Validation per runner.** Before, an option is valid or not for the whole test; there is no way to say "this option belongs to that runner". After, each entry is checked against its runner class's config model, and the error names the runner index and class. Each of these fails at load time:
- `dist` on a `SisyphusMonkey` entry, or a `CategoricalMonkey` entry without `dist`;
- a `FixedOrderMonkey` entry without `order`, with a `selector` or `multiply_factor` it would ignore, or with an `order` naming an unknown nemesis class (today that raises only when the runner is constructed, after the cluster is up);
- a misspelled option such as `selectr` on any entry.

#### Target shape

```yaml
nemesis:
  # Defaults for every runner
  interval: 5
  seed: 123
  multiply_factor: 2
  filter_seeds: false
  selector: "not disruptive"
  # Shared by all runners
  during_prepare: false
  sequence_sleep_between_ops: 2
  sla: false
  grow_shrink:
    add_node_cnt: 1
    instance_type: i4i.large
    double_load_duration: 30
  runners:
    - SisyphusMonkey: {}
    - SisyphusMonkey:
        selector: "disruptive"
        interval: 15
        seed: 456
    - FixedOrderMonkey:
        order: [StopStartMonkey, NoCorruptRepairMonkey]
    - CategoricalMonkey:
        dist: {StopStartMonkey: 2, HardRebootNodeMonkey: 1}
        default_weight: 0
```

The section has three kinds of fields:

- **Runners.** Every runner, single-runner tests included, is an entry of `nemesis.runners`. An entry is a single-key mapping `{<RunnerClass>: {<options>}}`.
- **Runner defaults.** `selector`, `seed`, `interval`, `multiply_factor` and `filter_seeds` at the `nemesis:` level apply to every runner that does not set them itself.
- **Shared settings.** `during_prepare`, `sequence_sleep_between_ops`, `sla` and the `grow_shrink` group are read outside a runner (by the test flow, the loaders or the grow/shrink disruptions) and apply to the whole test. `sla` gates the SLA nemeses (`sdcm/nemesis/monkey/sla.py`, its only reader) so they do not run in a non-SLA test.

#### Option mapping

| Old option | New path |
|---|---|
| `nemesis_class_name` | `nemesis.runners[].<RunnerClass>` (the entry's key) |
| `nemesis_selector` | `nemesis.selector` (default) or `nemesis.runners[].<RunnerClass>.selector` |
| `nemesis_seed` | `nemesis.seed` (default) or `nemesis.runners[].<RunnerClass>.seed` |
| `nemesis_interval` | `nemesis.interval`, overridable per runner |
| `nemesis_multiply_factor` | `nemesis.multiply_factor`, overridable per runner |
| `nemesis_filter_seeds` | `nemesis.filter_seeds`, overridable per runner |
| `nemesis_during_prepare` | `nemesis.during_prepare` |
| `nemesis_sequence_sleep_between_ops` | `nemesis.sequence_sleep_between_ops` |
| `sla` | `nemesis.sla` |
| `nemesis_add_node_cnt` | `nemesis.grow_shrink.add_node_cnt` |
| `nemesis_grow_shrink_instance_type` | `nemesis.grow_shrink.instance_type` |
| `nemesis_double_load_during_grow_shrink_duration` | `nemesis.grow_shrink.double_load_duration` |

The migration script turns parallel lists into one runner entry each. A list of one value becomes the `nemesis:` default.

Env: `SCT_NEMESIS__INTERVAL=10` for a scalar field. `SCT_NEMESIS` and `SCT_NEMESIS__RUNNERS` take a YAML or JSON string.

#### Runner config models

- `NemesisRunnerConfig` in `sdcm/nemesis/` is the model every runner uses by default: the per-runner options above, each unset by default, unknown options rejected.
- A runner with options of its own defines a model next to the runner and points its `config_model` class attribute at it. Adding options to a runner touches only the runner's module. Two runners do this today:
  - `CategoricalMonkeyConfig`: required `dist` (nemesis class name → weight) and `default_weight`, on top of the common options.
  - `FixedOrderMonkeyConfig`: required `order`, a list of nemesis class names run in that order and repeated; a name may repeat. It has no `selector` or `multiply_factor`, since the runner neither selects nor shuffles. Today's comma-separated selector form is gone.
- The `runners` field type is built from the runner registry (`NemesisRegistry` over `NemesisRunner`). Every runner class is one member of a discriminated union, tagged by its class name and validated by its `config_model`. A new runner needs no config change.
- Nemesis class names in `dist` and `order` are checked against the registered nemesis classes.
- Resolution fills only the options a runner's model declares, so a `nemesis: {selector: …}` default does not reach a `FixedOrderMonkey`.

#### How it works

1. **Load.** Each layer (defaults, test case, overlays, env) merges into `nemesis` under the merge rules. The defaults and shared settings merge field by field; `runners` is replaced by a later layer that sets it, or extended with `["++", …]`. `defaults/test_default.yaml` declares a single `NoOpMonkey` runner, so a test without nemesis needs no `nemesis:` key.
2. **Validate.** Each runner entry is validated by its class's config model when it is assigned. An entry fails when the class is unknown or not a `NemesisRunner`, an option is unknown or missing, or a value is out of range. The error location carries the runner index and class. Each validated entry keeps a reference to its runner class.
3. **Resolve.** `resolve_runners()` returns one `(runner class, resolved config)` pair per runner. For each option the entry leaves unset, it takes the `nemesis:` default. A runner without a seed, own or default, gets a random one. Resolution does not change the stored entries, so a default changed by a later layer still reaches every runner.
4. **Run.** `sdcm/tester.py:get_nemesis_class` returns `resolve_runners()`, and `add_nemesis` in `sdcm/cluster.py` and `sdcm/cluster_cassandra.py` builds each runner as `runner_class(tester_obj, termination_event, config=runner_config)`. The runner reads `selector`, `seed`, `interval` and its own options from `config`, never from `params`.

#### Validators

- The selector/seed parity check (`_validate_nemesis_parallel_config`) and the `nemesis_selector` special case in `_validate_value` are deleted: one entry per runner leaves nothing to pair.
- The filter-seeds vs. seed-count check (`_validate_nemesis_can_run_on_non_seed`) stays on the root. It reads `seeds_num`, `n_db_nodes` and the root `add_node_cnt`, so it spans sections. The kept `NemesisConfigMixin` is a possible later home for it, since a mixin's `model_validator` can read other mixins' fields, but moving validators is outside this plan. It walks `resolve_runners()` and fails when any runner other than `NoOpMonkey` has an effective `filter_seeds` of true while `sum(n_db_nodes) + add_node_cnt` does not exceed `seeds_num`. Without it, every disruption that needs a non-seed node fails at runtime with "node allocation failed", after the cluster is up.
- The grow/shrink instance-type check stays on the root. It needs backend, regions and cloud API calls, so it spans sections. It reads `config.nemesis.grow_shrink.instance_type`.

## Implementation Phases

### Phase 1: Tooling

Everything the group conversions need, built in one PR. No option changes. A throwaway test section, declared on a throwaway mixin, exercises the machinery, in the same way `test_mixin_validators.py` uses throwaway mixins.

- **Section model and strict access.**
  - `ConfigSection` base class, in a new `sdcm/sct_config/sections/` package that holds the section models. A group's mixin declares its section field; the mixin stays the unit of `CONFIG_GROUPS`.
  - `get()`, `[]`, `in` and `update()` on the root keep their behaviour. A converted group's flat names disappear from the root, so a missed `get("<old name>")` returns `None`. The group PR guards against that with the repo-wide migration (Phase 2 DoD), not with a change to `get()`.
- **Layered loading.**
  - YAML and env load into sections and merge across layers under the merge rules, which also apply to the existing nested options.
  - `++` appending works on section fields, from YAML and from env.
  - A list or a whole section can be given from env as a YAML or JSON string (`SCT_NEMESIS`, `SCT_NEMESIS__RUNNERS`).
- **Name tables fail loudly.** Any table lookup of a name that is not a root field raises an error instead of being dropped. This fixes the silent drop in `_check_backend_defaults` before any group can trip it. The one stale entry in the tables, `aws_root_disk_name_monitor` in `BACKEND_REQUIRED_PARAMS` (twice in `defaults.py`, no such option exists), is removed in the same PR, since it would otherwise raise. Tables are not taught section paths: when a group converts, its entries leave the string tables and become attribute access or section validators.
- **Docs.** The docs generator (`config.py` doc generation, `sct.py help`, `docs/configuration_options/`) renders nested values:
  - A section field that `_fields_by_group` finds on a mixin is expanded into its sub-fields on that mixin's page. `_group_blurb` keeps reading the mixin's docstring.
  - Each section page lists its fields under the section name, with the prefix-free field name (`interval`, not `nemesis_interval`).
  - Each option shows its full YAML path (`nemesis.interval`), its nested env name (`SCT_NEMESIS__INTERVAL`) and a YAML example in the nested form.
  - Anchors and the alphabetical index key on the dotted path, so fields with the same name in different sections do not collide.
  - Defaults of section fields are shown per field, not as one dumped section value.
  - `test_help.py` is updated to the new heading format.
- **Migration script.** The script from the Design section exists, with tests on a sample YAML file.

**DoD:**
- [ ] A throwaway section is readable and writable through attribute access only, and every flat form raises.
- [ ] Root `get()`, `[]`, `in` and `update()` behave as before. Tests pin it, including `get()` returning `None` for an unknown name and a dotted lookup into a dict-typed option returning `None` for a missing sub-key.
- [ ] Tests cover defaults plus a partial user section (defaults retained), `++` on a section field, and the nested env form.
- [ ] Tests cover each merge rule on each kind of value: a dict option, a model-typed option (merged field by field), a section and a field nested inside it, a replaced list, `++` on a nested list, and a string form from env.
- [ ] Every file under `test-cases/` produces the same `sct.py conf` dump on this branch and on `master`.
- [ ] A test proves an unknown name in `BACKEND_REQUIRED_PARAMS` fails loudly.
- [ ] Every name in the tables in `defaults.py` and in `SIZING_ROLE_PARAMS` is a root field, which a test pins.
- [ ] A test on a throwaway section shows the generated docs with the dotted YAML path, nested env name and nested YAML example, and the index lists the dotted path.
- [ ] `docs/configuration_options/` is regenerated in the new format and committed, and the `update-conf-docs` hook then produces no further diff.
- [ ] Unit tests, `uv run sct.py pre-commit` and `uv run sct.py lint-pipelines` pass.

**Dependencies:** none.

### Phase 2: Pilot — `nemesis` section

Why nemesis goes first:
- It has 12 options and 21 consumer files outside `sdcm/sct_config/`.
- No Jenkins variables reference it.
- None of the parallel loaders reads its keys.
- Its tests still need options from outside the section. Nemesis code reads about 30 other options through `params`, among them `cluster_backend`, `use_mgmt`, `use_cloud_manager`, `use_zero_nodes`, `db_type`, `backup_bucket_*`, `n_db_nodes`, `n_loaders` and `test_duration`. Today the tests supply them through `MagicMock` params, `side_effect` lambdas and ad hoc dicts (`test_manager.py`, `test_sla.py`, `test_modify_table.py`, `fake_cluster.py`). The section replaces the faked nemesis options only. **Needs Investigation:** whether the outside options get a shared test helper, such as a real `SCTConfiguration` built from a minimal dict or a mock restricted to real option names, so that a misspelled outside option fails the test instead of returning a `MagicMock`.
- Its main problem is structural, so it shows what a redesign buys, not only a regroup. See [Nemesis section redesign](#nemesis-section-redesign).

#### Scope of the PR

The PR may be split into commits for review, but it lands as one change:

- **Section.**
  - `NemesisConfigMixin` stays in `CONFIG_GROUPS` with its docstring and `config_group`, and declares one field, `nemesis: NemesisConfig`. Its 12 flat fields move into `NemesisConfig(ConfigSection)` in `sdcm/sct_config/sections/nemesis.py`, reshaped as described in [Nemesis section redesign](#nemesis-section-redesign), with `NemesisRunnerConfig` and `CategoricalMonkeyConfig` next to their runners. `defaults/test_default.yaml` declares a single `NoOpMonkey` runner.
  - `sdcm.nemesis` imports `sdcm.cluster`, which imports `sdcm.sct_config` at module level (`SCTConfiguration`, `backend_to_cloud`), so the config cannot import the runner classes. The cycle is broken first: backend helpers move to `sdcm/utils/cloud_backends.py`, and modules that only annotate `SCTConfiguration` import it for type checking only.
- **Validators.** Changed as listed under the redesign's Validators.
- **Runners and consumers.**
  - `NemesisRunner`, `CategoricalMonkey` and `FixedOrderMonkey` take their resolved config as a required constructor argument and read every option from it. The selector/seed/dist kwargs and the flat-option fallbacks are removed. `FixedOrderMonkey` reads `order` instead of splitting the selector. No test case in the repo uses it yet, so only its unit tests and `docs/nemesis.md` change.
  - `get_nemesis_class` and both `add_nemesis` implementations follow the redesign's Run step.
  - Readers outside a runner switch to section attributes: the longevity tests and `sdcm/utils/loader_utils.py` (`during_prepare`), `sdcm/utils/operations_thread.py` (`seed`, taken from the first runner's resolved seed, as it takes the first entry of `nemesis_seed` today), `grow_cluster_test.py` (runner classes), `longevity_balancer_test.py` and the grow/shrink code (`grow_shrink.*`).
  - Performance tests read `interval` and no longer pass it to `start_nemesis`, so each runner keeps its own.
  - The AWS AZ resolver, GCE zone resolver and capacity reservation list `nemesis.grow_shrink.instance_type`, so it is still checked for AZ and zone availability and reserved. Entries for it leave their string tables.
- **Tests.**
  - The dict fakes in `unit_tests/unit/nemesis/` and `test_scan_operation_thread.py` construct `NemesisConfig`.
  - Tests that assign `params["nemesis_…"]` set `params.nemesis.<field>` instead.
- **YAML.**
  - `cdc-replication-longevity.yaml` declares its `CategoricalMonkey` distribution, keyed by nemesis class names (the method-style keys it used in Python never matched any disruption), and `cdc_replication_test.py` starts it through `get_nemesis_class`.
  - `template-nemesis-config.yaml.j2` emits the nested form, and `configurations/nemesis/` is regenerated with `sdcm/nemesis/generator.py`.
  - The migration script rewrites the remaining files: about 220 files under `test-cases/`, `configurations/`, `defaults/` and `unit_tests/test_configs/`. Parallel lists become one runner entry each.
  - Overlay rule: because an overlay that sets `runners` replaces the whole list, the 10 overlays under `configurations/` that only tune selector, seed, interval or `during_prepare` become `nemesis:` defaults, and the base test case carries those options as `nemesis:` defaults too, unless its runners differ. An overlay that must change a runner-specific option restates the whole `runners` list.
- **Docs and guards.**
  - In `test_option_groups.py`, group identity still comes from the mixin. The prefix rule for a converted group becomes "section fields carry no group prefix", and the test descends into the section's `model_fields` to check it, since those fields are not in the root `model_fields`.
  - The group's docs page is regenerated and shows the nested paths and sub-options.
  - `docs/nemesis.md` documents per-runner overrides and runner-specific options with a two-runner example, and how a runner adds its own config model.
  - The docs and skills that cite nemesis option names are updated: `AGENTS.md`, `README.md`, `.github/copilot-instructions.md`, and the `writing-nemesis`, `downscaling-for-minicloud`, `labeling-pipelines` and `migrate-to-sizing` skills.

**DoD:**
- [ ] `NemesisConfig(...)` validates alone. A test shows an invalid runner entry rejected without building the full configuration.
- [ ] Runners start only from `nemesis.runners`. A two-runner config with different intervals gives each runner its own interval, and a `CategoricalMonkey` distribution loads from YAML alone.
- [ ] `config.py` keeps only the grow/shrink instance-type check as nemesis validation.
- [ ] Every existing use of a `nemesis_*` option is migrated to `config.nemesis.<field>` or a `NemesisConfig` method: `get()` and `[]` literals (69 sites in 21 files at the time of the audit), dynamic keys, `side_effect` lambdas and `MagicMock` params in tests.
- [ ] A repo-wide grep for config access by an old flat name finds nothing outside `docs/plans/` and the backport note in the `fix-backport-conflicts` skill. Config access means `get()`/`[]` keys, YAML keys and `SCT_*` names; code identifiers such as the `nemesis_selector` argument of `add_nemesis` are not config access.
- [ ] No unit test fakes nemesis options with a plain dict. Options from other groups may still be supplied by the test.
- [ ] Docs and README sweep: every `docs/**/*.md`, `README*` and skill file that cites the section's old flat option names or env vars is updated, and `docs/configuration_options/` is regenerated and committed.
- [ ] The migration script's verification passes: every repo YAML loads to the same runners, selectors, seeds, intervals and nested values on `master` and on the branch. Unit tests and `uv run sct.py lint-pipelines` pass too.
- [ ] `scylla-pkg` and `qa-internal` have no reference to an old nemesis `SCT_*` name or YAML key, or a companion PR migrating them is ready to merge with this one. The search result is recorded in the PR description.
- [ ] A parallel-runner nemesis test case, downscaled with the `downscaling-for-minicloud` skill, runs on minicloud from config load through teardown, log collection and resource cleanup.

**Dependencies:** Phase 1.

## Testing Requirements

| Phase | Unit | Integration / lint | Manual |
|---|---|---|---|
| 1 | Throwaway section: attribute access works and every flat form raises; layered merge; `++`; nested env; loud name-table failure; migration script on a sample file | `lint-pipelines` | `sct.py conf` docker dump compared with `master` |
| 2 | `NemesisConfig` instantiates and validates alone; runner entry validation by runner class (model selection, missing, foreign and out-of-range option, unknown class) and default resolution, with a test-only runner hierarchy; loading from a config file, overlay merge, env override; runners read their options from the config they are given; grow/shrink instance-type check, filter-seeds vs. seed-count check with a per-runner `filter_seeds` override, AZ/zone resolvers and capacity reservation with the nested value; migration script on single-runner, parallel-runner and overlay files; `test_option_groups` rule for the group; no dict fakes | `lint-pipelines`; migration-script verification over every test case; repo-wide grep for old names; search of `scylla-pkg` and `qa-internal` for old names | Minicloud run of a parallel-runner test case through teardown and cleanup; artifact test; one single-runner and one parallel-nemesis longevity; `cdc-replication-longevity` (`CategoricalMonkey` from YAML); `perf-regression-latency-backup-nemesis` (reads `interval` outside a runner); one GrowShrink test (`add_node_cnt`, capacity reservation) |

Every PR runs `uv run sct.py unit-tests`, `uv run sct.py pre-commit` and `uv run sct.py lint-pipelines`.

## Success Criteria

1. Goals 1–6 hold for `nemesis`, which means the DoD of Phases 1–2 is met.
2. No file in the repository, apart from `docs/plans/` and the `fix-backport-conflicts` backport note, reads or sets a nemesis option by its old flat name (`get()`/`[]` keys, YAML keys, `SCT_*` names).
3. A test case declares two runners with different `interval` values, and a `CategoricalMonkey` distribution, in YAML alone.
4. Converting further groups needs no further tooling work.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A later layer replaces a whole section and defaults silently disappear | High without Phase 1 | High: tests run with wrong settings and no error | Field-by-field merging is a Phase 1 requirement with its own tests; the migration script's verification compares every test case with `master` |
| A name table refers to a moved option and the check stops running | High | High: required-param and image checks skipped silently | Phase 1 makes unknown names fail loudly before any group moves |
| A consumer still reads a converted option by its old name, through `get()`, a dict fake or a dynamic key | Medium | High: `get()` returns `None`, so the consumer silently runs with a default | `get()` is unchanged, so the guard is the migration: repo-wide grep per group and no dict fakes. For `nemesis` the audit lists 69 literal sites in 21 files, all migrated in Phase 2 |
| Something outside this repo sets an old `SCT_*` variable or YAML key: other repos' jobs, local developer environments, older test-id files | Medium | Low: fails with an unknown-option error, never runs with a wrong value; a post-test stage (log collection, resource cleanup) that sets an old name fails late | The failure is loud by design. Each group PR searches `scylla-pkg` and `qa-internal` for old names and migrates hits in a companion PR, runs a test case through teardown and cleanup on minicloud, and announces its renamed variables in the PR description and in the weekly commit summary |
| Argus reads a converted group's flat keys from `sct_config` | None for `nemesis`; checked per later group | Medium: broken run pages | The Argus backend's key list is in Current State; a group whose keys Argus reads ships the Argus change with its PR |
| Saved Argus dashboard filters on old flat keys (`sct_config.nemesis_interval`) stop matching without an error | Low | Low: a filter shows no runs | Each group PR announces its renamed keys |
| An overlay that sets `runners` silently replaces the whole list | Medium | Medium: runners differ from the base test case | Tuning options live in the `nemesis:` defaults, which merge field by field; the migration script writes known overlays as defaults only; its verification compares resolved runners with `master` |
| Backports to release branches touch a converted option | High | Low: conflicts in the backport PR | Release branches keep flat names. The `fix-backport-conflicts` skill gets a note mapping old names to new paths for converted groups |
| The YAML rewrite damages comments or formatting across hundreds of files | Medium | Low | The script preserves comments and order; its verification compares loaded values with `master`; `lint-pipelines` gates the PR |
| A model-typed option set in two layers now merges field by field instead of being replaced | Low | Low: a test runs with an extra key from an earlier layer | The Phase 1 `sct.py conf` comparison with `master` lists every test case whose value changes; each is reviewed in that PR |
| Merge conflicts with option additions on `master` | High | Low | One group per PR, rebased just before merge; `test_option_groups` forces new options into the right section |
