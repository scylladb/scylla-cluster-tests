# Implementation Plan: Resilient Test Configuration Dependencies

## Problem Statement

SCT test configurations rely on Jenkins pipelines to chain multiple YAML config files together via the `test_config` parameter. When shared settings are extracted from individual configs into a centralized file (e.g., `configurations/auth_cassandra.yaml`), the dependency is only expressed in the Jenkinsfile — not in the config itself. This creates several failure modes:

1. **Jenkins parameter caching**: Jenkins caches parameter defaults from the previous build. After updating a Jenkinsfile's `test_config`, Build #1 loads new parameter definitions but uses old cached defaults. Build #2 is the first to actually use the updated value ([JENKINS-41929](https://issues.jenkins-ci.org/browse/JENKINS-41929)).
2. **Manual runs**: Users running tests manually with `sct.py run-test --config` must know which additional configs to chain — no enforcement exists.
3. **Silent breakage**: Missing a required config (like auth) doesn't fail at config loading time — it fails deep into the test run (e.g., `Unauthorized` error when creating SLA roles).
4. **Refactoring risk**: Extracting shared settings into separate files (a good DRY practice) becomes dangerous because the dependency graph is implicit.

### Triggering Incident

Commit `866d18e70` consolidated authentication settings from 53 YAML configs into `configurations/auth_cassandra.yaml` and updated 149 Jenkinsfiles. The `rolling-upgrade-with-sla-no-shares` test broke because:
- Auth settings were removed from `generic-rolling-upgrade.yaml`
- `auth_cassandra.yaml` was added to the Jenkinsfile's `test_config`
- Jenkins used the cached (old) `test_config` parameter on the first run
- The test ran without authentication, causing `Unauthorized` errors when creating SLA roles

## Goals

1. Eliminate silent config dependency failures
2. Make config files self-documenting about their requirements
3. Prevent Jenkins parameter caching from causing stale configs
4. Support manual `sct.py run-test` runs without requiring users to know implicit dependencies
5. Maintain backward compatibility with existing configs and pipelines

---

## Option A: YAML Includes via anyconfig's Built-in Jinja2 Templates

### Description

SCT already uses `anyconfig` for YAML loading. anyconfig 0.14.0 has **built-in Jinja2 template support** via the `ac_template=True` parameter. This enables `{% include 'file.yaml' %}` directives in config files — no custom include logic needed.

### Verified

Tested against SCT configs:
- `configurations/rolling-upgrade-with-sla-no-shares.yaml` — works (no Jinja2 conflicts)
- `configurations/auth_cassandra.yaml` — works
- `defaults/test_default.yaml` (184 keys) — works
- **No existing SCT config files contain `{{` or `{%` syntax** that would conflict with Jinja2 template processing

### Example

```yaml
# configurations/rolling-upgrade-with-sla-no-shares.yaml
{% include 'configurations/auth_cassandra.yaml' %}

stress_before_upgrade: "cassandra-stress write ..."
service_level_shares: [null, null]
```

```yaml
# configurations/nemesis/additional_configs/sla_config.yaml
{% include 'configurations/auth_cassandra.yaml' %}

sla: true
stress_cmd: [...]
```

Code change (single line in `sct_config.py`):
```python
# Before:
files = anyconfig.load(list(config_files))

# After:
files = anyconfig.load(list(config_files), ac_template=True)
```

### Advantages

- **Near-zero custom code** — uses anyconfig's existing feature
- Configs are self-contained and self-documenting
- Works for manual runs, Jenkins runs, and any other entry point
- Jinja2 handles recursive includes and error reporting
- Full Jinja2 power available if needed (conditionals, variables, etc.)
- No new dependencies (Jinja2 is already installed — version 3.1.6)

### Disadvantages

- Jinja2 template syntax (`{% include %}`) is less intuitive than a YAML-native `includes:` key for YAML config authors
- If future configs contain `{{` or `{%` in values (unlikely but possible), they would be interpreted as Jinja2 — could use `{% raw %}...{% endraw %}` blocks to escape
- Include paths must be relative to the template search path (project root), not the including file
- Slightly less explicit than a YAML-native key — looks like template magic rather than config declaration

### Sub-option A1: Custom `includes` Key (Original Proposal)

If Jinja2 syntax is considered too foreign for YAML config files, the original proposal of a YAML-native `includes` key remains viable:

```yaml
# configurations/rolling-upgrade-with-sla-no-shares.yaml
includes:
  - configurations/auth_cassandra.yaml

stress_before_upgrade: "cassandra-stress write ..."
```

This requires ~50-80 lines of custom code in `sct_config.py` but provides cleaner YAML-native syntax.

### Sub-option A2: `pyyaml-include` Library

The [`pyyaml-include`](https://pypi.org/project/pyyaml-include/) library adds a `!include` YAML tag:

```yaml
# configurations/rolling-upgrade-with-sla-no-shares.yaml
auth: !include configurations/auth_cassandra.yaml

stress_before_upgrade: "cassandra-stress write ..."
```

However, this doesn't work well for SCT because:
- It includes as a nested value under a key, not as top-level merge
- Would require restructuring how configs are organized
- Adds a new dependency

### Execution Plan

#### Phase 1: Enable `ac_template=True` in Config Loader

**Entry criteria**: None

**Actions**:
1. In `sdcm/sct_config.py`, add `ac_template=True` to `anyconfig.load()` calls that process config files (lines ~3048, ~3056)
2. Set the template search path to the project root so `{% include %}` paths resolve correctly
3. Verify no existing configs break by running `sct.py lint-pipelines` on all pipelines

**Exit criteria**:
- [ ] `anyconfig.load()` calls use `ac_template=True`
- [ ] All existing pipelines pass linting (no Jinja2 conflicts)
- [ ] Configs without `{% include %}` work identically to before

#### Phase 2: Unit Tests

**Entry criteria**: Phase 1 complete

**Actions**:
1. Create test config fixtures in `unit_tests/test_data/`:
   - `config_with_include.yaml` — uses `{% include 'auth.yaml' %}`
   - `auth.yaml` — the included config
2. Write pytest tests in `unit_tests/test_config_includes.py`:
   - `test_jinja2_include_basic` — verify included values are present
   - `test_jinja2_include_override` — verify current file values override included ones
   - `test_jinja2_include_missing_file` — verify missing file raises clear error
   - `test_no_include_unchanged` — verify configs without includes work as before
   - `test_jinja2_raw_block` — verify `{% raw %}` escaping works for edge cases

**Exit criteria**:
- [ ] All tests pass
- [ ] Edge cases covered

#### Phase 3: Migrate Existing Configs

**Entry criteria**: Phase 2 complete

**Actions**:
1. Add `{% include 'configurations/auth_cassandra.yaml' %}` to configs that need auth:
   - `configurations/rolling-upgrade-with-sla-no-shares.yaml`
   - `configurations/rolling-upgrade-with-sla.yaml`
   - `configurations/nemesis/additional_configs/sla_config.yaml`
   - `configurations/rolling-upgrade-alternator.yaml`
   - Other configs identified from commit `866d18e70`
2. Verify with `sct.py lint-pipelines` that all pipelines still pass
3. Keep `auth_cassandra.yaml` in Jenkinsfile `test_config` arrays for backward compatibility (double-include is harmless — later values override)

**Exit criteria**:
- [ ] All configs that need auth self-include it
- [ ] `sct.py lint-pipelines` passes
- [ ] Manual `sct.py run-test --config` works without explicitly chaining auth

#### Phase 4: Documentation

**Entry criteria**: Phase 3 complete

**Actions**:
1. Update `docs/sct-configuration.md` with `{% include %}` usage, path resolution, and examples
2. Document `{% raw %}` escaping for edge cases

**Exit criteria**:
- [ ] Documentation covers syntax and examples

---

## Option B: Hardcode `test_config` in Pipeline Environment (Not a Parameter)

### Description

Move `test_config` from a Jenkins parameter (which gets cached) to a hardcoded value in the pipeline's environment or script block. Users can still override via `extra_environment_variables` but the default always comes from the Jenkinsfile source code.

### Example

```groovy
// Before (cached parameter):
string(defaultValue: "${pipelineParams.get('test_config', '')}",
       name: 'test_config')

// After (hardcoded, always fresh):
environment {
    SCT_CONFIG_FILES = "${pipelineParams.get('test_config', '')}"
}
```

### Advantages

- Simple change — no new concepts
- Eliminates Jenkins parameter caching for `test_config`
- Always uses the value from the current Jenkinsfile source

### Disadvantages

- Only fixes Jenkins caching — doesn't help manual runs
- Users lose ability to override `test_config` from Jenkins UI (may be desired or not)
- Doesn't make configs self-documenting
- Config dependencies remain implicit

### Execution Plan

#### Phase 1: Audit Pipeline Parameter Usage

**Entry criteria**: None

**Actions**:
1. Search all `vars/*.groovy` files for `test_config` parameter definitions
2. Identify which pipelines allow user override of `test_config` and whether that's actually used
3. Document findings

**Exit criteria**:
- [ ] List of all pipeline files with `test_config` parameter
- [ ] Assessment of whether user override is needed per pipeline

#### Phase 2: Move `test_config` to Environment

**Entry criteria**: Phase 1 complete, confirmed that user override is rarely needed

**Actions**:
1. In each `vars/*.groovy` pipeline file:
   - Remove `test_config` from `parameters {}` block
   - Add `SCT_CONFIG_FILES = "${pipelineParams.get('test_config', '')}"` to `environment {}` block
2. If user override is needed for some pipelines, keep the parameter but merge with the hardcoded value:
   ```groovy
   def final_config = params.test_config ?: pipelineParams.get('test_config', '')
   ```

**Exit criteria**:
- [ ] `test_config` is no longer a cached parameter in affected pipelines
- [ ] Pipeline runs use the Jenkinsfile's value, not a cached default

#### Phase 3: Testing

**Entry criteria**: Phase 2 complete

**Actions**:
1. Trigger affected pipelines and verify `test_config` uses the correct value on Build #1
2. Verify that any remaining user-override paths work correctly

**Exit criteria**:
- [ ] Build #1 uses correct `test_config` value
- [ ] No regressions in pipeline behavior

---

## Option C: Config Validation at Load Time (Required Keys Per Feature)

### Description

Add validation rules to SCT config that check for required settings when certain features are used. For example: if `service_level_shares` is set, require `authenticator: PasswordAuthenticator`.

### Example

```python
# In SCTConfiguration validation
CONFIG_REQUIREMENTS = {
    "service_level_shares": {
        "requires": {"authenticator": "PasswordAuthenticator"},
        "error": "SLA features require PasswordAuthenticator. Add configurations/auth_cassandra.yaml to your config."
    },
    "sla": {
        "requires": {"authenticator": "PasswordAuthenticator"},
        "error": "SLA features require PasswordAuthenticator."
    },
}
```

### Advantages

- Fails fast at config load time with a clear error message
- Catches missing dependencies regardless of how the test is launched
- Self-documenting via the requirement rules
- Can be extended to other feature dependencies

### Disadvantages

- Reactive — detects the problem but doesn't fix it automatically
- Requires maintaining a rules table
- May produce false positives if requirements change across Scylla versions
- Doesn't address Jenkins caching (user still sees the failure, just earlier)

### Execution Plan

#### Phase 1: Define Requirement Rules

**Entry criteria**: None

**Actions**:
1. Survey all known config dependencies:
   - SLA features → `PasswordAuthenticator`
   - LDAP features → `authenticator` + `ldap_*` settings
   - Encryption at rest → `kms_*` settings
   - Alternator auth → `authenticator` settings
2. Define rules as a dict in `sdcm/sct_config.py`
3. Implement `_validate_config_requirements()` method

**Exit criteria**:
- [ ] Rules table covers known dependencies
- [ ] Validation method implemented

#### Phase 2: Integrate Validation

**Entry criteria**: Phase 1 complete

**Actions**:
1. Call `_validate_config_requirements()` during `SCTConfiguration.__init__()` after all config files are merged
2. Raise `ConfigurationError` with clear message including the fix (e.g., "Add `configurations/auth_cassandra.yaml` to your config")
3. Add to `sct.py lint-pipelines` validation path as well

**Exit criteria**:
- [ ] Missing requirements fail at config load time
- [ ] Error message tells the user exactly what to add
- [ ] `lint-pipelines` catches the issue

#### Phase 3: Unit Tests

**Entry criteria**: Phase 2 complete

**Actions**:
1. Test that SLA config without auth raises error
2. Test that SLA config with auth passes
3. Test that unrelated configs are not affected

**Exit criteria**:
- [ ] Tests cover all defined rules
- [ ] No false positives on existing valid configs

---

## Option D: `requires` Directive in YAML Configs (Lightweight Dependency Declaration)

### Description

Similar to Option A's `includes`, but instead of auto-loading the required config, it only **validates** that the required settings are present in the merged config. This is a lighter-weight approach that doesn't change the config loading mechanism.

### Example

```yaml
# configurations/rolling-upgrade-with-sla-no-shares.yaml
requires:
  - authenticator: PasswordAuthenticator
  - authenticator_user  # just check presence, any value

stress_before_upgrade: "cassandra-stress write ..."
service_level_shares: [null, null]
```

### Advantages

- Simpler than full `includes` — no merge order complexity
- Configs declare what they need
- Fails fast with clear error
- No recursive loading

### Disadvantages

- Doesn't auto-fix the problem — user must manually add the missing config
- Still requires the Jenkinsfile (or user) to chain the right configs
- Less powerful than `includes`

### Execution Plan

#### Phase 1: Implement `requires` Validation

**Entry criteria**: None

**Actions**:
1. Modify config loader to extract `requires` key from each config file
2. After all configs are merged, validate that all `requires` conditions are met
3. Raise error with clear message if not

**Exit criteria**:
- [ ] `requires` key is processed and removed before config validation
- [ ] Missing requirements produce clear error at load time

#### Phase 2: Add `requires` to Configs That Need Auth

**Entry criteria**: Phase 1 complete

**Actions**:
1. Add `requires` to configs that depend on authentication
2. Test with `sct.py lint-pipelines`

**Exit criteria**:
- [ ] All SLA/auth-dependent configs have `requires` declarations
- [ ] Linting catches missing dependencies

---

## Option E: Auto-Include Auth Based on Config Content

### Description

Have the SCT config loader automatically detect when authentication is needed (based on config values like `service_level_shares`, `sla: true`, `<sla credentials>` in stress commands) and auto-include `configurations/auth_cassandra.yaml` if no authenticator is configured.

### Example

```python
# In SCTConfiguration, after loading all config files:
def _auto_include_auth_if_needed(self):
    needs_auth = (
        self.get("service_level_shares")
        or self.get("sla")
        or any("<sla credentials" in str(cmd) for cmd in self._get_all_stress_cmds())
    )
    if needs_auth and self.get("authenticator") != "PasswordAuthenticator":
        self._load_config_file("configurations/auth_cassandra.yaml")
```

### Advantages

- Zero config changes needed — fully automatic
- No new YAML keys to learn
- Works for all entry points (Jenkins, manual, etc.)
- Self-healing — detects and fixes the missing dependency

### Disadvantages

- Magic behavior — implicit auto-loading can be surprising
- Hard to extend to other dependency types
- Tight coupling between feature detection logic and config files
- May mask configuration errors that should be surfaced

### Execution Plan

#### Phase 1: Implement Auto-Detection

**Entry criteria**: None

**Actions**:
1. Add `_auto_include_auth_if_needed()` to `SCTConfiguration`
2. Call it after all explicit config files are loaded but before validation
3. Log a warning when auto-including so it's visible

**Exit criteria**:
- [ ] Auth is auto-included when SLA features are detected
- [ ] Warning is logged
- [ ] No effect when auth is already configured

#### Phase 2: Testing

**Entry criteria**: Phase 1 complete

**Actions**:
1. Test SLA config without auth — verify auto-include
2. Test SLA config with auth — verify no double-include
3. Test non-SLA config — verify no auto-include

**Exit criteria**:
- [ ] All cases handled correctly

---

## Recommendation

| Option | Fixes Jenkins Caching | Fixes Manual Runs | Self-Documenting | Complexity | Recommended |
|--------|:---------------------:|:-----------------:|:----------------:|:----------:|:-----------:|
| A: anyconfig `{% include %}` | Yes | Yes | Yes | **Very Low** (1-line code change) | **Yes** |
| A1: Custom `includes` key | Yes | Yes | Yes | Medium | Alternative to A |
| B: Hardcode env | Yes | No | No | Low | Partial |
| C: Validation rules | No (fails faster) | Yes (fails faster) | Partially | Low | Complementary |
| D: `requires` | No (fails faster) | Yes (fails faster) | Yes | Low | Alternative to C |
| E: Auto-include | Yes | Yes | No (magic) | Medium | No |

**Primary recommendation: Option A (anyconfig `{% include %}`) + Option C (validation rules)**

- **Option A** is nearly free — anyconfig already supports it, Jinja2 is already installed, and no existing configs conflict. The code change is a single `ac_template=True` parameter. This solves the root cause: configs declare their own dependencies.
- **Option C** adds defense-in-depth — even if someone forgets `{% include %}`, the validation catches it at load time with a clear message instead of failing 2 hours into a test run.
- **Option B** is a quick win for Jenkins specifically but doesn't address the broader problem.
- **Option E** is too magical and would be hard to maintain.

### Suggested Implementation Order

1. **Option A first** (1 day) — the code change is trivial (`ac_template=True`), the migration is mechanical (add `{% include %}` lines to configs)
2. **Option C second** (1-2 days) — defense-in-depth for cases where includes are forgotten
3. **Option B optionally** — if Jenkins caching remains a problem for other parameters beyond `test_config`

---

## References

- Triggering commit: `866d18e70` (consolidate auth settings)
- Jenkins caching bug: [JENKINS-41929](https://issues.jenkins-ci.org/browse/JENKINS-41929)
- Affected test: `rolling-upgrade-with-sla-no-shares`
- Config loader: `sdcm/sct_config.py`
- Pipeline: `vars/rollingUpgradePipeline.groovy`
- Auth config: `configurations/auth_cassandra.yaml`
