---
status: draft
domain: config
created: 2026-03-11
last_updated: 2026-04-12
owner: null
---
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

### Real-World Composition Patterns in SCT Jenkinsfiles

SCT Jenkins pipelines compose configs in two fundamentally different ways. Option A (includes) only fits one of them.

#### Pattern 1: Hard Dependencies — Include Fits

Some configs are always paired across every Jenkinsfile that uses a given test case. These are true dependencies — the test cannot run without them.

**Example:** `longevity-200GB-48h-verifier-LimitedMonkey-tls.yaml` appears in 14+ Jenkinsfiles and **every single one** includes `auth_cassandra.yaml`:

```
longevity-200gb-48h.jenkinsfile:            ["...-tls.yaml", "auth_cassandra.yaml"]
longevity-200gb-48h-gce.jenkinsfile:        ["...-tls.yaml", "auth_cassandra.yaml"]
longevity-200gb-48h-azure.jenkinsfile:      ["...-tls.yaml", "auth_cassandra.yaml"]
longevity-200gb-48h-arm.jenkinsfile:        ["...-tls.yaml", "arm_instance_types/...", "auth_cassandra.yaml"]
longevity-200gb-48h-rf1.jenkinsfile:        ["...-tls.yaml", "rf1-non-disruptive.yaml", "auth_cassandra.yaml"]
longevity-200gb-48h-asymmetric.jenkinsfile: ["...-tls.yaml", "db-nodes-shards-random.yaml", "auth_cassandra.yaml"]
features-ldap-authorization-200gb.jenkinsfile: ["...-tls.yaml", "ldap-authorization.yaml", "auth_cassandra.yaml"]
```

Here, `{% include 'configurations/auth_cassandra.yaml' %}` inside the test case YAML is correct — auth is not optional, it's part of the test design.

**Rule:** Include in the test YAML if the dependency is present across **all** Jenkinsfiles using that test case.

**Additional Hard Dependencies Discovered:**

**LDAP configs → `auth_cassandra.yaml` (22/22 Jenkinsfiles = 100%)**

Every LDAP config (`ldap-authorization.yaml`, `ldap-authenticator.yaml`, `ldap-authorization-and-authentication.yaml`, `ms-ad-ldap-authenticator.yaml`, `ms-ad-ldap-authorization.yaml`, `ms-ad-ldap-authorization-and-authentication.yaml`) always appears with `auth_cassandra.yaml`. The LDAP configs only set `use_ldap: true`, `ldap_server_type`, and `use_ldap_authorization/authentication` — they don't set the authenticator or credentials that LDAP requires to function.

```
features-ldap-authorization-200gb.jenkinsfile:     ["...-tls.yaml", "ldap-authorization.yaml", "auth_cassandra.yaml"]
features-ldap-authentication-200gb.jenkinsfile:    ["...-tls.yaml", "ldap-authenticator.yaml", "auth_cassandra.yaml"]
features-ldap-ms-ad-authorization.jenkinsfile:     ["...-tls.yaml", "ms-ad-ldap-authorization.yaml", "auth_cassandra.yaml"]
```

**`sla_config.yaml` → `auth_cassandra.yaml` (30/30 Jenkinsfiles = 100%)**

Every Jenkinsfile using `sla_config.yaml` also chains `auth_cassandra.yaml`. SLA features require `PasswordAuthenticator` to create service levels and roles. This is also confirmed by the nemesis `additional_configs` declarations (see Pattern 5 below).

**`rolling-upgrade-with-sla*.yaml` → `auth_cassandra.yaml` (3/3 Jenkinsfiles = 100%)**

Both SLA rolling upgrade configs always pair with auth:

```
rolling-upgrade-with-sla.jenkinsfile:           ["rolling-upgrade-with-sla.yaml", "auth_cassandra.yaml", ...]
rolling-upgrade-with-sla-no-shares.jenkinsfile: ["rolling-upgrade-with-sla-no-shares.yaml", "auth_cassandra.yaml", ...]
```

**`generic-rolling-upgrade.yaml` → `auth_cassandra.yaml` + `rolling-upgrade-artifacts.yaml` (6/6 = 100%, 3-way)**

Every rolling upgrade pipeline chains all three files:

```
rolling-upgrade-*.jenkinsfile: ["generic-rolling-upgrade.yaml", "rolling-upgrade-artifacts.yaml", "auth_cassandra.yaml", ...]
```

**Alternator auth configs → `auth_cassandra.yaml` (18/18 Jenkinsfiles = 100%)**

All alternator Jenkinsfiles include `auth_cassandra.yaml`. Additionally, `configurations/alternator/enforce-authorization.yaml` and `configurations/rolling-upgrade-alternator.yaml` **duplicate** auth settings inline (authenticator, user, password, authorizer) rather than relying on the chained config. These are dedup candidates via `{% include %}`.

**`object_storage.yaml` + `object_storage_*_method.yaml` (10/10 Jenkinsfiles = 100%)**

Every Jenkinsfile using `object_storage.yaml` also includes either `object_storage_native_method.yaml` or `object_storage_rclone_method.yaml`. The base config sets S3 endpoints; the method file selects the backup transport. However, the method choice varies per pipeline, so the method configs are orthogonal to each other (Pattern 2). The base → at-least-one-method relationship is a **pair dependency** best handled by validation (Option C) rather than include.

**Performance profile pairs (100% co-occurrence)**

Performance test base configs always pair with specific profile overlays:
- `perf-regression-predefined-throughput-steps.yaml` + `latency-decorator-error-thresholds-steps-ent-vnodes.yaml` (10/10)
- `perf-regression-predefined-throughput-steps.yaml` + `cassandra_stress_gradual_load_steps_enterprise.yaml` (8/8)

These profile pairs could use `{% include %}` to self-document the dependency.

#### Pattern 2: Orthogonal Feature Composition — Include Does NOT Fit

The second pattern applies optional, orthogonal capabilities to a base test. Different Jenkinsfiles select different combinations:

**Example:** `longevity-200GB-48h-network-monkey.yaml` with varying overlays:

```
tier2:   ["network-monkey.yaml", "two_interfaces.yaml", "auth_cassandra.yaml"]
raft:    ["network-monkey.yaml", "raft/enable_raft_experimental.yaml", "two_interfaces.yaml", "auth_cassandra.yaml"]
vnodes:  ["network-monkey.yaml", "two_interfaces.yaml", "tablets_disabled.yaml", "auth_cassandra.yaml"]
```

The optional overlays are tiny 1-3 line files that toggle features:
- `raft/enable_raft_experimental.yaml` → `experimental_features: [consistent-topology-changes]`
- `tablets_disabled.yaml` → `append_scylla_yaml: {enable_tablets: false, ...}`
- `db-nodes-shards-random.yaml` → `db_nodes_shards_selection: "random"`

These **cannot** live in the base test case via include because different Jenkinsfiles apply different combinations.

**Rule:** Keep in Jenkinsfile if the config is an optional feature flag applied selectively.

#### Pattern 3: OS/Distro-Specific Overlays — Include Does NOT Fit

The same base test runs with different distro-specific configs:

```
debian12-manager-install.jenkinsfile: ["manager-installation-set-distro.yaml", "manager/debian12.yaml"]
rocky9-manager-install.jenkinsfile:   ["manager-installation-set-distro.yaml", "manager/rocky9.yaml"]
ubuntu24-manager-install.jenkinsfile: ["manager-installation-set-distro.yaml", "manager/ubuntu24.yaml"]
```

Each distro file is 3-4 lines (AMI ID, SSH user, monitor branch). These are mutually exclusive choices — one per Jenkinsfile.

#### Pattern 4: Complex Multi-Layer Composition (up to 5 configs)

The most complex case found chains 5 configs:

```
# performance staging with all optimizations
test_config: ["perf-regression-predefined-throughput-steps.yaml",
              "cassandra_stress_gradual_load_steps.yaml",
              "disable_kms.yaml",
              "tablets_disabled.yaml",
              "disable_speculative_retry.yaml"]
```

Breakdown: base test + load profile + 3 independent feature toggles. Only the load profile could be an include candidate (it always pairs with this test). The feature toggles are optional and vary by pipeline.

#### Pattern 5: Nemesis `additional_configs` — Existing Code-Level Dependency Declaration

The nemesis generator (`sdcm/nemesis/monkey/__init__.py`) already has a config dependency system via `additional_configs` class attributes on monkey classes. This is **prior art** that validates the concept of declaring config dependencies at the point of use:

```python
# sdcm/nemesis/monkey/__init__.py examples:
class SlaMonkey(NemesisBaseClass):
    additional_configs = ["sla_config.yaml", "auth_cassandra.yaml"]

class LdapAuthorizationMonkey(NemesisBaseClass):
    additional_configs = ["ldap-authorization.yaml", "auth_cassandra.yaml"]

class NetworkMonkey(NemesisBaseClass):
    additional_configs = ["two_interfaces.yaml"]
```

The generator (`sdcm/nemesis/generator.py`, line 127) loads these at runtime. The pattern confirms:
- SLA nemeses → `sla_config.yaml` + `auth_cassandra.yaml` (6 monkey classes)
- LDAP nemeses → `ldap-authorization.yaml` + `auth_cassandra.yaml` (2 monkey classes)
- Network nemeses → `two_interfaces.yaml` (3 monkey classes)

This is a parallel dependency mechanism that complements the config-level approach. Any solution (Option A/C/D) should be consistent with these existing declarations.

#### Pattern 6: Configs That Duplicate Dependencies Inline (Dedup Opportunity)

Some configs duplicate `auth_cassandra.yaml` settings inline rather than relying on Jenkinsfile chaining. These are prime candidates for `{% include %}` to eliminate duplication:

- **`configurations/alternator/enforce-authorization.yaml`** — duplicates `authenticator`, `authenticator_user`, `authenticator_password`, `authorizer` inline, then adds alternator-specific settings
- **`configurations/rolling-upgrade-alternator.yaml`** — duplicates the same auth block with a comment: _"NOTE: authenticator/authorizer settings below are just to ease the test case lint and the validation code"_

After implementing Option A, these files can replace the inline auth block with `{% include 'configurations/auth_cassandra.yaml' %}`, reducing duplication and ensuring consistency.

#### Classification Summary

| Config Type | Example | Include? | Reason |
|------------|---------|:--------:|--------|
| Auth for SLA/TLS tests | `auth_cassandra.yaml` in all TLS tests | **Yes** | Always required, present in every variant |
| LDAP configs → auth | `ldap-authorization.yaml` → `auth_cassandra.yaml` | **Yes** | 22/22 Jenkinsfiles; LDAP requires authenticator |
| SLA config → auth | `sla_config.yaml` → `auth_cassandra.yaml` | **Yes** | 30/30 Jenkinsfiles; SLA requires PasswordAuthenticator |
| SLA rolling upgrade → auth | `rolling-upgrade-with-sla*.yaml` → `auth_cassandra.yaml` | **Yes** | 3/3 Jenkinsfiles; always paired |
| Rolling upgrade → auth + artifacts | `generic-rolling-upgrade.yaml` → `auth_cassandra.yaml` + `rolling-upgrade-artifacts.yaml` | **Yes** | 6/6 Jenkinsfiles; 3-way hard dependency |
| Alternator auth (inline dup) | `alternator/enforce-authorization.yaml` duplicates auth | **Yes** | Dedup via include; 18/18 Jenkinsfiles chain auth |
| Perf profile pairs | `perf-regression-*.yaml` + load profile | **Yes** | 8-10/8-10 per pair; always co-occur |
| Object storage pair | `object_storage.yaml` + `*_method.yaml` | Validate | 10/10 co-occur but method varies; use Option C |
| Feature toggles | `tablets_disabled.yaml`, `raft/enable_raft_experimental.yaml` | No | Optional, varies by pipeline |
| OS/distro selection | `manager/debian12.yaml`, `manager/ubuntu24.yaml` | No | Mutually exclusive choice |
| Instance/storage type | `arm_instance_types/*.yaml`, `ebs/*.yaml` | No | Infrastructure choice per pipeline |
| Network config | `two_interfaces.yaml` | Depends | Include if always paired; Jenkinsfile if optional |
| Load profile | `cassandra_stress_gradual_load_steps.yaml` | Depends | Include if 1:1 with base test; Jenkinsfile if shared |
| KMS encryption | `kms-ear.yaml` | No | Only 2/8 Jenkinsfiles pair with auth; orthogonal |

#### Merge Order Consideration

`auth_cassandra.yaml` contains `scylla_network_config` (list type). When both auth and network configs are chained:

```
[auth_cassandra.yaml, two_interfaces.yaml]  →  two_interfaces.yaml overrides network config
```

If the test case includes `auth_cassandra.yaml` inline and the Jenkinsfile adds `two_interfaces.yaml`, the network settings from auth get overridden — which is the **current intended behavior**. Includes must load **before** Jenkinsfile-provided configs in the merge order. The `anyconfig` list merge behavior (replace, not append) must be verified for double-include scenarios.

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

```yaml
# configurations/ldap-authorization.yaml
{% include 'configurations/auth_cassandra.yaml' %}

use_ldap: true
ldap_server_type: 'openldap'
use_ldap_authorization: true
```

```yaml
# configurations/alternator/enforce-authorization.yaml (BEFORE — inline duplication)
append_scylla_yaml:
  authenticator: PasswordAuthenticator
  authorizer: CassandraAuthorizer
authenticator_user: cassandra
authenticator_password: cassandra
alternator_enforce_authorization: true
alternator_access_key_id: alternator_access_key
alternator_secret_access_key: alternator_secret_key

# AFTER — dedup via include
{% include 'configurations/auth_cassandra.yaml' %}

alternator_enforce_authorization: true
alternator_access_key_id: alternator_access_key
alternator_secret_access_key: alternator_secret_key
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
- **Only suitable for hard dependencies** — orthogonal feature composition must remain in Jenkinsfiles

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

#### Phase 3: Migrate Existing Configs (Hard Dependencies Only)

**Entry criteria**: Phase 2 complete

**Scope**: Only configs where the dependency is present in **all** Jenkinsfiles using that test case (Pattern 1 — hard dependencies). Orthogonal feature toggles, OS/distro overlays, and infrastructure choices remain in Jenkinsfiles.

**Actions**:
1. Audit commit `866d18e70` to classify each of the 53 affected configs as hard dependency vs. orthogonal composition (see Classification Summary above)
2. Add `{% include 'configurations/auth_cassandra.yaml' %}` to hard dependency configs. Full list of candidates:

   **SLA configs (auth always required):**
   - `configurations/nemesis/additional_configs/sla_config.yaml`
   - `configurations/rolling-upgrade-with-sla.yaml`
   - `configurations/rolling-upgrade-with-sla-no-shares.yaml`

   **LDAP configs (auth always required):**
   - `configurations/ldap-authorization.yaml`
   - `configurations/ldap-authenticator.yaml`
   - `configurations/ldap-authorization-and-authentication.yaml`
   - `configurations/ms-ad-ldap-authenticator.yaml`
   - `configurations/ms-ad-ldap-authorization.yaml`
   - `configurations/ms-ad-ldap-authorization-and-authentication.yaml`

   **Rolling upgrade configs (auth always required):**
   - `test-cases/upgrades/generic-rolling-upgrade.yaml` (also include `rolling-upgrade-artifacts.yaml`)

   **Alternator configs (replace inline auth duplication with include):**
   - `configurations/alternator/enforce-authorization.yaml` — remove duplicated auth block, add `{% include 'configurations/auth_cassandra.yaml' %}`
   - `configurations/rolling-upgrade-alternator.yaml` — remove duplicated auth block, add include

   **Performance profile pairs (include the load profile in the base test):**
   - Performance base configs that always pair with a specific load profile (to be determined per pair)

3. Verify `anyconfig` list merge behavior for double-include scenarios (auth's `scylla_network_config` is a list — confirm later Jenkinsfile configs override correctly)
4. Verify with `sct.py lint-pipelines` that all pipelines still pass
5. Keep `auth_cassandra.yaml` in Jenkinsfile `test_config` arrays for backward compatibility during migration
6. In a follow-up, remove now-redundant `auth_cassandra.yaml` entries from Jenkinsfiles where the include makes them unnecessary

**Exit criteria**:
- [ ] Hard-dependency configs self-include their requirements
- [ ] Orthogonal composition configs are unchanged (remain Jenkinsfile-driven)
- [ ] Double-include merge order verified for list-type values
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
    # --- SLA / Service Level ---
    "service_level_shares": {
        "requires": {"authenticator": "PasswordAuthenticator"},
        "error": "SLA features require PasswordAuthenticator. Add configurations/auth_cassandra.yaml to your config."
    },
    "sla": {
        "requires": {"authenticator": "PasswordAuthenticator"},
        "error": "SLA features require PasswordAuthenticator."
    },

    # --- LDAP ---
    "use_ldap": {
        "requires": {"ldap_server_type": ["openldap", "ms_ad"]},
        "error": "LDAP requires ldap_server_type to be set ('openldap' or 'ms_ad')."
    },
    "use_ldap_authorization": {
        "requires": {"use_ldap": True, "authorizer": "CassandraAuthorizer"},
        "error": "LDAP authorization requires use_ldap=true and authorizer=CassandraAuthorizer."
    },
    "use_ldap_authentication": {
        "requires": {"use_ldap": True},
        "error": "LDAP authentication requires use_ldap=true and a compatible authenticator."
    },

    # --- Alternator ---
    "alternator_enforce_authorization": {
        "requires": {"authenticator": "PasswordAuthenticator", "alternator_access_key_id": "<non-empty>"},
        "error": "Alternator authorization requires PasswordAuthenticator and alternator_access_key_id/secret."
    },

    # --- Object Storage (custom logic) ---
    # If backup_bucket_location is set, backup_bucket_backend must also be set.
    # Enforces that object_storage.yaml is always paired with a method config.

    # --- Encryption at Rest (custom logic) ---
    # If scylla_encryption_options contains KmsKeyProviderFactory, server_encrypt must be true.

    # --- Manager Backup (custom logic, AWS-specific) ---
    # If backup_bucket_location is set and cluster_backend=='aws',
    # aws_instance_profile_name_db should be set for S3 access.
}
```

#### Existing Validations Already in Code

The following dependencies are **already enforced** in `sdcm/sct_config.py` and do not need new rules:

| Trigger | Requirement | Code Location |
|---------|-------------|---------------|
| `authenticator: PasswordAuthenticator` | `authenticator_user` + `authenticator_password` must be set | Lines 2849-2856 |
| `alternator_enforce_authorization: true` | `authenticator` + `authorizer` must be defined | Lines 2858-2862 |
| `k8s_enable_sni: true` | `k8s_enable_tls: true` required | Lines 2957-2958 |
| `simulated_regions > 1` | Endpoint snitch auto-set to GossipingPropertyFileSnitch | Lines 2897-2910 |
| `data_volume_disk_num > 0` | `data_volume_disk_type` + `data_volume_disk_size` required | `_verify_data_volume_configuration()` |
| `perf_gradual_throttle_steps` | `perf_gradual_threads` must match | Lines 2977-3009 |

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
   - SLA features → `PasswordAuthenticator` + credentials
   - SLA `service_level_shares` array length → must match `<sla credentials N>` count in stress commands
   - LDAP features → `authenticator` + `ldap_server_type` + (optionally `prepare_saslauthd`)
   - LDAP authorization → `use_ldap: true` + `authorizer: CassandraAuthorizer`
   - LDAP authentication → `use_ldap: true` + compatible authenticator (SaslauthdAuthenticator)
   - Alternator auth → `authenticator` + `alternator_access_key_id` + `alternator_secret_access_key`
   - Encryption at rest (KMS) → `server_encrypt: true` when KmsKeyProviderFactory is in options
   - Object storage → `backup_bucket_backend` must be set when `backup_bucket_location` is set
   - Manager backup on AWS → `aws_instance_profile_name_db` should be set
2. Define rules as a dict in `sdcm/sct_config.py`
3. Implement `_validate_config_requirements()` method
4. For complex dependencies (KMS, object storage, manager backup), implement custom validation methods

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
3. Test that LDAP config without `ldap_server_type` raises error
4. Test that LDAP authorization without `use_ldap: true` raises error
5. Test that alternator auth without credentials raises error
6. Test that object storage without method config raises error
7. Test that unrelated configs are not affected
8. Test that all existing valid pipeline configs pass validation (regression)

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

## Appendix: Complete Config Dependency Map

This appendix catalogs all discovered config interdependencies across SCT, organized by enforcement status.

### Already Enforced in Code (`sdcm/sct_config.py`)

| # | Trigger | Requirement | Method |
|---|---------|-------------|--------|
| 1 | `authenticator: PasswordAuthenticator` | `authenticator_user` + `authenticator_password` non-empty | Direct check (L2849-2856) |
| 2 | `alternator_enforce_authorization: true` | `authenticator` + `authorizer` defined | Direct check (L2858-2862) |
| 3 | `k8s_enable_sni: true` | `k8s_enable_tls: true` | Direct check (L2957-2958) |
| 4 | `simulated_regions > 1` | Snitch auto-set to GossipingPropertyFileSnitch | Auto-fix (L2897-2910) |
| 5 | `data_volume_disk_num > 0` | `data_volume_disk_type` + `data_volume_disk_size` | `_verify_data_volume_configuration()` |
| 6 | `perf_gradual_throttle_steps` | `perf_gradual_threads` must match keys/count | Direct check (L2977-3009) |
| 7 | `use_mgmt: true` + docker backend | Warns/validates docker limitations | `_validate_docker_backend_parameters()` |

### Implicit — Need New Validation Rules (Option C Candidates)

| # | Trigger | Requirement | Evidence | Priority |
|---|---------|-------------|----------|----------|
| 1 | `sla: true` or `service_level_shares` set | `authenticator: PasswordAuthenticator` | 30/30 Jenkinsfiles; nemesis `additional_configs` | **High** |
| 2 | `service_level_shares` array | Length must match `<sla credentials N>` in stress cmds | `longevity-sla-100gb-4h.yaml` | **High** |
| 3 | `use_ldap: true` | `ldap_server_type` set to 'openldap' or 'ms_ad' | 22/22 Jenkinsfiles; `ldap-authenticator.yaml` | **High** |
| 4 | `use_ldap_authorization: true` | `use_ldap: true` + `authorizer: CassandraAuthorizer` | `ldap-authorization.yaml` | **High** |
| 5 | `use_ldap_authentication: true` | `use_ldap: true` + `authenticator` compatible | `ldap-authenticator.yaml` | **High** |
| 6 | `alternator_enforce_authorization: true` | `alternator_access_key_id` + `alternator_secret_access_key` | `enforce-authorization.yaml` | **High** |
| 7 | `backup_bucket_location` set | `backup_bucket_backend` must be set | 10/10 object_storage Jenkinsfiles | **Medium** |
| 8 | KmsKeyProviderFactory in `scylla_encryption_options` | `server_encrypt: true` | `kms-ear.yaml` | **Medium** |
| 9 | `backup_bucket_location` + `cluster_backend: aws` | `aws_instance_profile_name_db` should be set | Manager test-cases | **Low** |
| 10 | Gemini/Harry stress tools | `n_test_oracle_db_nodes >= 1` | `gemini/*.yaml`, `cdc/*.yaml` | **Low** |

### Hard Dependencies — Include Candidates (Option A)

| # | Config File | Must Include | Jenkinsfile Evidence |
|---|-------------|-------------|---------------------|
| 1 | `configurations/nemesis/additional_configs/sla_config.yaml` | `auth_cassandra.yaml` | 30/30 |
| 2 | `configurations/rolling-upgrade-with-sla.yaml` | `auth_cassandra.yaml` | 3/3 |
| 3 | `configurations/rolling-upgrade-with-sla-no-shares.yaml` | `auth_cassandra.yaml` | 3/3 |
| 4 | `configurations/ldap-authorization.yaml` | `auth_cassandra.yaml` | 22/22 |
| 5 | `configurations/ldap-authenticator.yaml` | `auth_cassandra.yaml` | 22/22 |
| 6 | `configurations/ldap-authorization-and-authentication.yaml` | `auth_cassandra.yaml` | 22/22 |
| 7 | `configurations/ms-ad-ldap-authenticator.yaml` | `auth_cassandra.yaml` | 22/22 |
| 8 | `configurations/ms-ad-ldap-authorization.yaml` | `auth_cassandra.yaml` | 22/22 |
| 9 | `configurations/ms-ad-ldap-authorization-and-authentication.yaml` | `auth_cassandra.yaml` | 22/22 |
| 10 | `configurations/alternator/enforce-authorization.yaml` | `auth_cassandra.yaml` (dedup) | 18/18 |
| 11 | `configurations/rolling-upgrade-alternator.yaml` | `auth_cassandra.yaml` (dedup) | 18/18 |
| 12 | `test-cases/upgrades/generic-rolling-upgrade.yaml` | `auth_cassandra.yaml` + `rolling-upgrade-artifacts.yaml` | 6/6 |

### Statistics

- **Total Jenkinsfiles analyzed**: 1,124
- **Multi-config arrays found**: 316
- **Unique config files in use**: 256
- **Hard dependencies (100% co-occurrence)**: 12 config files → `auth_cassandra.yaml`
- **Existing validations in code**: 7
- **New validation rules needed**: 10

---

## References

- Triggering commit: `866d18e70` (consolidate auth settings)
- Jenkins caching bug: [JENKINS-41929](https://issues.jenkins-ci.org/browse/JENKINS-41929)
- Affected test: `rolling-upgrade-with-sla-no-shares`
- Config loader: `sdcm/sct_config.py`
- Pipeline: `vars/rollingUpgradePipeline.groovy`
- Auth config: `configurations/auth_cassandra.yaml`
