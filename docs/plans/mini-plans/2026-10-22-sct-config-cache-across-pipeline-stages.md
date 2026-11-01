# Mini-Plan: Cache the Loaded SCTConfiguration Across Pipeline Stages

**Date:** 2026-10-22, updated 2026-10-23
**Estimated LOC:** ~300 (plus ~150 of tests and lint)
**Related PR:** none yet

## Problem

Every `hydra` command builds `SCTConfiguration()` from scratch. The constructor merges the config
sources, resolves cloud images, repositories and instance sizes over the network, fetches keystore
secrets and reserves AWS capacity. A longevity pipeline constructs it ten times per run (argus run
creation, test duration, cost estimate, runner creation, provisioning, the test itself, log
collection, cleanup, argus finish, email) from the same job parameters and the same config files,
so nine of the ten loads recompute what the first one already produced.

The job parameters reach `hydra` as `SCT_*` exports that each stage wrapper in `vars/` writes
out for itself: 312 `export SCT_*` lines across ten wrappers, each a different subset of the
same 61 variables. `runSctTest` exports 53 of them, `getJobTimeouts` 8, `runCleanupResource` 16
including the `post_behavior_*` set, and `SCT_REUSE_CLUSTER` appears in five wrappers and is
absent from `getJobTimeouts`. A cache is only as complete as the environment its producer ran
under, so the producer has to run under the union of what every consumer exports.

Measured on `longevity-100gb-4h.yaml`, AWS backend, warm credentials:

| stage of a load | docker | aws |
|---|---|---|
| import | 2.1s | 3.1s |
| `SCTConfiguration()` | 0.2s | 7.7s |
| `verify_configuration()` chain | 0.0s | 1.4s |

Of the 7.7s, 5.3s is AMI/SSM lookups and image resolution, 1.3s is parsing the instance-sizing
catalog and 0.4s is a Secrets Manager fetch. The 'Get test duration' stage pays all of it to read
two integers out of `output-conf`. Every stage also carries the cloud lookups' failure modes: an
email or argus-finish step fails when an AMI lookup fails. Values resolved as "latest" can
legitimately differ between stages, so each stage currently works from its own resolution.

The pipeline already hands resolved state between stages for three fields: the provision→test
region fallback writes region, availability zone and AMIs to a test-id-keyed file in the results
directory, the constructor patches them in after the merge and re-resolves AMIs a second time to
restore ordering. This plan generalises that hand-off into a cache of the whole loaded
configuration, carried by the transport the pipeline already has: `hydra` rsyncs the checkout to
the runner on every remote call.

## Approach

Flow, once, end to end:

`hydra load-config --export-cache <file>` runs in 'Get test duration' under the union of every
stage's `SCT_*` overrides, does a full load and writes it -> `SCT_CONFIG_CACHE_FILE=<file>` is set
build-wide, pointing inside the checkout -> rsync carries the file to the runner on the next remote
hydra call -> every later `SCTConfiguration()` reads the variable, and restores the model from the
file when the file exists, or does a full load otherwise -> the constructor's side effects are
replayed -> the resolved placement delta is applied as today -> the verify chain runs where it runs
today (provision, run-test).

- Producer is an explicit command, `load-config`, with `--export-cache <file>`. It prints the dumped
  configuration like `output-conf` does, so 'Get test duration' calls it in place of `output-conf`
  and is the producer. Writing the cache happens only through this command; it loads cold even with
  the variable set.
- The producer stage exports the union of every consumer's `SCT_*` set through one shared helper,
  `exportSctEnv(params, region, pipelineParams, instanceTypeDb)`, built from the per-wrapper export
  blocks with `runSctTest`'s value expressions. A unit test keeps every wrapper's export set a subset
  of the helper's, so a consumer that gains an export fails CI until the helper gains it too.
- Consumer is `SCT_CONFIG_CACHE_FILE`, relative to the SCT root or absolute. When set and the file
  exists, the constructor restores the model through the per-field Pydantic validation of the
  stored values (nested models come back as models) and returns; a log line records the hit. When
  unset, or the file is missing, the constructor does today's full load; a warning records the
  missing file. The variable is accepted by the "unsupported `SCT_*` environment variables" check.
- The file holds every field whose value differs from its default, the fields excluded from
  `model_dump` among them, the private loader attributes and the set of explicitly set fields. A hit
  is indistinguishable from a cold load.
- Pointing `SCT_CONFIG_CACHE_FILE` at a file is an explicit choice to use it as is; the producer
  always overwrites. The pipeline owns freshness.
- Side effects the constructor performs beyond computing fields are replayed on a hit: keystore
  environment export, event severity limit rules, resolved placement (with the AMI re-resolution a
  relocated region needs), AWS capacity reservation and dedicated host lookup. The replay is pinned
  by tests because the test stage depends on every one of them.
- Two stages decide `SCT_*` overrides after the producer. BYO ScyllaDB sets the image variables
  after its build, so `byoScylladb` runs the producer again at its end and overwrites the file.
  Minicloud decides its `SCT_MINICLOUD_*` set in 'Start Minicloud', on the runner, so minicloud jobs
  keep loading cold in every stage: the producer skips `--export-cache` for them.
- Resolved placement keeps its file and its readers: it is the runner-side delta on top of the
  builder-side cache, written by the region fallback feature after provisioning.
- The file lives in an ignored path inside the checkout and is written on the builder, so the
  builder→runner rsync carries it and its `--delete` keeps builder-side files.
- Consumer wrappers keep their own export blocks in this PR; a cache hit ignores them, a cold load
  uses them. 'Create Argus Test Run' runs before 'Get test duration' and loads cold.
- `longevityPipeline` sets the variable; the other pipelines load cold until they are switched on
  one by one after a green longevity run.

Follow-ups, each its own PR: switch `jepsenPipeline`, `managerPipeline`,
`rollingOperatorUpgradePipeline` and the parallel-branch pipelines (one producer per branch);
move the pure minicloud derivation ahead of the producer; drop the consumer export blocks and
turn a missing cache file into an error together with them; the anyconfig parser-discovery cost in
the docker-image defaults load (0.5s per process).

## Files to Modify

- `sdcm/sct_config/cache.py` -- new; path resolution, payload, write, read, restore, `cold_load()`
- `sdcm/sct_config/config.py` -- `SCTConfiguration.__init__` restores from the cache when the file exists; `_replay_load_side_effects`; the variable is accepted by the unsupported-variables check
- `sct.py` -- new `load-config` command with `--export-cache`
- `vars/exportSctEnv.groovy` -- new; the union export helper
- `vars/getJobTimeouts.groovy` -- export through `exportSctEnv`, call `load-config --export-cache` in place of `output-conf`, skip the export on minicloud jobs
- `vars/byoScylladb.groovy` -- run the producer again after the BYO image variables are set
- `vars/longevityPipeline.groovy` -- set `SCT_CONFIG_CACHE_FILE` in the environment block; pass `pipelineParams` and the region to the two wrappers
- `.gitignore` -- the cache file, next to `sct_runner_ip`
- `unit_tests/unit/config/test_config_cache.py` -- new; see Verification

## Verification

- [ ] Cache round-trip: a cache-hit `SCTConfiguration` has identical `model_dump()`, fields set, excluded fields, private loader attributes and nested model types to the cold-load instance it was written from (docker backend)
- [ ] Side-effect replay: a cache hit calls the keystore export, the severity limit rules, the capacity-reservation and dedicated-host lookups
- [ ] Resolved placement is applied on top of a cache hit
- [ ] `SCT_CONFIG_CACHE_FILE` unset -> no cache; set to a missing path -> warning, full load; relative path resolves against the SCT root; `cold_load()` hides and restores the variable; `verify_configuration()` accepts it
- [ ] Every stage wrapper's `export SCT_*` set is a subset of `exportSctEnv`'s
- [ ] `hydra load-config --export-cache <file>` prints the same dump as `output-conf` and writes the file; `getJobTimeouts` parses the durations from it (needs valid AWS credentials: `sct.py` builds a `KeyStore` at startup)
- [ ] On a real AWS longevity job: 'Create SCT Runner', 'Provision Resources', 'Run SCT Test', 'Collect log data', 'Clean resources', 'Finish Argus Test Run' and 'Send email' each log "Loaded the configuration from the cache file"; 'Get test duration' logs the write; with `post_behavior_db_nodes=destroy` and a `scylla_version` job parameter, both are in the file and cleanup acts on them
- [ ] `uv run pytest unit_tests/unit/config/test_config_cache.py` and `uv run sct.py pre-commit` pass
