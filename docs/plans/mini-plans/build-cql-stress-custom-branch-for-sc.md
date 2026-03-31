# Mini-Plan: Build Custom cql-stress Docker Image for Strong Consistency

**Date:** 2026-03-31
**Estimated LOC:** ~50 (SCT config change only; Docker build is external)
**Related PR:** https://github.com/scylladb/cql-stress/pull/181

## Problem

Early performance testing of ScyllaDB's strong consistency feature requires a custom build of cql-stress with a Raft-leader-aware load balancing policy (PR #181). The current default image (`scylladb/cql-stress:0.2.7`) does not include this policy. We need to build a custom Docker image from the PR branch and configure SCT to use it.

## Approach

### Preamble

Do not create a new branch. All changes go as a new commit on the current branch.

### Phase 1: Clone cql-stress and checkout PR branch

1. Clone the cql-stress repo: `git clone https://github.com/scylladb/cql-stress.git`
2. Add the PR author's fork as a remote and fetch the PR branch:
   ```
   cd cql-stress
   git remote add jadw1 https://github.com/Jadw1/cql-stress.git
   git fetch jadw1 strong_consistent_load_balancing_policy
   git checkout jadw1/strong_consistent_load_balancing_policy
   ```

### Phase 2: Build custom Docker image locally

3. Build the Docker image using the existing `Dockerfile` in the cql-stress repo root. The Dockerfile uses a multi-stage build: Rust builder stage compiles binaries, then copies them into a slim Debian image.

   For faster builds (~5 min), edit the Dockerfile to use `--release` instead of `--profile dist` and update COPY paths from `target/dist/` to `target/release/`:
   ```diff
   -    && cargo build --profile dist --all
   +    && cargo build --release --all
   ...
   -COPY --from=builder /app/target/dist/cql-stress-cassandra-stress ...
   -COPY --from=builder /app/target/dist/cql-stress-scylla-bench ...
   +COPY --from=builder /app/target/release/cql-stress-cassandra-stress ...
   +COPY --from=builder /app/target/release/cql-stress-scylla-bench ...
   ```
   Then build:
   ```
   docker build -t scylladb/cql-stress:strong-consistency .
   ```
   **Note:** Use `--profile dist` (LTO, 20+ min) only if you need a production-quality optimized build.

### Phase 3: Configure SCT to use the custom image

4. Create a config fragment following the established pattern in `configurations/stress_images/cs-java8.yaml`. SCT uses deep dict merging, so only the `cql-stress-cassandra-stress` key is overridden — all other stress images are preserved.

   The file can be included in any test's config chain:
   ```bash
   --config test-cases/my-test.yaml,configurations/stress_images/cql-stress-strong-consistency.yaml
   ```
   Alternatively, override via environment variable: `SCT_STRESS_IMAGE='{"cql-stress-cassandra-stress": "scylladb/cql-stress:strong-consistency"}'`

## Files to Create

- `configurations/stress_images/cql-stress-strong-consistency.yaml` — override cql-stress image tag:
  ```yaml
  stress_image:
    cql-stress-cassandra-stress: 'scylladb/cql-stress:strong-consistency'
  ```

## Verification

- [ ] Docker image builds successfully: `docker build -t scylladb/cql-stress:strong-consistency .`
- [ ] Image contains the binary: `docker run --rm scylladb/cql-stress:strong-consistency -c "cql-stress-cassandra-stress --version"`
- [ ] SCT picks up the custom image: check `stress_image.cql-stress-cassandra-stress` resolves to the new tag


## Decisions

- The custom image is local-only and tagged `strong-consistency` to distinguish from upstream releases
- PR #181 is a draft (DO NOT MERGE) — this build is strictly for early strong-consistency performance testing
- The Raft leader policy only works with strong-consistent keyspaces; non-strong-consistent keyspaces will fail
