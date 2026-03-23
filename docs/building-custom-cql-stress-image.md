# Building a Custom cql-stress Docker Image

This document describes how to build a custom Docker image for
[cql-stress](https://github.com/scylladb/cql-stress) from a feature branch or
pull request, push it to Docker Hub, and configure SCT to use it.

## Background

SCT uses the `cql-stress` Docker image to run `cql-stress-cassandra-stress` and
`cql-stress-scylla-bench` stress tools on loader nodes. The default image is
defined in `defaults/docker_images/cql-stress-cassandra-stress/values_cql-stress-cassandra-stress.yaml`.

When testing experimental features that require cql-stress changes not yet
released, you need to build a custom image from source.

### Reference: Strong Consistency Build (2026-03-31)

This guide was created while building a custom image from
[PR #181](https://github.com/scylladb/cql-stress/pull/181) — a Raft-leader-aware
load balancing policy for strong consistency performance testing. The resulting
image is published at `<repo>/cql-stress:strong-consistency` on docker hub.

## Prerequisites

- Docker installed and running
- Docker Hub account with a Personal Access Token (PAT) for pushing
- Git

## Step-by-Step Instructions

### Step 1: Clone the cql-stress repository

```bash
git clone https://github.com/scylladb/cql-stress.git
cd cql-stress
```

### Step 2: Checkout the target branch or PR

If the code is in an unmerged PR from a fork, add the fork as a remote:

```bash
# Replace <fork-user> and <branch-name> with actual values
git remote add <fork-user> https://github.com/<fork-user>/cql-stress.git
git fetch <fork-user> <branch-name>
git checkout <fork-user>/<branch-name>
```

**Example** (PR #181 — strong consistency):

```bash
git remote add jadw1 https://github.com/Jadw1/cql-stress.git
git fetch jadw1 strong_consistent_load_balancing_policy
git checkout jadw1/strong_consistent_load_balancing_policy
```

If the code is on a branch in the main repo, simply:

```bash
git checkout <branch-name>
```

### Step 3: (Optional) Speed up the build

The default Dockerfile uses `cargo build --profile dist` which enables LTO
(Link Time Optimization) and takes 20+ minutes. For development/testing builds,
edit the Dockerfile to use `--release` instead (~5 minutes):

```bash
sed -i 's/--profile dist/--release/' Dockerfile
sed -i 's|target/dist/|target/release/|g' Dockerfile
```

Or apply the changes manually:

```diff
# In the builder stage RUN command:
-    && cargo build --profile dist --all
+    && cargo build --release --all

# In the production stage COPY commands:
-COPY --from=builder /app/target/dist/cql-stress-cassandra-stress /usr/local/bin/cql-stress-cassandra-stress
-COPY --from=builder /app/target/dist/cql-stress-scylla-bench /usr/local/bin/cql-stress-scylla-bench
+COPY --from=builder /app/target/release/cql-stress-cassandra-stress /usr/local/bin/cql-stress-cassandra-stress
+COPY --from=builder /app/target/release/cql-stress-scylla-bench /usr/local/bin/cql-stress-scylla-bench
```

Use `--profile dist` only when building a production-quality image for official
benchmarks.

### Step 4: Build the Docker image

```bash
docker build -t <your-dockerhub-user>/cql-stress:<tag> .
```

**Example:**

```bash
docker build -t aleksbykov/cql-stress:strong-consistency .
```

### Step 5: Verify the image

```bash
# Check the binary runs and prints version info
docker run --rm <your-dockerhub-user>/cql-stress:<tag> -c "cql-stress-cassandra-stress version"
```

Expected output includes the git SHA matching the branch head commit:

```
cql-stress:
- Version: 0.2.4
- Build Date: 2026-03-30T09:13:14Z
- Git SHA: f0222094ec3f58b66d20cc74e55725bd31c561a9
scylla-driver:
- Version: 1.5.0
```

> **Note:** The version subcommand is `version` (not `--version`).

### Step 6: Push the image to Docker Hub

```bash
# Log in (use a PAT, not your password)
docker login -u <your-dockerhub-user>

# Push
docker push <your-dockerhub-user>/cql-stress:<tag>
```

**Example:**

```bash
docker login -u aleksbykov
docker push aleksbykov/cql-stress:strong-consistency
```

## Configure SCT to Use the Custom Image

### Option A: Config fragment file (recommended)

Create a YAML file in `configurations/stress_images/`:

```yaml
# configurations/stress_images/cql-stress-<feature-name>.yaml
stress_image:
  cql-stress-cassandra-stress: '<your-dockerhub-user>/cql-stress:<tag>'
```

Include it in any test's config chain:

```bash
uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
  --backend aws \
  --config test-cases/my-test.yaml,configurations/stress_images/cql-stress-<feature-name>.yaml
```

SCT uses deep dict merging — only the `cql-stress-cassandra-stress` key is
overridden; all other stress tool images are preserved.

### Option B: Environment variable

```bash
export SCT_STRESS_IMAGE='{"cql-stress-cassandra-stress": "<your-dockerhub-user>/cql-stress:<tag>"}'
```

### Option C: Inline in test-case YAML

Add directly to any test-case YAML file:

```yaml
stress_image:
  cql-stress-cassandra-stress: '<your-dockerhub-user>/cql-stress:<tag>'
```

## How SCT Loads Stress Images

1. `sct_config.py` → `load_docker_images_defaults()` scans all YAML files under
   `defaults/docker_images/` and populates the `stress_image` dict
2. User config files are merged on top (deep merge — partial overrides work)
3. Environment variables override last
4. `CqlStressCassandraStressThread` reads the image from
   `stress_image.cql-stress-cassandra-stress` and pulls it on loader nodes
   before running

## Files Reference

| File | Purpose |
|------|---------|
| `defaults/docker_images/cql-stress-cassandra-stress/values_cql-stress-cassandra-stress.yaml` | Default cql-stress image (upstream release) |
| `configurations/stress_images/cql-stress-strong-consistency.yaml` | Override for strong-consistency custom build |
| `configurations/stress_images/cs-java8.yaml` | Existing example of stress image override |
| `sdcm/cql_stress_cassandra_stress_thread.py` | Thread class that runs cql-stress via Docker |
| `sdcm/sct_config.py` (`load_docker_images_defaults`) | Config loading logic for stress images |

## Cleanup

After testing is complete:

1. Remove the config fragment if no longer needed
2. Optionally delete the image from Docker Hub
3. Remove the local clone: `rm -rf /path/to/cql-stress`
