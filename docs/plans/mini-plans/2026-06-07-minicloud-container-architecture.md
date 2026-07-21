# Minicloud Container Architecture

> **Note:** This mini-plan will be dropped once all items are implemented and key highlights have been merged into [`docs/plans/minicloud-local-testing.md`](../minicloud-local-testing.md). See [PR #14009](https://github.com/scylladb/scylla-cluster-tests/pull/14009) for the consolidated plan.

## Problem

Running minicloud inside hydra requires:
1. Adding QEMU/KVM/OVMF packages to the hydra Dockerfile (image bloat)
2. Complex process lifecycle management (PID files, orphan detection, signal handling)
3. Port conflicts when host minicloud collides with container minicloud (`--net=host`)
4. OVMF firmware path differences across distros (`OVMF_CODE not found` errors)
5. Tight coupling between minicloud release cadence and SCT/hydra image builds

Other projects wanting to use minicloud would need to replicate all hydra modifications.

## Approach

Run minicloud in its **own Docker/Podman container** — same pattern as rsyslog/vector companion services in SCT. Minicloud ships its own image with all dependencies baked in.

### 1. Minicloud Docker Image (minicloud repo — separate work)

The minicloud repo publishes a container image with:
- `minicloud` binary at `/usr/local/bin/minicloud`
- QEMU (`qemu-system-x86`), OVMF firmware, networking tools
- Entrypoint: `["minicloud"]` with all args passthrough

Runtime requirements (user provides):
```bash
docker run -d --name minicloud \
    --device /dev/kvm \
    --net=host \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    -e AWS_REGION=eu-west-1 \
    minicloud:latest \
    --port 5000 --aws-region eu-west-1
```

### 2. SCT Activation Script (`scripts/start-minicloud.sh`)

Shell script to start/stop the minicloud container:
```bash
#!/bin/bash
set -euo pipefail
MINICLOUD_VERSION=${MINICLOUD_VERSION:-latest}
MINICLOUD_PORT=${MINICLOUD_PORT:-5000}
MINICLOUD_IMAGE=${MINICLOUD_IMAGE:-minicloud:${MINICLOUD_VERSION}}

# Stop existing if running
docker stop minicloud 2>/dev/null && docker rm minicloud 2>/dev/null || true

docker run -d --name minicloud \
    --device /dev/kvm \
    --net=host \
    -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    -e AWS_REGION="${AWS_REGION:-eu-west-1}" \
    ${GCS_KEY_FILE:+-v "${GCS_KEY_FILE}:/etc/minicloud/gcs-key.json:ro"} \
    "${MINICLOUD_IMAGE}" \
    --port "${MINICLOUD_PORT}" \
    --aws-region "${AWS_REGION:-eu-west-1}" \
    --s3-passthrough-buckets "${MINICLOUD_S3_BUCKETS:-downloads.scylladb.com}"

# Wait for health
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${MINICLOUD_PORT}" \
        -d "Action=DescribeRegions&Version=2016-11-15" >/dev/null 2>&1; then
        echo "minicloud is healthy on port ${MINICLOUD_PORT}"
        exit 0
    fi
    sleep 1
done
echo "ERROR: minicloud failed to become healthy" >&2
docker logs minicloud
exit 1
```

### 3. Jenkins Pipeline Changes (`vars/minicloudPipeline.groovy`)

Replace the "Setup Minicloud" stage (binary download + extraction) with container start:
```groovy
stage('Start Minicloud') {
    steps {
        script {
            dir('scylla-cluster-tests') {
                timeout(time: 5, unit: 'MINUTES') {
                    sh """
                        export MINICLOUD_VERSION="${params.minicloud_version}"
                        bash scripts/start-minicloud.sh
                    """
                    completed_stages['start_minicloud'] = true
                }
            }
        }
    }
}
```

Add cleanup in `post { always }`:
```groovy
sh 'docker stop minicloud || true; docker rm minicloud || true'
```

### 4. Revert Hydra Dockerfile

Remove QEMU/KVM packages added for minicloud from `docker/env/hydra.sh` and the hydra Dockerfile.

### 5. MinicloudManager Health Check (already done)

`sdcm/utils/minicloud.py` `start()` now calls `_is_endpoint_healthy()` first — if minicloud is already responding (from the container), it reuses it and skips starting a new process.

## Benefits

- **No hydra image changes** — zero coupling between minicloud deps and SCT container
- **Pipeline lifecycle** — container persists across stages; `docker stop` kills minicloud + all QEMU VMs
- **Port conflict solved** — container lifecycle = minicloud lifecycle; no orphan processes
- **OVMF path guaranteed** — image packages firmware at known path
- **Other projects** — any CI system or project can `docker run minicloud:latest` without importing SCT tooling
- **Independent versioning** — minicloud image tagged by git SHA, released on its own cadence

## Files to Modify

| File | Action | Purpose |
|------|--------|---------|
| `scripts/start-minicloud.sh` | Create | Container activation/health-check script |
| `vars/minicloudPipeline.groovy` | Modify | Replace binary download stage with `docker run` |
| `docker/env/Dockerfile.hydra` | Modify | Remove QEMU/KVM/OVMF packages |
| `docker/env/hydra.sh` | Verify | MINICLOUD_OPTIONS forwarding still works (env vars) |
| `sdcm/utils/minicloud.py` | Done | `_is_endpoint_healthy()` detects external container |
| `scripts/minicloud-runner-setup.sh` | Delete | No longer needed (deps in container image) |
| `scripts/minicloud-package.sh` | Delete | Replaced by Docker image build/push |

## Verification

- [ ] `scripts/start-minicloud.sh` starts minicloud container and health check passes
- [ ] SCT test detects running minicloud via `_is_endpoint_healthy()` and skips self-start
- [ ] `docker stop minicloud` kills minicloud + all QEMU VMs inside the container
- [ ] Hydra image builds without QEMU/KVM packages (smaller image)
- [ ] Jenkins pipeline runs full cycle: start container → provision → test → collect logs → stop container
- [ ] Credentials (AWS, optional GCS) correctly passed to minicloud container
