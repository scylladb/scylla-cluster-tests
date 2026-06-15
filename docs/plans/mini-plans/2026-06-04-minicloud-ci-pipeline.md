# Minicloud CI Pipeline

## Problem

Minicloud tests currently only run locally and require CI integration. Key challenges:
1. **Bare-metal KVM requirement**: Minicloud starts QEMU/KVM VMs directly, requiring `/dev/kvm` access on the host. Standard Jenkins builders running Docker/Hydra cannot be used.
2. **Infrastructure distribution**: No binary releases exist. We need an S3-hosted distribution mechanism.
3. **Runner setup**: Automation is needed to install system dependencies (QEMU, bridge-utils) and the minicloud binary on fresh KVM-capable runners.
4. **Multi-stage pipeline minicloud awareness**: The `minicloudPipeline.groovy` shared library runs multiple stages (`provision-resources`, `collect-logs`, `clean-resources`) in separate hydra containers. Each stage calls `sct.py` commands that use boto3 — without minicloud running, those boto3 calls go to **real AWS** instead of minicloud. Only `run-test` (via `tester.py` setUp) currently starts minicloud automatically.

## Approach

### 1. Run Scripts (✅ Done)

The following scripts are already implemented and use `uv run sct.py` under the hood:
- `scripts/run-minicloud-artifact-test.sh`
- `scripts/run-minicloud-artifact-install-test.sh`
- `scripts/run-minicloud-provision-test.sh`
- `scripts/run-minicloud-gce-provision-test.sh`
- `scripts/run-minicloud-prepare-region.sh`
- `scripts/run-minicloud-tests.sh`

### 2. Minicloud-Aware `sct.py` Commands (TODO)

**Root Cause (CI build #16):** `provision-resources` creates instances on REAL AWS (no minicloud running), then `run_tests` starts fresh minicloud, finds 0 instances, creates new ones that fail to boot due to memory constraints.

**Solution:** Add minicloud startup/readiness check to `sct.py` commands that run in their own pipeline stages. Each command needs to detect `is_minicloud_active()` and either start or verify minicloud is running before making any boto3 calls.

#### Commands Requiring Minicloud Awareness

| Command | Entry Point | Why It Needs Minicloud |
|---------|-------------|----------------------|
| `provision-resources` | `sct.py` L265 | Creates EC2 instances via boto3 — must target minicloud, not real AWS |
| `clean-resources` | `sct.py` L384 | Terminates instances via boto3 — must target minicloud to find/destroy the right instances |
| `collect-logs` | `sct.py` L2008 | SSHs into nodes to collect logs — needs minicloud networking (TUN/routes) to reach VMs at `10.12.x.x` |

#### Implementation Pattern (same for all three)

```python
from sdcm.utils.minicloud import MinicloudConfig, MinicloudManager, is_minicloud_active

if is_minicloud_active():
    cfg = MinicloudConfig.from_env()
    cfg.log_file = os.path.join(logdir, "minicloud.log")
    manager = MinicloudManager(cfg)
    manager.keep_alive = True  # container must survive between pipeline stages
    manager.preflight_check()
    manager.start()
    if backend in ("aws", "aws-siren"):
        manager.prepare_region()
```

**Key details:**
- `keep_alive=True` prevents atexit cleanup — container must persist across stages
- `MinicloudManager.start()` is idempotent — if container is already running (from a prior stage), it reattaches via health check
- `prepare_region()` ensures VPC/subnet/SG exist in minicloud for the configured region
- `MINICLOUD_AWS_REGION` env var must match `SCT_REGION_NAME` (currently mismatched: minicloud defaults to `us-east-1`, CI uses `eu-west-1`)

#### Pipeline Stages to Review

Beyond the three commands above, review ALL `minicloudPipeline.groovy` stages for minicloud dependency:

| Stage | Shared Library Call | Minicloud Needed? | Status |
|-------|-------------------|-------------------|--------|
| Checkout | `checkout scm` | No | OK |
| Create Argus Test Run | `createArgusTestRun()` | No | OK |
| Get test duration | `getJobTimeouts()` | No | OK |
| Create SCT Runner | `createSctRunner()` | No (real AWS for runner itself) | OK |
| **Provision Resources** | `provisionResources()` | **YES** | TODO |
| **Run SCT Test** | `runSctTest()` | YES (handled by tester.py) | ✅ Done |
| **Collect log data** | `runCollectLogs()` | **YES** | TODO |
| **Clean resources** | `runCleanupResource()` | **YES** | TODO |
| Finish Argus Test Run | `finishArgusTestRun()` | No | OK |
| Send email | `runSendEmail()` | No | OK |
| Clean SCT Runners | `cleanSctRunners()` | No (real AWS for runner itself) | OK |

**Note:** The `post { always { ... } }` block in `minicloudPipeline.groovy` also retries `collect-logs` and `clean-resources` if they failed during the main stages — those retries have the same minicloud dependency.

### 3. S3 Tarball Packaging (TODO: `scripts/minicloud-package.sh`)

Package the locally built minicloud binary and helper scripts, then upload to `s3://scylla-qa-minicloud/`.

```bash
#!/bin/bash
set -euo pipefail
MINICLOUD_DIR="${1:?Usage: $0 /path/to/minicloud/repo}"
VERSION=$(git -C "$MINICLOUD_DIR" rev-parse --short HEAD)
TARBALL="minicloud-${VERSION}.tar.gz"
S3_BUCKET="s3://scylla-qa-minicloud"

tar -czf "/tmp/$TARBALL" -C "$MINICLOUD_DIR" target/release/minicloud minicloud-setup.sh
aws s3 cp "/tmp/$TARBALL" "$S3_BUCKET/$TARBALL"
aws s3 cp "/tmp/$TARBALL" "$S3_BUCKET/minicloud-latest.tar.gz"
```

### 3. Runner Setup Script (TODO: `scripts/minicloud-runner-setup.sh`)

Install system dependencies and download the minicloud binary from S3.

```bash
#!/bin/bash
set -euo pipefail
S3_BUCKET="s3://scylla-qa-minicloud"
INSTALL_DIR="/opt/minicloud"

sudo apt-get update
sudo apt-get install -y --no-install-recommends qemu-system-x86 qemu-utils bridge-utils iproute2 iptables dnsmasq cloud-image-utils

if [ ! -e /dev/kvm ]; then echo "ERROR: /dev/kvm not available"; exit 1; fi

sudo mkdir -p "$INSTALL_DIR"
aws s3 cp "$S3_BUCKET/minicloud-latest.tar.gz" /tmp/minicloud.tar.gz
sudo tar -xzf /tmp/minicloud.tar.gz -C "$INSTALL_DIR"
sudo ln -sf "$INSTALL_DIR/target/release/minicloud" /usr/local/bin/minicloud
```

### 4. Jenkins Pipeline (TODO: `jenkins-pipelines/oss/artifacts/artifacts-minicloud.jenkinsfile`)

A standalone declarative pipeline that runs directly on the host.

```groovy
pipeline {
    agent { label 'minicloud-kvm' }
    environment {
        AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        SCT_MINICLOUD_ENDPOINT_URL = 'http://localhost:5000'
    }
    parameters {
        string(defaultValue: '', description: 'Scylla version', name: 'scylla_version')
        string(defaultValue: '', description: 'Scylla AMI ID', name: 'scylla_ami_id')
    }
    stages {
        stage('Setup') {
            steps {
                sh 'bash scripts/minicloud-runner-setup.sh'
                sh 'minicloud --version'
            }
        }
        stage('Run Tests') {
            steps {
                sh """
                    export SCT_SCYLLA_VERSION="${params.scylla_version ?: '2025.3.0'}"
                    if [ -n "${params.scylla_ami_id}" ]; then export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"; fi
                    bash scripts/run-minicloud-tests.sh --aws-only
                """
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: '~/sct-results/latest/*.log', allowEmptyArchive: true
            archiveArtifacts artifacts: '~/sct-results/latest/events_log/**', allowEmptyArchive: true
        }
    }
}
```

### 5. Shared Library Update (TODO: `vars/getJenkinsLabels.groovy`)

Add the following mapping to the `jenkins_labels` map:
```groovy
'minicloud': 'minicloud-kvm-builders-v1',
```

## Files to Modify

| File | Action | Purpose |
|------|--------|---------|
| `scripts/minicloud-package.sh` | Create | Build + upload tarball to S3 |
| `scripts/minicloud-runner-setup.sh` | Create | Install deps + download from S3 |
| `jenkins-pipelines/oss/artifacts/artifacts-minicloud.jenkinsfile` | Create | Standalone host-based pipeline |
| `vars/getJenkinsLabels.groovy` | Modify | Add `minicloud` label mapping |

## Verification

- [ ] `scripts/minicloud-runner-setup.sh` successfully installs dependencies on a fresh Ubuntu host.
- [ ] `/dev/kvm` is verified to be accessible on the runner.
- [ ] `scripts/run-minicloud-tests.sh --aws-only` passes on the `minicloud-kvm` runner.
- [ ] Jenkins pipeline successfully archives `minicloud.log` and SCT event logs.
- [ ] `uv run sct.py run-test` is confirmed as the execution engine (via the run scripts).

## Pipeline Strategy: Dedicated vs. Unified

**Decision:** Start with a dedicated `minicloudPipeline.groovy`, then plan integration into all pipelines after first successful CI runs.

### Phase 1 (current): Dedicated pipeline

| Aspect | Dedicated pipeline | Adapt all pipelines |
|--------|-------------------|-------------------|
| Risk to production | None — isolated | High — conditional logic in longevityPipeline |
| Runner requirements | KVM-capable m8i.xlarge (fundamentally different from standard builders) | Must conditionally select runner type |
| Iteration speed | Fast — change one file | Slow — must verify both paths |
| Code duplication | Yes — same stage skeleton as longevityPipeline | No — single source of truth |
| PR validation reuse | Needs separate Jenkinsfile per minicloud test | Any job can minicloud-ify by setting env vars |
| Complexity in shared libs | Low — minicloud logic stays in its own pipeline | High — `if (minicloud)` sprinkled across stages |

**Why dedicated first:**
- Runner type is fundamentally different (KVM required, larger memory)
- Extra "Start Minicloud" stage doesn't belong in normal runs
- Zero risk to 50+ existing production Jenkinsfiles
- Python-side sharing (`ensure_minicloud_ready`, `collect_minicloud_logs`) already works across all `sct.py` commands regardless of which pipeline calls them

### Phase 2 (future): Integrate into all pipelines

Once minicloud is stable and proven in CI:
1. Extract shared stages into reusable functions (`vars/runMinicloudStage.groovy`)
2. Add `minicloud_enabled` parameter to `longevityPipeline` (default: false)
3. When enabled: select KVM runner, inject "Start Minicloud" stage, set env vars
4. PR validation jobs can opt-in by setting the parameter
5. Gradually convert existing test jobs to support both modes
