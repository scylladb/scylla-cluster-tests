# Minicloud CI Pipeline (SUPERSEDED)

> **NOTE**: This plan is partially superseded by `2026-06-07-minicloud-container-architecture.md`.
> The binary-download + runner-setup approach is replaced by running minicloud in its own
> Docker container. The run scripts (section 1) remain valid. Sections 2-5 are obsolete.

## Problem

Minicloud tests currently only run locally and require CI integration. Key challenges:
1. **Bare-metal KVM requirement**: Minicloud starts QEMU/KVM VMs directly, requiring `/dev/kvm` access on the host. Standard Jenkins builders running Docker/Hydra cannot be used.
2. **Infrastructure distribution**: No binary releases exist. We need an S3-hosted distribution mechanism.
3. **Runner setup**: Automation is needed to install system dependencies (QEMU, bridge-utils) and the minicloud binary on fresh KVM-capable runners.

## Approach

### 1. Run Scripts (✅ Done)

The following scripts are already implemented and use `uv run sct.py` under the hood:
- `scripts/run-minicloud-artifact-test.sh`
- `scripts/run-minicloud-artifact-install-test.sh`
- `scripts/run-minicloud-provision-test.sh`
- `scripts/run-minicloud-gce-provision-test.sh`
- `scripts/run-minicloud-prepare-region.sh`
- `scripts/run-minicloud-tests.sh`

### 2. S3 Tarball Packaging (TODO: `scripts/minicloud-package.sh`)

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
