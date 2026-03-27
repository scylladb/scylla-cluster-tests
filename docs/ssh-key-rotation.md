# SSH Key Rotation

## Problem

The SSH keys used by SCT (`scylla_test_id_ed25519`, `scylla-test`, `scylla-qa-ec2`) need to be regenerated as part of routine credential rotation. These keys are stored in the `scylla-qa-keystore` S3 bucket and are used by every SCT test run to SSH into provisioned cloud instances across all backends (AWS, GCE, Azure, OCI, Docker). They are also referenced in Jenkins pipelines and utility scripts.

## Approach

### Phase 1: Generate New Keys

Generate fresh Ed25519 and RSA key pairs locally:

```bash
# Primary key (Ed25519) — used by all backends
ssh-keygen -t ed25519 -f scylla_test_id_ed25519 -N "" -C "sct-qa@scylladb.com"

# Legacy RSA key — still synced by sct.py (line 242) and used by
# vars/copyLogsFromSctRunner.groovy and OCI Jenkins plugin config
ssh-keygen -t rsa -b 4096 -f scylla-qa-ec2 -N "" -C "sct-qa@scylladb.com"

# Legacy key alias — synced by sct.py but functionally identical to scylla-qa-ec2;
# generate separately so S3 objects remain independent
ssh-keygen -t rsa -b 4096 -f scylla-test -N "" -C "sct-qa@scylladb.com"
```

### Phase 2: Upload to S3

Upload the new keys (private + public) to the keystore bucket:

```bash
BUCKET=scylla-qa-keystore

# Ed25519
aws s3 cp scylla_test_id_ed25519     s3://$BUCKET/scylla_test_id_ed25519
aws s3 cp scylla_test_id_ed25519.pub s3://$BUCKET/scylla_test_id_ed25519.pub

# RSA legacy keys
aws s3 cp scylla-qa-ec2     s3://$BUCKET/scylla-qa-ec2
aws s3 cp scylla-qa-ec2.pub s3://$BUCKET/scylla-qa-ec2.pub
aws s3 cp scylla-test        s3://$BUCKET/scylla-test
aws s3 cp scylla-test.pub    s3://$BUCKET/scylla-test.pub
```

### Phase 3: Update AWS EC2 Key Pairs

The EC2 key pair named `scylla_test_id_ed25519` must be updated in every AWS region SCT uses, because EC2 imports the public key at instance launch time:

```bash
# For each region SCT provisions in:
for REGION in us-east-1 us-west-2 eu-west-1 eu-west-2 eu-north-1 eu-central-1; do
    # Delete old key pair
    aws ec2 delete-key-pair --key-name scylla_test_id_ed25519 --region $REGION
    # Import new public key
    aws ec2 import-key-pair \
        --key-name scylla_test_id_ed25519 \
        --public-key-material fileb://scylla_test_id_ed25519.pub \
        --region $REGION
done
```

**Needs Investigation:** Get the full list of AWS regions from `sdcm/utils/aws_region.py` and the SCT region configuration. Also check if `scylla-qa-ec2` or `scylla-test` are imported as EC2 key pairs in any region.

### Phase 4: Update GCE Project Metadata

GCE uses project-level or instance-level SSH metadata. The public key is embedded via `KeyStore.get_gce_ssh_key_pair()` at instance creation time (`sdcm/cluster_gce.py`). No separate GCE-side key registration is needed — new instances will pick up the new key automatically from S3.

### Phase 5: Update Jenkins Credentials

Jenkins stores SSH keys as credentials for SCT runner access and log collection:

1. **Jenkins Credential:** `user-jenkins_scylla-qa-ec2-rsa.pem`
   - Referenced in: `sdcm/utils/builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl`
   - Update: Jenkins → Manage Jenkins → Credentials → find `scylla-qa-ec2` → replace private key content

2. **Any other SSH key credentials** used by Jenkins for SCT runner SSH access
   - Check: Jenkins → Manage Jenkins → Credentials → search for "scylla"
   - Update each credential with the corresponding new private key

**Needs Investigation:** Enumerate all Jenkins credentials referencing SCT SSH keys. The `Jenkinsfile` (line 269) and `vars/copyLogsFromSctRunner.groovy` (line 17) load keys from `~/.ssh/` which are synced from S3, so they'll auto-update. But any Jenkins-native SSH credentials must be updated manually.

### Phase 6: Rebuild SCT Runner Images

After rotating the SSH keys, new SCT runner images must be created so that runners launched in the future have the updated keys baked in. Existing runners will continue to work because they sync keys from S3 at startup, but new images ensure a clean baseline.

Follow the process documented in [docs/sct-runners.md](sct-runners.md#process-of-updating-sct-runner-images):

1. Update the version number in `sct_runner.py`
2. Build new images for all cloud providers:
   ```bash
   ./sct.py create-runner-image -c aws -r eu-west-2 -z a
   ./sct.py create-runner-image -c gce -r us-east1 -z b
   SCT_GCE_PROJECT=gcp-local-ssd-latency ./sct.py create-runner-image -c gce -r us-east1 -z b
   ./sct.py create-runner-image -c azure -r eastus -z a
   ./sct.py create-runner-image -c oci -r us-ashburn-1 -z a
   ./sct.py create-runner-image -c oci -r  us-phoenix-1 -z a
   ```
3. Update version references in `aws_builder.py` and `gce_builder.py`
4. Rebuild Jenkins builder configuration:
   ```bash
   ./sct.py configure-jenkins-builders -c gce
   ./sct.py configure-jenkins-builders -c aws
   ```
5. Update `vars/getJenkinsLabels.groovy` with the new labels

### Phase 7: Validate

1. Delete local cached keys: `rm -f ~/.ssh/scylla_test_id_ed25519 ~/.ssh/scylla-qa-ec2 ~/.ssh/scylla-test`
2. Run `KeyStore().sync()` to download new keys from S3
3. Run a Docker backend test to verify basic SSH connectivity:
   ```bash
   uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
       --backend docker --config test-cases/PR-provision-test.yaml
   ```
4. Run a cloud backend test (e.g., AWS) to verify EC2 key pair works
5. Verify Jenkins can still collect logs from SCT runners

### Phase 8: Cleanup

1. Securely delete the generated key files from the local machine:
   ```bash
   shred -u scylla_test_id_ed25519 scylla_test_id_ed25519.pub
   shred -u scylla-qa-ec2 scylla-qa-ec2.pub
   shred -u scylla-test scylla-test.pub
   ```
2. Invalidate any known_hosts entries for old keys on developer machines

## Files Affected

| File | Role |
|------|------|
| `sdcm/keystore.py` | Downloads keys from `scylla-qa-keystore` S3 bucket — no code changes needed |
| `sct.py:242` | Syncs 3 keys: `scylla-qa-ec2`, `scylla-test`, `scylla_test_id_ed25519` |
| `defaults/aws_config.yaml` | `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` |
| `defaults/gce_config.yaml` | `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` |
| `defaults/azure_config.yaml` | `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` |
| `defaults/oci_config.yaml` | `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` |
| `defaults/docker_config.yaml` | `user_credentials_path: ~/.ssh/scylla_test_id_ed25519` |
| `vars/copyLogsFromSctRunner.groovy:17` | `ssh-add ~/.ssh/scylla-qa-ec2` — auto-updates from S3 sync |
| `Jenkinsfile:269` | `ssh-add ~/.ssh/scylla_test_id_ed25519` — auto-updates from S3 sync |
| `sdcm/utils/builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl` | References Jenkins credential `user-jenkins_scylla-qa-ec2-rsa.pem` |

No code changes are required. This is a purely operational procedure — the keys are external data consumed by the existing infrastructure.

## Verification

- [ ] New keys generated (Ed25519 + 2 RSA)
- [ ] All 6 key objects uploaded to `s3://scylla-qa-keystore/`
- [ ] EC2 key pairs updated in all SCT AWS regions
- [ ] New SCT runner images built for all cloud providers (see [sct-runners.md](sct-runners.md))
- [ ] Jenkins builder configuration rebuilt with new runner images
- [ ] Jenkins SSH credentials updated
- [ ] `KeyStore().sync()` downloads new keys successfully
- [ ] Docker backend test passes with new keys
- [ ] Cloud backend test passes (EC2 SSH works)
- [ ] Jenkins log collection works from SCT runners
- [ ] Local key files securely deleted
