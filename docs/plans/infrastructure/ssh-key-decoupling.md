---
status: draft
domain: cluster
created: 2026-03-29
last_updated: 2026-03-29
owner: fruch
---

# SSH Key Decoupling from Baked SCT Runner Images

## Problem Statement

SSH public keys are hardcoded into SCT runner images during the image baking process (`install_prereqs()`). When the SSH key is rotated in the S3 keystore (`scylla-qa-keystore`), all baked images across all backends (AWS, GCE, Azure, OCI) become stale — the `jenkins` user's `authorized_keys` still contains the old key. The only remedy is to rebuild every image on every backend, which is slow and operationally disruptive.

### Current Behavior

In `sdcm/sct_runner.py`, `install_prereqs()` (lines 233–336) runs during image baking and:

1. **Reads the current SSH public key** from KeyStore/S3:
   ```python
   public_key = self.key_pair.public_key.decode()  # line 239
   ```

2. **Writes it to the primary OS user** (`ubuntu`):
   ```bash
   echo "{public_key}" >> /home/{login_user}/.ssh/authorized_keys  # line 254
   ```

3. **Creates a `jenkins` user** and writes the same key:
   ```bash
   echo "{public_key}" >> /home/jenkins/.ssh/authorized_keys  # line 317
   ```

Both keys are frozen into the baked image snapshot. When a runner instance later launches from this image:
- The **primary OS user** (`ubuntu`) gets the *current* key injected at launch time by the cloud provider's native SSH mechanism (see Current State below) — so the baked key is **redundant**.
- The **`jenkins` user** gets **no launch-time key injection** — it relies entirely on the baked key.

### Root Causes

1. **No launch-time key injection for `jenkins`**: Cloud provider SSH mechanisms (EC2 key pairs, GCE metadata `ssh-keys`, Azure OS profile, OCI `ssh_authorized_keys`) only target the primary OS user.
2. **Redundant baked key for `ubuntu`**: The `install_prereqs()` key write for `ubuntu` duplicates what cloud providers already do at launch time.
3. **All backends use the same key**: All four `get_*_ssh_key_pair()` methods in `sdcm/keystore.py` (lines 94–103) retrieve the identical `scylla_test_id_ed25519` key from S3 bucket `scylla-qa-keystore`.

## Current State

### Image-Bake-Time Key Injection (`install_prereqs()`)

**File**: `sdcm/sct_runner.py`, lines 233–336

| User | File Path | Permissions | Written At |
|------|-----------|-------------|------------|
| `ubuntu` (or `LOGIN_USER`) | `/home/{login_user}/.ssh/authorized_keys` | `600`, owned `{login_user}:{login_user}` | Line 254 |
| `jenkins` | `/home/jenkins/.ssh/authorized_keys` | `600`, owned `jenkins:jenkins` | Line 317 |

Both users receive `self.key_pair.public_key` — the same S3-stored key.

### Launch-Time Key Injection per Backend

| Backend | Class | Mechanism | Code Location | Target User |
|---------|-------|-----------|---------------|-------------|
| **AWS** | `AwsSctRunner` | EC2 `KeyName` parameter → guest agent | `sct_runner.py` line 640, `aws_region.py` lines 700–726 | `ubuntu` only |
| **GCE** | `GceSctRunner` | Instance metadata `ssh-keys` field | `sct_runner.py` line 1006 | `ubuntu` only |
| **Azure** | `AzureSctRunner` | OS profile `public_keys` array | `sct_runner.py` line 1182, `provision/azure/virtual_machine_provider.py` lines 246–250 | `ubuntu` only |
| **OCI** | `OciSctRunner` | `ssh_authorized_keys` metadata + cloud-init | `sct_runner.py` line 1546, `runner_configs/oci-sct-runner-cloud-config.yaml` lines 20–27 | `ubuntu` only |

**Key finding**: All backends already inject the *current* key for `ubuntu` at launch time. The `install_prereqs()` write for `ubuntu` is redundant.

### KeyStore Architecture

**File**: `sdcm/keystore.py`

```python
def get_ec2_ssh_key_pair(self):   return self.get_ssh_key_pair(name="scylla_test_id_ed25519")  # line 94
def get_gce_ssh_key_pair(self):   return self.get_ssh_key_pair(name="scylla_test_id_ed25519")  # line 97
def get_azure_ssh_key_pair(self): return self.get_ssh_key_pair(name="scylla_test_id_ed25519")  # line 100
def get_oci_ssh_key_pair(self):   return self.get_ssh_key_pair(name="scylla_test_id_ed25519")  # line 103
```

All backends use the same key from S3 bucket `scylla-qa-keystore`.

### Gap Analysis

If we remove key writes from `install_prereqs()`:

| User | AWS | GCE | Azure | OCI | Status |
|------|-----|-----|-------|-----|--------|
| `ubuntu` | ✅ EC2 key pair | ✅ metadata | ✅ OS profile | ✅ metadata + cloud-init | **SAFE** — covered by cloud providers |
| `jenkins` | ❌ | ❌ | ❌ | ❌ | **CRITICAL** — no launch-time injection |

## Goals

1. **Decouple SSH keys from baked images** so key rotation in S3 does not require image rebuilds
2. **Remove redundant `ubuntu` key write** from `install_prereqs()` — cloud providers already handle this
3. **Add a boot-time mechanism** to inject the current SSH key into `jenkins` user's `authorized_keys` on every instance launch
4. **Backend-agnostic solution** — a single mechanism that works across AWS, GCE, Azure, and OCI without per-backend cloud-init configs

## Implementation Phases

### Phase 1: Create Systemd Oneshot Service for Jenkins Key Sync

**Objective**: Install a systemd service during image baking that runs on every boot and copies the cloud-injected `ubuntu` SSH key to the `jenkins` user.

**Rationale**: All backends already inject the current SSH key for the `ubuntu` user at launch time via their cloud-native mechanisms. A systemd oneshot service can copy this key to `jenkins` on every boot, ensuring `jenkins` always has the current key without any per-backend logic.

**Implementation**:

1. Create a new systemd service file `sct-jenkins-ssh-sync.service`:
   ```ini
   [Unit]
   Description=Sync SSH authorized keys from ubuntu to jenkins user
   After=cloud-init.service cloud-config.service cloud-final.service
   After=google-guest-agent.service google-accounts-daemon.service
   After=walinuxagent.service
   Wants=cloud-init.service

   [Service]
   Type=oneshot
   RemainAfterExit=yes
   ExecStart=/usr/local/bin/sct-sync-jenkins-ssh-keys.sh

   [Install]
   WantedBy=multi-user.target
   ```

2. Create the sync script `/usr/local/bin/sct-sync-jenkins-ssh-keys.sh`:
   ```bash
   #!/bin/bash
   set -euo pipefail

   LOGIN_USER="${1:-ubuntu}"
   SOURCE="/home/${LOGIN_USER}/.ssh/authorized_keys"
   TARGET="/home/jenkins/.ssh/authorized_keys"

   # Wait for the source authorized_keys to be populated by cloud provider
   MAX_WAIT=120
   WAITED=0
   while [ ! -s "$SOURCE" ] && [ "$WAITED" -lt "$MAX_WAIT" ]; do
       sleep 2
       WAITED=$((WAITED + 2))
   done

   if [ ! -s "$SOURCE" ]; then
       echo "ERROR: $SOURCE not populated after ${MAX_WAIT}s" >&2
       exit 1
   fi

   # Ensure target directory exists with correct permissions
   mkdir -p /home/jenkins/.ssh
   cp "$SOURCE" "$TARGET"
   chmod 600 "$TARGET"
   chown jenkins:jenkins "$TARGET"
   chown jenkins:jenkins /home/jenkins/.ssh

   echo "Synced SSH keys from $SOURCE to $TARGET"
   ```

3. During `install_prereqs()`, install the service and script, and enable it:
   ```python
   # Install systemd service and script
   remoter.run(f"cat > /usr/local/bin/sct-sync-jenkins-ssh-keys.sh << 'SCRIPT'\n{script_content}\nSCRIPT")
   remoter.run("chmod +x /usr/local/bin/sct-sync-jenkins-ssh-keys.sh")
   remoter.run(f"cat > /etc/systemd/system/sct-jenkins-ssh-sync.service << 'UNIT'\n{unit_content}\nUNIT")
   remoter.run("systemctl daemon-reload")
   remoter.run("systemctl enable sct-jenkins-ssh-sync.service")
   ```

**Files to modify**:
- `sdcm/sct_runner.py` — `install_prereqs()` method (lines 233–336)

**Definition of Done**:
- [ ] Systemd service file and shell script are installed during `install_prereqs()`
- [ ] Service is enabled and ordered after cloud-init and cloud guest agents
- [ ] Script waits for `ubuntu`'s `authorized_keys` to be populated (up to 120s timeout)
- [ ] Script copies keys with correct ownership (`jenkins:jenkins`) and permissions (`600`)
- [ ] `jenkins` user creation, home dir, `.ssh` dir, and sudoers setup remain in `install_prereqs()`

---

### Phase 2: Remove Redundant Key Writes from `install_prereqs()`

**Objective**: Remove the hardcoded SSH key writes for both `ubuntu` and `jenkins` users from `install_prereqs()`.

**Implementation**:

1. **Remove `ubuntu` key write** (currently line 254):
   ```python
   # REMOVE this line:
   echo "{public_key}" >> /home/{login_user}/.ssh/authorized_keys
   ```
   Cloud providers already inject this key at launch time.

2. **Remove `jenkins` key write** (currently line 317):
   ```python
   # REMOVE this line:
   echo "{public_key}" >> /home/jenkins/.ssh/authorized_keys
   ```
   The systemd service from Phase 1 now handles this at boot time.

3. **Keep `public_key` variable** if still used elsewhere in the method, or remove if it becomes unused.

4. **Keep all other `jenkins` user setup**:
   - User creation (`useradd`)
   - Home directory and `.ssh` directory creation
   - Permissions and ownership (the systemd service handles `authorized_keys` ownership)
   - Sudoers entry

**Files to modify**:
- `sdcm/sct_runner.py` — `install_prereqs()` method

**Dependencies**: Phase 1 (systemd service must be installed before removing the old key writes)

**Definition of Done**:
- [ ] No SSH public key is written to `authorized_keys` during image baking for either user
- [ ] `jenkins` user is still created with home dir, `.ssh` dir, and sudoers
- [ ] `ubuntu` user's `.ssh` directory setup is preserved (cloud providers need it to exist)
- [ ] `self.key_pair.public_key` is still used by launch-time mechanisms (not removed from class)

---

### Phase 3: Manual Verification on All Backends

**Objective**: Verify that SSH access works for both `ubuntu` and `jenkins` users on all backends after the changes.

**Verification procedure per backend**:

1. Build a new image using the modified code
2. Launch an instance from the new image
3. SSH as `ubuntu` — verify access works (cloud provider key injection)
4. SSH as `jenkins` — verify access works (systemd service key sync)
5. Check systemd service status: `systemctl status sct-jenkins-ssh-sync.service`
6. Verify key content matches: `diff /home/ubuntu/.ssh/authorized_keys /home/jenkins/.ssh/authorized_keys`

**Backends to test**:
- [ ] AWS (EC2 key pair mechanism)
- [ ] GCE (metadata `ssh-keys`)
- [ ] Azure (OS profile `public_keys`)
- [ ] OCI (metadata `ssh_authorized_keys` + cloud-init)

**Key rotation test** (optional but recommended on at least one backend):
1. Build image with key A
2. Rotate key in S3 keystore to key B
3. Launch instance from old image
4. Verify `ubuntu` gets key B (from cloud provider)
5. Verify `jenkins` gets key B (from systemd sync)

**Dependencies**: Phases 1 and 2

**Definition of Done**:
- [ ] SSH as `ubuntu` works on all 4 backends
- [ ] SSH as `jenkins` works on all 4 backends
- [ ] Systemd service runs successfully on boot (checked via `systemctl status`)
- [ ] Keys match between `ubuntu` and `jenkins` users

## Testing Requirements

### Unit Tests
No unit tests planned — this is an infrastructure-level change (SSH key provisioning, systemd services) that cannot be meaningfully unit tested. The verification is inherently manual: build an image, launch an instance, SSH in.

### Manual Testing
See Phase 3 above for the full manual verification procedure.

### Integration Tests
No integration tests needed — the change is confined to the image baking process and boot-time behavior, which are not covered by SCT's integration test infrastructure.

## Success Criteria

1. **Key rotation without image rebuild**: After rotating the SSH key in S3 (`scylla-qa-keystore`), new instances launched from existing images allow SSH access for both `ubuntu` and `jenkins` users with the new key
2. **No baked keys**: `install_prereqs()` no longer writes any SSH public keys to `authorized_keys` files
3. **Boot-time sync works**: The `sct-jenkins-ssh-sync.service` systemd unit runs on every boot and correctly populates `jenkins`'s `authorized_keys` from `ubuntu`'s keys
4. **All backends work**: Verified on AWS, GCE, Azure, and OCI
5. **No regression**: Existing SCT test workflows continue to work — runners can be provisioned and accessed via SSH as before

## Risk Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Cloud provider doesn't populate `ubuntu` keys before systemd service runs | **High** — `jenkins` SSH broken | Low | Systemd `After=` ordering on cloud-init/guest agents + 120s polling wait in script |
| Specific backend has delayed key injection (e.g., Azure is slow) | **High** — timeout in sync script | Low | 120-second wait with 2s polling; can increase if needed |
| `ubuntu` user's `authorized_keys` path differs across backends | **Medium** — script copies from wrong path | Very Low | All backends use `/home/ubuntu/.ssh/authorized_keys`; verified in research |
| Baked image has no `ubuntu` `.ssh` directory | **Low** — script fails | Very Low | `install_prereqs()` still creates `.ssh` dirs; cloud-init also creates them |
| Systemd service fails silently | **Medium** — `jenkins` access broken, not noticed until later | Low | Script uses `set -euo pipefail` and exits non-zero on failure; `systemctl status` shows failure |

### Rollback Strategy

If issues are found after deploying new images:
1. **Immediate**: Re-add the key writes to `install_prereqs()` and rebuild images (revert to current behavior)
2. **The systemd service is harmless even with baked keys** — it just overwrites them with the same value. So Phase 1 can be deployed independently as a no-risk addition before Phase 2 removes the old writes.

### Recommended Deployment Order

1. **Deploy Phase 1 first** (add systemd service) — zero risk, purely additive
2. **Verify on one backend** that the systemd service works correctly
3. **Deploy Phase 2** (remove baked keys) — now safe because Phase 1 handles `jenkins` keys
4. **Verify on all backends** (Phase 3)

## References

- `sdcm/sct_runner.py` — `install_prereqs()` (lines 233–336), backend classes, `_create_instance()` methods
- `sdcm/keystore.py` — `KeyStore` class, `get_*_ssh_key_pair()` methods (lines 86–103)
- `sdcm/utils/aws_region.py` — `SCT_KEY_PAIR_NAME` constant (line 40), `update_sct_key_pair()` (lines 700–726)
- `sdcm/runner_configs/oci-sct-runner-cloud-config.yaml` — OCI cloud-init SSH key fetch
- `sdcm/provision/azure/virtual_machine_provider.py` — Azure SSH key injection (lines 246–250)
- `sdcm/provision/gce/instance_provider.py` — GCE metadata SSH key injection (line 229)
- `sdcm/provision/oci/virtual_machine_provider.py` — OCI metadata SSH key injection (line 269)
