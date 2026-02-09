# Implementation Plan: Fix GCE Node Vector Bootstrap on Rocky Linux 10

**Jira Issue**: [SCT-67](https://scylladb.atlassian.net/browse/SCT-67)
**Status**: Approved for Implementation
**Author**: Rovo Dev
**Date**: 2026-02-09

## Problem Statement

GCE node creation fails during the bootstrap step on Rocky Linux 10 when attempting to install the Vector logging agent. The installation fails with:

- "No match for argument: vector"
- "Failed to enable unit: Unit vector.service does not exist"

This impacts users trying to set up test environments for upgrades and other tests on Rocky Linux 10.

## Root Cause Analysis

The failure occurs because:

1. **Rocky Linux 10 is RPM-based** (RHEL family), not DEB-based
2. **Current `download_vector_locally()` only downloads `.deb` packages** regardless of target OS
3. **The `install_vector_from_local_pkg()` function only handles DEB packages** using `dpkg` and `apt-get`
4. When the script tries to install a `.deb` package on Rocky Linux 10:
   - `dpkg -i` fails (command doesn't exist on RHEL systems)
   - `apt-get install` fails (APT not available on RHEL systems)
   - Vector service is never installed, causing the systemctl failure

### Current Code Flow

```
configure_remote_logging() [sdcm/cluster_cloud.py:350]
  ↓
download_vector_locally(arch) → downloads .deb only
  ↓
install_vector_from_local_pkg() → uses dpkg + apt-get only
  ↓
FAILS on Rocky Linux 10 (RPM-based system)
```

## Solution Design

### Overview

Add multi-distro support to the Vector installation process by:

1. **Making `download_vector_locally()` distro-aware** - Download `.rpm` for RHEL-based systems, `.deb` for Debian-based systems
2. **Updating `install_vector_from_local_pkg()` to handle both package formats** - Use `rpm`/`yum` for RPM packages, `dpkg`/`apt-get` for DEB packages
3. **Adding distro detection** in `configure_remote_logging()` to select the correct package type

### Package URLs

Vector provides packages at:
- **DEB**: `https://packages.timber.io/vector/latest/vector_latest-1_{arch}.deb`
- **RPM**: `https://packages.timber.io/vector/latest/vector-latest-1.{arch}.rpm`

Note: The setup script at `https://setup.vector.dev` already supports Rocky Linux, confirming that Vector has proper RPM packages available.

## Implementation Plan

### Step 1: Update `download_vector_locally()` Function

**File**: `sdcm/cluster_cloud.py`

**Changes**:
```python
def download_vector_locally(arch="amd64", pkg_type="deb", dest_dir="downloads"):
    """
    Download the latest vector.dev installer package for a given architecture.

    Args:
        arch: Architecture (amd64, arm64, etc.)
        pkg_type: Package type - "deb" for Debian-based, "rpm" for RHEL-based
        dest_dir: Destination directory for download

    Returns:
        Path: Path to downloaded package file
    """
    os.makedirs(dest_dir, exist_ok=True)

    if pkg_type == "rpm":
        # RPM package URL format
        url = f"{VECTOR_BASE_URL}/vector-latest-1.{arch}.rpm"
        dest = Path(dest_dir) / f"vector-latest-1.{arch}.rpm"
    else:
        # DEB package URL format (default)
        url = f"{VECTOR_BASE_URL}/vector_latest-1_{arch}.deb"
        dest = Path(dest_dir) / f"vector_latest-1_{arch}.deb"

    LOGGER.debug(f"➡ Downloading {url} -> {dest}")
    download_file(url=url, dest=str(dest))
    LOGGER.debug(f"✔ Downloaded: {dest}")
    return dest
```

### Step 2: Update `install_vector_from_local_pkg()` Function

**File**: `sdcm/provision/common/utils.py`

**Changes**:
```python
def install_vector_from_local_pkg(pkg_path: str) -> str:
    """Install Vector from a local package file (.deb or .rpm)

    Args:
        pkg_path: Path to the vector package file

    Returns:
        str: Shell script for installing vector
    """
    is_rpm = pkg_path.endswith('.rpm')

    if is_rpm:
        # RPM-based installation (Rocky, CentOS, AlmaLinux, RHEL)
        return dedent(f"""\
            # Install Vector RPM package
            rpm -i --force {pkg_path} || true

{update_repo_cache()}
            # Resolve dependencies with yum/dnf
            for n in 1 2 3; do
                if yum install -y vector 2>/dev/null || dnf install -y vector 2>/dev/null; then
                    break
                fi
                sleep $(backoff $n)
            done

            # Verify installation
            if ! rpm -q vector ; then
                echo "ERROR: Failed to install vector package"
                exit 1
            fi

            systemctl enable vector
            systemctl start vector
        """)
    else:
        # DEB-based installation (existing logic for Debian/Ubuntu)
        return dedent(f"""\
            dpkg -i --force-confold --force-confdef {pkg_path}

{update_repo_cache()}
            for n in 1 2 3; do
                DEBIAN_FRONTEND=noninteractive apt-get install -o Dpkg::Options::=--force-confold -o Dpkg::Options::=--force-confdef -o DPkg::Lock::Timeout=300 -y vector || true
                if dpkg-query --show vector ; then
                    break
                fi
                sleep $(backoff $n)
            done

            if ! dpkg-query --show vector ; then
                echo "ERROR: Failed to install vector package"
                exit 1
            fi

            systemctl enable vector
            systemctl start vector
        """)
```

### Step 3: Update `configure_remote_logging()` Method

**File**: `sdcm/cluster_cloud.py`

**Changes**:
```python
def configure_remote_logging(self):
    if self.xcloud_connect_supported and self.parent_cluster.params.get("logs_transport") == "vector":
        # Detect architecture
        ret = self.remoter.run("dpkg --print-architecture 2>/dev/null || uname -m", retry=0)
        arch = ret.stdout.strip() if ret.return_code == 0 else "amd64"

        # Normalize architecture names for RPM (x86_64 vs amd64)
        if arch == "x86_64":
            arch = "amd64"
        elif arch == "aarch64":
            arch = "arm64"

        # Detect package manager type (DEB vs RPM)
        ret = self.remoter.run("command -v apt-get >/dev/null 2>&1 && echo 'deb' || echo 'rpm'", retry=0)
        pkg_type = ret.stdout.strip() if ret.return_code == 0 else "deb"

        LOGGER.debug(f"Detected OS type: {pkg_type}, architecture: {arch}")

        package_path = download_vector_locally(arch=arch, pkg_type=pkg_type)

        remote_path = f"/tmp/{os.path.basename(package_path)}"

        LOGGER.debug(f"➡ Copying {package_path}")
        self.remoter.send_files(str(package_path), remote_path)

        LOGGER.debug("➡ Installing Vector")
        ssh_cmd = configure_backoff_timeout() + install_vector_from_local_pkg(remote_path)
        self.remoter.sudo(shell_script_cmd(ssh_cmd, quote="'"), retry=0, verbose=True)
        host, port = TestConfig().get_logging_service_host_port()
        ssh_cmd = configure_vector_target_script(host=host, port=port)
        self.remoter.sudo(shell_script_cmd(ssh_cmd, quote="'"), retry=0, verbose=True)
    else:
        self.log.debug("Skip configuring remote logging on scylla-cloud, for anything but vector transport")
```

### Step 4: Update `install_vector.py` Helper Script (Optional)

**File**: `install_vector.py`

This is a standalone test script. Update it for consistency:

```python
def download_vector_locally(arch="amd64", pkg_type="deb", dest_dir="downloads"):
    """
    Download the Vector installer package for a given architecture.

    Args:
        arch: Architecture (amd64, arm64)
        pkg_type: Package type - "deb" or "rpm"
        dest_dir: Destination directory
    """
    os.makedirs(dest_dir, exist_ok=True)

    if pkg_type == "rpm":
        url = f"{VECTOR_BASE_URL}/vector-latest-1.{arch}.rpm"
        dest = Path(dest_dir) / f"vector-latest-1.{arch}.rpm"
    else:
        url = f"{VECTOR_BASE_URL}/vector_latest-1_{arch}.deb"
        dest = Path(dest_dir) / f"vector_latest-1_{arch}.deb"

    if dest.exists():
        print(f"✔ Already downloaded: {dest}")
        return dest

    print(f"➡ Downloading {url} -> {dest}")
    subprocess.run(["wget", "-q", url, "-O", str(dest)], check=True)
    print(f"✔ Downloaded: {dest}")
    return dest
```

## Testing Strategy

### 1. Unit Tests

Create tests for package type detection:

```python
# unit_tests/test_vector_installation.py
def test_download_vector_deb():
    """Test DEB package download"""
    path = download_vector_locally(arch="amd64", pkg_type="deb")
    assert path.suffix == ".deb"
    assert "vector_latest-1_amd64.deb" in str(path)

def test_download_vector_rpm():
    """Test RPM package download"""
    path = download_vector_locally(arch="amd64", pkg_type="rpm")
    assert path.suffix == ".rpm"
    assert "vector-latest-1.amd64.rpm" in str(path)

def test_install_script_rpm():
    """Test RPM installation script generation"""
    script = install_vector_from_local_pkg("/tmp/vector.rpm")
    assert "rpm -i" in script
    assert "yum install -y vector" in script or "dnf install -y vector" in script
    assert "systemctl enable vector" in script

def test_install_script_deb():
    """Test DEB installation script generation"""
    script = install_vector_from_local_pkg("/tmp/vector.deb")
    assert "dpkg -i" in script
    assert "apt-get install" in script
    assert "systemctl enable vector" in script
```

### 2. Integration Tests

**Test on Rocky Linux 10**:
```bash
# Run artifacts test on Rocky 10
uv run sct.py run-test artifacts_test \
  --backend gce \
  --config test-cases/artifacts/rocky10.yaml
```

**Verify vector installation**:
```bash
# On the Rocky 10 node
rpm -q vector
systemctl status vector
journalctl -u vector -n 50
```

### 3. Regression Tests

**Test on existing platforms**:
```bash
# Ubuntu/Debian (DEB-based)
uv run sct.py run-test artifacts_test \
  --backend gce \
  --config test-cases/artifacts/ubuntu22.yaml

# Rocky Linux 9 (RPM-based, should also work)
uv run sct.py run-test artifacts_test \
  --backend gce \
  --config test-cases/artifacts/rocky9.yaml
```

### 4. Manual Verification

Test the standalone script:
```bash
# Test on Rocky 10 node
python3 install_vector.py --pkg-type rpm --arch amd64
```

## Files to Modify

| File | Changes | Lines Affected |
|------|---------|----------------|
| `sdcm/cluster_cloud.py` | Update `download_vector_locally()` signature and implementation | ~150-165 |
| `sdcm/cluster_cloud.py` | Update `configure_remote_logging()` to detect OS type | ~350-368 |
| `sdcm/provision/common/utils.py` | Update `install_vector_from_local_pkg()` to handle RPM | ~394-416 |
| `install_vector.py` | Update helper script for consistency (optional) | ~10-25 |

## Backwards Compatibility

- **DEB-based systems** (Ubuntu, Debian): No impact, existing logic preserved
- **Existing tests**: No changes needed to test configurations
- **Default behavior**: Falls back to DEB if detection fails (safe default)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| RPM package URL incorrect | Low | High | Verify URL pattern from Vector docs; add error handling |
| Architecture mapping issues | Medium | Medium | Test on both x86_64 and aarch64; normalize arch names |
| Package manager not available | Low | Medium | Add fallback to repository-based installation |
| Dependency resolution fails | Low | Medium | Use retries with backoff; log detailed error messages |

## Alternative Approaches Considered

### Option 1: Use Repository-Based Installation Only

Instead of local packages, always use `install_vector_service()` which already handles both YUM and APT.

**Pros**:
- Already tested and working
- Handles dependencies automatically
- Less code changes

**Cons**:
- Requires internet connectivity on nodes
- Depends on Vector repository availability
- Slower than local package installation
- Inconsistent with current approach for DEB systems

**Decision**: Rejected - We want to maintain consistency with the local package approach used for DEB systems.

### Option 2: Cloud-Init Based Installation

Move Vector installation to cloud-init/user-data scripts.

**Pros**:
- Runs during instance provisioning
- No SSH required

**Cons**:
- Harder to debug failures
- Less flexible
- Doesn't work for Scylla Cloud nodes with limited access

**Decision**: Rejected - Current SSH-based approach is more maintainable.

## Success Criteria

1. ✅ Vector successfully installs on Rocky Linux 10 GCE instances
2. ✅ Vector service starts and runs without errors
3. ✅ Logs are correctly forwarded to SCT runner
4. ✅ No regression on Ubuntu/Debian systems
5. ✅ No regression on Rocky Linux 8/9 systems
6. ✅ Unit tests pass
7. ✅ Integration test completes successfully

## Implementation Checklist

- [ ] Update `download_vector_locally()` function in `sdcm/cluster_cloud.py`
- [ ] Update `install_vector_from_local_pkg()` function in `sdcm/provision/common/utils.py`
- [ ] Update `configure_remote_logging()` method in `sdcm/cluster_cloud.py`
- [ ] Add unit tests for package type detection and script generation
- [ ] Run integration test on Rocky Linux 10
- [ ] Run regression tests on Ubuntu 22.04
- [ ] Run regression tests on Rocky Linux 9
- [ ] Update documentation if needed
- [ ] Code review and approval
- [ ] Merge to master

## Related Issues

- Vector installation uses repository-based approach in `install_vector_service()` which already works on Rocky
- This fix aligns the local package installation approach with the repository-based one

## References

- [Vector Installation Documentation](https://vector.dev/docs/setup/installation/)
- [Vector Package Repository](https://packages.timber.io/vector/latest/)
- [Vector Setup Script](https://setup.vector.dev) - Confirms Rocky Linux support
- [SCT-67 Jira Issue](https://scylladb.atlassian.net/browse/SCT-67)
