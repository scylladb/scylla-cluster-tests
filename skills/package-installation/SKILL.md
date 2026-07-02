---
name: package-installation
description: >-
  Guides writing remote package installation commands (apt-get, yum, dnf, zypper)
  in SCT code. Use when adding apt-get install/update, yum install, dnf install,
  or zypper install calls via remoter.run/remoter.sudo. Ensures timeouts, retries,
  and non-interactive flags are always present to prevent hangs in CI.
---

# Package Installation Commands in SCT

Best practices for running package manager commands on remote nodes via `remoter.run()` / `remoter.sudo()`.

## Why This Matters

Remote package installation can hang indefinitely due to:
- Network timeouts to package repositories
- GPG keyserver unavailability
- dpkg lock contention from unattended-upgrades
- Interactive prompts when running non-interactively

A single hung `apt-get` call can block an entire SCT test for hours/days.

## Mandatory Options by Package Manager

### apt-get (Debian/Ubuntu)

Every `apt-get` call MUST include:

```python
# For apt-get update:
self.remoter.sudo(
    "apt-get -o Acquire::http::Timeout=60 -o Acquire::Retries=3 update",
    ignore_status=True,
)

# For apt-get install:
self.remoter.sudo(
    "DEBIAN_FRONTEND=noninteractive apt-get install "
    "-o DPkg::Lock::Timeout=120 "
    "-o Acquire::http::Timeout=60 "
    "-o Acquire::Retries=3 "
    '-o Dpkg::Options::="--force-confold" '
    '-o Dpkg::Options::="--force-confdef" '
    "-y <package>",
)
```

**Required options:**
| Option | Purpose |
|--------|---------|
| `DEBIAN_FRONTEND=noninteractive` | Prevents interactive prompts (install only) |
| `-o Acquire::http::Timeout=60` | 60s timeout for HTTP downloads — prevents indefinite hangs |
| `-o Acquire::Retries=3` | Retry failed downloads 3 times |
| `-o DPkg::Lock::Timeout=120` | Wait up to 120s for dpkg lock (install only) |
| `-o Dpkg::Options::="--force-confold"` | Keep existing config files (install only) |
| `-o Dpkg::Options::="--force-confdef"` | Use default for new config files (install only) |
| `-y` | Assume yes to all prompts (install only) |

### yum / dnf (RHEL/CentOS/Fedora)

```python
# yum/dnf have built-in timeouts (default 30s) but always use:
self.remoter.sudo("yum install -y <package>", retry=3)

# For long-running operations, add timeout to the remoter call:
self.remoter.sudo("yum install -y <package>", retry=3, timeout=600)
```

**Required options:**
| Option | Purpose |
|--------|---------|
| `-y` | Non-interactive |
| `retry=3` on remoter call | Retry on SSH/command failures |
| `timeout=600` on remoter call | Prevent indefinite hangs (for large packages) |

### zypper (SLES)

```python
self.remoter.sudo("zypper install -y <package>", retry=3)
```

**Required options:**
| Option | Purpose |
|--------|---------|
| `-y` | Non-interactive |
| `retry=3` on remoter call | Retry on failures |

## The `install_package()` Method

Prefer using `BaseNode.install_package()` from `sdcm/cluster.py` which already handles:
- Distro detection (apt vs yum vs zypper)
- Proper timeout/retry options
- `@retrying` decorator for transient failures
- Version pinning

```python
# Preferred:
self.install_package("scylla-manager-agent")

# Only use raw remoter commands when install_package doesn't fit
# (e.g., shell scripts, multi-command pipelines)
```

## Shell Script Blocks

When apt-get commands are embedded in shell scripts (via `shell_script_cmd()` or heredocs), the same options apply:

```python
script = dedent("""
    apt-get -o Acquire::http::Timeout=60 -o Acquire::Retries=3 update
    apt-get -o Acquire::http::Timeout=60 -o Acquire::Retries=3 install -y docker.io
""")
self.remoter.sudo(f"bash -ce '{script}'")
```

## GPG Key Fetching

Use `BaseNode.fetch_apt_keys()` which handles:
- Multiple HKP keyserver fallbacks
- HTTPS fallback
- Timeout on each attempt (`--keyserver-options timeout=10`)
- Raises `NodeSetupFailed` if all sources fail

Never write bare `apt-key adv --recv-keys` without timeout and fallback logic.

## Anti-Patterns

```python
# BAD: No timeout, will hang forever on network issues
self.remoter.sudo("apt-get update")
self.remoter.sudo("apt-get install -y foo")

# BAD: Missing DEBIAN_FRONTEND, may prompt
self.remoter.sudo("apt-get install -y foo")

# BAD: No Acquire timeout, download can hang
self.remoter.sudo("DEBIAN_FRONTEND=noninteractive apt-get install -y foo")

# BAD: ignore_status without retry — silently fails
self.remoter.sudo("apt-get update", ignore_status=True)
```

## Checklist for Code Review

When reviewing code that adds package installation:

- [ ] `Acquire::http::Timeout=60` present on all apt-get calls?
- [ ] `Acquire::Retries=3` present on all apt-get calls?
- [ ] `DEBIAN_FRONTEND=noninteractive` on all apt-get install calls?
- [ ] `-y` flag on all install commands (apt, yum, zypper)?
- [ ] `DPkg::Lock::Timeout` on apt-get install calls?
- [ ] `retry=N` on remoter call for yum/zypper?
- [ ] No bare `apt-key adv` without timeout/fallback?
- [ ] Prefer `install_package()` over raw commands?
