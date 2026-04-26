# SCT Codebase Health Audit — Summary Report

**Date:** April 2025
**Scope:** Core framework (`sdcm/`), Skills (`.claude/skills/`)
**Baseline:** All 1,741 unit tests pass, zero lint issues from `ruff`

## Executive Summary

The SCT codebase is **functionally healthy** — tests pass, linting is clean, and the framework successfully orchestrates complex multi-cloud test infrastructure. However, six subsystem audits revealed **7 critical**, **9 high**, **22 medium**, and **7 low** severity findings concentrated in resource lifecycle management, error handling, and defensive coding gaps.

The most urgent issues are **resource leaks** (sockets, SSH threads, AWS EMR clusters) and **missing defensive coding** in the REST client. These affect production reliability under failure conditions that don't surface in normal test runs.

## Findings by Area

| Area | Critical | High | Medium | Low | Top Risk |
|------|----------|------|--------|-----|----------|
| [Remote Execution](detailed/remote-execution.md) | 3 | 3 | 4 | 0 | Socket leak, unbounded memory, API key exposure |
| [Provisioning & Utils](detailed/provisioning-utils.md) | 2 | 2 | 5 | 1 | EMR cluster leak, hardcoded AWS account ID |
| [Config, REST & Tester](detailed/config-rest-tester.md) | 2 | 1 | 3 | 1 | REST client has no timeout/retry, `silence()` swallows all exceptions |
| [Cluster Core](detailed/cluster.md) | 0 | 2 | 6 | 3 | Class-level state leaks between test instances, 7000-line god class |
| [Nemesis](detailed/nemesis.md) | 0 | 1 | 4 | 2 | Fragile index-based column selection, inconsistent error handling |
| [Skills Directory](detailed/skills-directory.md) | — | — | — | — | 2 skills with structural issues, 4 orphaned files |

## Cross-Cutting Patterns

### 1. Broad Exception Swallowing (~100+ instances)

Bare `except Exception` blocks appear across every subsystem. Many log the error but unconditionally continue, masking failures that should propagate. The worst offender is `tester.py`'s `silence()` context manager, which swallows _all_ exceptions during teardown.

### 2. Resource Lifecycle Gaps

Multiple components create resources (sockets, threads, SSH connections, cloud instances) without guaranteed cleanup:
- `RemoteCmdRunnerBase._init_socket()` — leaks socket on `connect()` failure
- `SSHReaderThread` — unbounded `BytesIO` buffer, no size cap
- `EmrProvisionerBase` — creates EMR clusters + IAM roles with **no** `destroy()` method
- SSH threads lack daemon flags or join timeouts

### 3. Missing Defensive Coding in REST Client

`sdcm/rest/rest_client.py` issues HTTP requests with:
- No response status code validation
- No request timeouts
- No retry logic
- No structured error handling

This affects every REST-based operation (storage service, compaction manager, RAFT API).

### 4. Configuration Complexity

`sct_config.py` is a ~4,500-line file with:
- A single monolithic dict of ~300+ parameters
- Scattered validation logic
- Multiple overlapping multi-tenant configuration sources
- No schema-level validation

## Recommended Triage Order

Work these area-by-area, one at a time:

1. **Remote Execution** — Socket leak and API key exposure are the highest-impact critical findings
2. **REST Client** — Zero defensive coding affects all REST operations
3. **EMR Provisioner** — Missing cleanup leaks real AWS resources and costs money
4. **Tester `silence()`** — Swallowing all teardown exceptions masks real failures
5. **Cluster.py refactoring** — Structural debt, not urgent but compounds over time
6. **Nemesis hardening** — Lower severity, mostly robustness improvements
7. **Skills directory cleanup** — Housekeeping, lowest priority

## Detailed Reports

- [Remote Execution](detailed/remote-execution.md)
- [Provisioning & Utils](detailed/provisioning-utils.md)
- [Config, REST & Tester](detailed/config-rest-tester.md)
- [Cluster Core](detailed/cluster.md)
- [Nemesis](detailed/nemesis.md)
- [Skills Directory](detailed/skills-directory.md)
