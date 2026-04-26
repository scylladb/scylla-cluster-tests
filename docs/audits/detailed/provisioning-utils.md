# Audit Report: Provisioning & Utils (`sdcm/provision/`, `sdcm/utils/`)

**Date:** April 2025
**Severity Summary:** 2 Critical, 2 High, 5 Medium, 1 Low

## Critical Findings

### C1. EMR Provisioner Has No Cleanup Method

**File:** `sdcm/provision/aws/emr_provision.py`
**Severity:** Critical
**Impact:** Leaked AWS EMR clusters and IAM roles accumulate cost indefinitely

`EmrProvisionerBase` creates EMR clusters and associated IAM roles but has **no** `destroy()`, `cleanup()`, or `terminate()` method. If a test fails or is interrupted, the EMR cluster continues running with no automated way to clean it up. Each leaked cluster incurs ongoing AWS charges.

**What to fix:** Implement a `destroy()` method that terminates the EMR cluster and deletes associated IAM roles. Register it with the test framework's cleanup hooks.

---

### C2. Hardcoded AWS Account ID

**File:** `sdcm/provision/aws/emr_provision.py`
**Severity:** Critical
**Impact:** Security exposure; breaks portability across AWS accounts

An AWS account ID is hardcoded in the provisioner source code. This is a security concern (account IDs should not be in source control) and prevents the framework from being used with different AWS accounts without code changes.

**What to fix:** Move the account ID to configuration (environment variable or SCT config parameter). Add it to `.gitignore`-protected config files if needed.

---

## High Findings

### H1. OCI Provisioner Error Handling Gaps

**File:** `sdcm/provision/oci/`
**Severity:** High
**Impact:** Orphaned OCI resources on provisioning failures

The OCI provisioner's error handling during instance creation doesn't always clean up partially-created resources. If instance creation succeeds but post-creation configuration fails, the instance is left running without being tracked by the framework.

**What to fix:** Implement transactional provisioning: track all created resources and roll back on any failure during the creation pipeline.

---

### H2. Azure Provisioner Missing Retry Logic

**File:** `sdcm/provision/azure/`
**Severity:** High
**Impact:** Transient Azure API failures cause test failures

Azure API calls in the provisioner have no retry logic. Azure APIs are known for transient `429 Too Many Requests` and `503 Service Unavailable` responses. Without retries, these transient issues cause hard test failures.

**What to fix:** Add exponential backoff retry for Azure API calls, especially for resource creation and status polling.

---

## Medium Findings

### M1. `common.py` Is a Utility Dumping Ground

**File:** `sdcm/utils/common.py`
**Severity:** Medium
**Impact:** Maintenance burden, hard to find functions, circular dependency risk

`common.py` is a ~3,000-line file containing unrelated utility functions (AWS helpers, string processing, file operations, network utilities). This makes it hard to maintain, test, and understand. Functions in this file have different dependency requirements.

### M2. Docker Utils Missing Container Cleanup

**File:** `sdcm/utils/docker_utils.py`
**Severity:** Medium
**Impact:** Orphaned containers from failed operations

Docker operations that create containers don't always have corresponding cleanup. If an operation fails mid-way, containers may be left in stopped state, consuming disk space.

### M3. AWS Region Handling Inconsistency

**File:** `sdcm/provision/aws/`
**Severity:** Medium
**Impact:** Region mismatch errors in multi-region setups

AWS region handling is inconsistent across different provisioning modules. Some use `boto3.Session().region_name`, some read from SCT config, and some have hardcoded defaults. Multi-region test configurations can hit region mismatch errors.

### M4. No Dry-Run Mode for Provisioning

**File:** `sdcm/provision/` (all backends)
**Severity:** Medium
**Impact:** No way to validate provisioning config without creating real resources

There's no dry-run or validation-only mode. Misconfigured test cases are only caught when actual cloud resources are created, wasting time and money.

### M5. `remote_logger.py` Unbounded Log Buffer

**File:** `sdcm/utils/remote_logger.py`
**Severity:** Medium
**Impact:** Memory growth during long tests with verbose logging

The remote logger buffers log lines without a size cap. During long-running tests with verbose database logging, this can consume significant memory.

---

## Low Findings

### L1. Deprecated Boto3 API Usage

**File:** `sdcm/utils/aws_utils.py`
**Severity:** Low
**Impact:** Future compatibility risk

Some AWS API calls use deprecated parameter names or older API versions. These still work but will eventually be removed by AWS.
