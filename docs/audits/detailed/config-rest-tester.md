# Audit Report: Config, REST & Tester (`sdcm/sct_config.py`, `sdcm/rest/`, `sdcm/tester.py`)

**Date:** April 2025
**Severity Summary:** 2 Critical, 1 High, 3 Medium, 1 Low

## Critical Findings

### C1. REST Client Has No Timeout, Retry, or Status Validation

**File:** `sdcm/rest/rest_client.py`
**Severity:** Critical
**Impact:** Hung requests, silent failures, cascading test timeouts

The REST client used for all Scylla REST API interactions (storage service, compaction manager, RAFT) issues HTTP requests with:
- **No response status code checking** — a `500 Internal Server Error` is silently treated as success
- **No request timeout** — a hung Scylla node causes the client to block indefinitely
- **No retry logic** — transient failures (502, 503) immediately fail the operation
- **No structured error handling** — exceptions from `requests` propagate raw

This is the foundation for all REST-based operations in the framework. Every REST call inherits these deficiencies.

**What to fix:** Add configurable timeouts (connect + read), response status validation with meaningful error messages, retry with exponential backoff for transient errors, and structured exception types.

---

### C2. `silence()` Context Manager Swallows All Exceptions

**File:** `sdcm/tester.py`
**Severity:** Critical
**Impact:** Real failures hidden during test teardown; tests appear to pass when cleanup fails

The `silence()` context manager in `ClusterTester` is used during `tearDown()` to wrap cleanup operations. It catches **all** exceptions unconditionally and logs them at a level that's easily missed. This means:
- Failed cluster cleanup is silently ignored
- Resource leaks from failed teardown are invisible
- Post-test verification that something was properly cleaned up is unreliable

**What to fix:** Replace blanket exception swallowing with:
1. Catch specific expected exceptions during cleanup
2. Log unexpected exceptions at ERROR level
3. Optionally collect and re-raise after all cleanup steps complete (multi-exception pattern)

---

## High Findings

### H1. `ClusterTester` Uses Class-Level Variables

**File:** `sdcm/tester.py`
**Severity:** High
**Impact:** State leaks between test instances in the same process

`ClusterTester` defines several attributes at the class level (e.g., cluster references, configuration caches). In pytest's default mode, if multiple test classes inherit from `ClusterTester` and run in the same process, class-level state from one test can leak into another.

**What to fix:** Move class-level attributes to `__init__` or `setUp`. Ensure each test instance has isolated state.

---

## Medium Findings

### M1. `sct_config.py` Is a 4,500-Line Monolith

**File:** `sdcm/sct_config.py`
**Severity:** Medium
**Impact:** Hard to maintain, test, and extend configuration handling

The entire configuration system — ~300+ parameters, validation logic, multi-source merging, type coercion — lives in a single file. Key issues:
- No schema-level validation (individual validators are scattered)
- Configuration precedence logic is complex and hard to trace
- Adding a new parameter requires understanding the entire file
- No configuration documentation generation from schema

### M2. REST Client Module Has No Base Error Class

**File:** `sdcm/rest/`
**Severity:** Medium
**Impact:** Callers can't catch REST-specific errors generically

There's no `ScyllaRestError` or similar base exception. Callers must catch raw `requests.exceptions` or generic `Exception`, making error handling imprecise.

### M3. Test Fixture Lifecycle in Tester

**File:** `sdcm/tester.py`
**Severity:** Medium
**Impact:** Complex setup/teardown ordering issues

`ClusterTester`'s `setUp` and `tearDown` methods are long and have implicit ordering dependencies. Some operations in teardown depend on resources that may have already been cleaned up, leading to secondary errors that obscure the real failure.

---

## Low Findings

### L1. Configuration Parameter Documentation Drift

**File:** `sdcm/sct_config.py`
**Severity:** Low
**Impact:** Docs may not match actual parameter behavior

Some configuration parameters have docstrings that don't match their current behavior (e.g., default values mentioned in docs differ from actual defaults in code). The pre-commit hook regenerates `docs/configuration_options.md`, but inline docstrings aren't auto-validated.
