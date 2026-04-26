# Audit Report: Cluster Core (`sdcm/cluster.py`)

**Date:** April 2025
**Severity Summary:** 0 Critical, 2 High, 6 Medium, 3 Low

## Overview

`cluster.py` is the heart of SCT — a ~7,000-line file containing `BaseNode`, `BaseCluster`, `BaseScyllaCluster`, `BaseMonitorSet`, `BaseLoaderSet`, and related classes. It's functionally correct (all tests pass), but carries significant structural debt.

## High Findings

### H1. God Class — `cluster.py` at ~7,000 Lines

**File:** `sdcm/cluster.py`
**Severity:** High
**Impact:** Cognitive load, merge conflicts, hard to test individual components

A single file contains the base classes for nodes, clusters, monitors, and loaders. This creates:
- Frequent merge conflicts when multiple developers work on different cluster types
- Difficulty unit-testing individual behaviors in isolation
- Long import times and circular dependency risks
- Hard to understand for new contributors

**Recommendation:** Consider splitting into `base_node.py`, `base_cluster.py`, `base_monitor.py`, `base_loader.py` as a future refactoring effort.

---

### H2. Deep Inheritance Hierarchy with Fragile Overrides

**File:** `sdcm/cluster.py` + all `cluster_*.py` backends
**Severity:** High
**Impact:** Method signature changes in base classes break backends silently

The cluster hierarchy is 3-5 levels deep (e.g., `BaseCluster` → `BaseScyllaCluster` → `AWSCluster` → specific implementations). Key methods like `add_nodes()`, `_create_instances()`, `wait_for_init()`, and `destroy()` have 5-18+ overrides across backends. Adding a parameter to any of these methods requires updating all overrides — and missing one causes a runtime `TypeError`.

This was a real production issue: PR #13445 added `ami_id` to `_create_instances()` but missed `MonitorSetEKS._create_instances()` in `cluster_k8s/eks.py`. It was caught only after merge (PR #14113).

**Recommendation:** Consider using composition over inheritance for backend-specific behavior, or add automated override-compatibility checks.

---

## Medium Findings

### M1. Broad Exception Handling in Node Operations

**File:** `sdcm/cluster.py`
**Severity:** Medium

Multiple methods catch `Exception` broadly during node operations (start, stop, restart, wait_for_init). While this prevents individual node failures from crashing the entire test, it also masks unexpected errors that should fail the test.

### M2. Implicit State Dependencies Between Methods

**File:** `sdcm/cluster.py`
**Severity:** Medium

Several methods assume other methods have been called first (e.g., `wait_for_init()` assumes nodes are already created, certain methods assume SSH is already configured). These dependencies are documented only in method docstrings (if at all), not enforced programmatically.

### M3. Mixed Abstraction Levels

**File:** `sdcm/cluster.py`
**Severity:** Medium

The same file contains high-level orchestration (`add_nodes` coordinates creation, configuration, and initialization) alongside low-level details (SSH command construction, log parsing). This makes it hard to understand the overall flow.

### M4. Node Status Tracking Is Stringly-Typed

**File:** `sdcm/cluster.py`
**Severity:** Medium

Node status is tracked using string comparisons in multiple places. While there are some enum definitions, not all status checks use them consistently, risking typo-based bugs.

### M5. Long Methods Exceeding 100 Lines

**File:** `sdcm/cluster.py`
**Severity:** Medium

Several methods exceed 100 lines (some over 200), making them hard to understand, test, and maintain. Examples include setup methods, cleanup methods, and node initialization flows.

### M6. Monitoring Integration Tightly Coupled

**File:** `sdcm/cluster.py`
**Severity:** Medium

Monitoring setup (Prometheus targets, Grafana dashboards) is interleaved with cluster lifecycle management. This coupling makes it hard to change monitoring infrastructure without touching cluster code.

---

## Low Findings

### L1. Magic Numbers in Wait/Retry Logic

**File:** `sdcm/cluster.py`
**Severity:** Low

Wait timeouts and retry counts are hardcoded throughout (e.g., `wait_for(timeout=300)`, `retries=5`). These should be constants or configuration parameters.

### L2. Dead Code Paths

**File:** `sdcm/cluster.py`
**Severity:** Low

Some code branches handle backends or configurations that appear unused (e.g., legacy provisioning paths). These add noise and maintenance burden.

### L3. Inconsistent Logging Format

**File:** `sdcm/cluster.py`
**Severity:** Low

Log messages use inconsistent formats — some include node name, some don't; some use f-strings, some use `%`-style formatting. This makes log grep/analysis harder.
