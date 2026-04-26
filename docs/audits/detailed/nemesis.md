# Audit Report: Nemesis (`sdcm/nemesis/`)

**Date:** April 2025
**Severity Summary:** 0 Critical, 1 High, 4 Medium, 2 Low

## Overview

The nemesis subsystem is well-architected — clean separation between `NemesisBaseClass` (flag-bearing subclasses), `NemesisRunner` (disruption logic), `NemesisRegistry` (discovery/filtering), and `NemesisNodeAllocator` (thread-safe node locking). The package structure supports auto-discovery and the flag-based filtering system is elegant. Issues found are robustness and maintainability concerns, not architectural problems.

## High Findings

### H1. Fragile Index-Based Column Selection in Index Nemesis

**File:** `sdcm/nemesis/utils/indexes.py`
**Severity:** High
**Impact:** Wrong column selected if schema changes; potential data corruption in test validation

The index nemesis utilities select columns by positional index (e.g., `columns[2]`) rather than by name or type. If the table schema changes (columns added, reordered, dropped), the nemesis operates on the wrong column. This produces misleading test results — the test may pass when the intended column was never tested, or fail for the wrong reason.

**What to fix:** Select columns by name pattern or type, not by position. Add schema validation before operating on columns.

---

## Medium Findings

### M1. Inconsistent Error Handling Across Disruptions

**File:** `sdcm/nemesis/monkey/runners.py`
**Severity:** Medium

Different `disrupt_*` methods handle errors differently:
- Some catch specific exceptions and report them as nemesis failures
- Some catch broad `Exception` and log warnings
- Some let exceptions propagate, crashing the nemesis thread

This inconsistency means some nemesis failures are properly reported while others silently stop the nemesis thread or are logged at a level that's easy to miss.

### M2. Node Allocator Timeout Handling

**File:** `sdcm/nemesis/utils/node_allocator.py`
**Severity:** Medium

`NemesisNodeAllocator` uses thread-safe locking to prevent conflicting nemesis on the same node. However, if a nemesis thread hangs (e.g., waiting for a node that never comes back), the lock is held indefinitely, blocking other nemesis threads that target the same node.

### M3. Missing Nemesis Precondition Validation

**File:** `sdcm/nemesis/monkey/runners.py`
**Severity:** Medium

Some disruption methods assume certain cluster state (e.g., minimum number of nodes, specific topology) without validating preconditions. If run on a cluster that doesn't meet these assumptions, they fail with confusing errors rather than clear "precondition not met" messages.

### M4. Long Disruption Methods

**File:** `sdcm/nemesis/monkey/runners.py`
**Severity:** Medium

Several `disrupt_*` methods exceed 100 lines, mixing setup, execution, verification, and cleanup. This makes them hard to understand and maintain.

---

## Low Findings

### L1. Hardcoded Wait Times in Disruptions

**File:** `sdcm/nemesis/monkey/runners.py`
**Severity:** Low

Many disruption methods have hardcoded `time.sleep()` or `wait_for(timeout=N)` values. These are usually reasonable defaults but aren't configurable for different cluster sizes or hardware profiles.

### L2. Duplicated Utility Logic

**File:** `sdcm/nemesis/`
**Severity:** Low

Some utility patterns (e.g., "pick a random node, exclude seed nodes, wait for operation, verify") are repeated across multiple disruption methods with slight variations. These could be extracted into shared helpers.
