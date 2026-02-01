# Integration Test Timeout Tracking

## Overview

This document describes the timeout tracking and logging improvements added to help identify which integration tests are getting stuck or timing out.

## Problem Statement

Integration tests were timing out (taking more than 45 minutes) without clear indication of which specific test was stuck. Examples included:
- `test_cassandra_stress_thread.py::test_03_cassandra_stress_client_encrypt@docker_heavy` - TimeoutError: 1 (of 1) futures unfinished
- `test_gemini_thread.py::test_01_gemini_thread` - TimeoutError: 1 (of 1) futures unfinished
- `test_vector_store.py::test_vector_search@docker_heavy` - RuntimeError: Vector indexing did not complete within 300 seconds

## Solution

### 1. pytest-timeout Plugin

Added `pytest-timeout==2.3.1` to project dependencies (pyproject.toml). This plugin provides:
- Per-test timeout enforcement
- Configurable timeout methods (thread-based or signal-based)
- Clear timeout error messages with test identification

### 2. pytest.ini Configuration

Updated `unit_tests/pytest.ini` with:
```ini
addopts = --strict-markers --durations=20  --dist loadscope --timeout=3600 --timeout-method=thread
timeout = 3600
timeout_method = thread
```

- **timeout = 3600**: Sets a 1-hour (3600 seconds) timeout for each test
- **timeout_method = thread**: Uses thread-based timeout (safer for integration tests with Docker)

### 3. Enhanced Logging Hooks

Added pytest hooks in `unit_tests/conftest.py` to track and log integration test execution:

#### Test Start Hook (`pytest_runtest_setup`)
- Logs when an integration test begins setup
- Records start time for duration tracking
- Format: `[INTEGRATION TEST START] test_name`

#### Test Call Hook (`pytest_runtest_call`)
- Logs when test execution (call phase) begins
- Shows how long setup took
- Format: `[INTEGRATION TEST CALL] test_name (setup took X.XXs)`

#### Test Teardown Hook (`pytest_runtest_teardown`)
- Logs when teardown phase begins
- Shows total runtime so far
- Format: `[INTEGRATION TEST TEARDOWN] test_name (total runtime so far: X.XXs)`

#### Test Report Hook (`pytest_runtest_makereport`)
- Logs test completion with status
- Shows duration of call phase and total time
- Format: `[INTEGRATION TEST PASSED/FAILED/SKIPPED] test_name (duration: X.XXs)`
- Format: `[INTEGRATION TEST COMPLETE] test_name (total time: X.XXs)`

## Benefits

1. **Clear Test Identification**: Logs show exactly which test is running at any moment
2. **Phase Tracking**: Separate logs for setup, call, teardown phases help identify where tests get stuck
3. **Duration Tracking**: Elapsed time is logged at each phase
4. **Timeout Enforcement**: Tests are automatically terminated after 1 hour with clear error message
5. **Integration Test Focus**: Logging only applies to tests marked with `@pytest.mark.integration`

## Example Output

```
INFO [INTEGRATION TEST START] unit_tests/test_gemini_thread.py::test_01_gemini_thread
INFO [INTEGRATION TEST CALL] unit_tests/test_gemini_thread.py::test_01_gemini_thread (setup took 15.34s)
INFO [INTEGRATION TEST TEARDOWN] unit_tests/test_gemini_thread.py::test_01_gemini_thread (total runtime so far: 125.67s)
INFO [INTEGRATION TEST PASSED] unit_tests/test_gemini_thread.py::test_01_gemini_thread (duration: 110.33s)
INFO [INTEGRATION TEST COMPLETE] unit_tests/test_gemini_thread.py::test_01_gemini_thread (total time: 125.67s)
```

If a test times out after 1 hour:
```
ERROR   pytest_timeout: Test exceeded timeout of 3600 seconds
```

## Usage

The feature is automatically active for all tests marked with `pytest.mark.integration`. No code changes are needed in individual test files.

To disable timeout for a specific test (not recommended):
```python
@pytest.mark.timeout(0)  # Disable timeout
def test_something_long():
    pass
```

To set a different timeout for a specific test:
```python
@pytest.mark.timeout(7200)  # 2 hours
def test_something_very_long():
    pass
```

## CI/Jenkins Integration

The logging output will appear in Jenkins console logs, making it easy to:
1. Identify which test was running when a timeout occurred
2. See how long each phase of the test took
3. Determine if a test is stuck in setup, execution, or teardown

## Related Issues

- Issue: Integration test something times out (take more than 45m)
- Jenkins failures showing "TimeoutError: 1 (of 1) futures unfinished"
- Vector search tests timing out after 300 seconds
