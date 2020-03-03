# scylla-cluster-tests events

## Overview

![Overview](./sct_events.png?raw=true "Overview")

### Event Analyzer

Recently we introduced a background thread (`sdcm/sct_events_analyzer.py`) that stops the test on:
 * critical events
 * stress commands failures

Cause of that `DatabaseLogEvent` default severity was lowered to `ERROR` level

### Event Levels

* `NORMAL = 1` - as example, start of a Nemesis or start of a Stress Thread
* `WARNING = 2` - event that we should consider checking, but not fail the tests cause of them.
* `ERROR = 3` - will fail the test, but isn't considered for `keep-on-failure`, i.e. resources would be deleted.
* `CRITICAL = 4` - will send a signal (SIGUSR2) and would stop the test immediately, when `keep-on-failure` is used instances will be saved
