# Performance Tests Region Scheduling

## Problem

There are performance tests that are triggered on different schedules:
- Weekly
- Once per 3 weeks
- Monthly
- On every release (unpredictable timing)

We need to assign regions so that tests do not overlap in the same region at the same time.
Release tests may overlap with monthly triggered tests, but NOT with weekly or 3-weekly tests.

## Original Assignment (before change)

```
MASTER (monthly): scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-vnodes - us-east-1
RELEASE: scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-vnodes - us-east-1

MASTER (monthly): scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-vnodes - us-east-2
RELEASE: scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-vnodes - us-east-2

MASTER (weekly): scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-tablets - us-east-1
RELEASE: scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-tablets - us-east-1

MASTER (3-weeks): scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-i8g-tablets - us-west-2
RELEASE: scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-i8g-tablets - us-west-2

MASTER (3-weeks): scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-tablets - us-east-2
RELEASE: scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-tablets - us-east-2
```

## Available Regions

```
us-east-1
us-east-2
us-west-2
eu-west-2
eu-west-3
eu-north-1
```

## Test Schedule and Triggers

```
Master weekly (master-weekly):
- predefined-throughput-steps-i8g-tablets — versions: ['master'], labels: ['master-weekly'], all 4 sub tests

Master 3-weeks (master-3weeks):
- latency-650gb-during-rolling-upgrade-i8g-tablets — versions: ['master'], labels: ['master-3weeks'], mixed load
- latency-650gb-with-nemesis-i8g-tablets — versions: ['master'], labels: ['master-3weeks'], mixed load

Master monthly (master-monthly):
- predefined-throughput-steps-i8g-vnodes — versions: ['master'], labels: ['master-monthly'], all 4 sub tests
- latency-650gb-with-nemesis-i8g-vnodes — versions: ['master'], labels: ['master-monthly'], all 3 sub tests (mixed, read, write)

>= Scylla version 2025.3 (non-master):
- predefined-throughput-steps-i8g-tablets — ignore_versions: ['2025.2', '2025.1', '2024.1', '2024.2', 'master'], all sub tests
- latency-650gb-during-rolling-upgrade-i8g-tablets — ignore_versions: ['2025.2', '2025.1', '2024.2', '2024.1', 'master'], mixed load
- latency-650gb-with-nemesis-i8g-tablets — ignore_versions: ['2025.2', '2025.1', '2024.2', '2024.1', 'master'], read + mixed
- predefined-throughput-steps-i8g-vnodes — ignore_versions: ['2025.2', '2025.1', '2024.2', '2024.1', 'master'], mixed only
- latency-650gb-with-nemesis-i8g-vnodes — ignore_versions: ['2025.2', '2025.1', '2024.2', '2024.1', 'master'], mixed only
```

## Final Region Assignment

| Test | MASTER region | RELEASE region |
|------|---------------|----------------|
| scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-vnodes (monthly) | eu-west-2 | eu-west-2 |
| scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-vnodes (monthly) | eu-west-3 | eu-west-1 |
| scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-tablets (weekly) | us-east-1 | us-west-2 |
| scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-i8g-tablets (3-weeks) | us-east-2 | eu-west-3 |
| scylla-enterprise-perf-regression-latency-650gb-with-nemesis-i8g-tablets (3-weeks) | eu-north-1 | eu-west-2 |
