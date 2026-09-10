# Capacity Error Signatures

How each AWS capacity failure is raised in SCT, whether the framework recovers from it, and what it looks
like in Argus. Detection must be based on Argus events rather than raw logs, because a string that appears
in a log may belong to an attempt the framework then recovered from.

## Raise Sites

### CapacityReservationError — no availability zone

`sdcm/provision/aws/capacity_reservation.py:201`

```python
raise CapacityReservationError("Failed to create capacity reservation in any availability zone.")
```

`SCTCapacityReservation.create()` walks every supported AZ in the region and requests a reservation for each
instance type. If any instance type fails in an AZ it cancels that AZ's partial reservations and falls back
to the next one. This exception is only raised after **every** AZ has been tried, so it is genuine regional
capacity exhaustion for the requested instance types. This is the dominant signature in practice: in a
12-week sweep of the enterprise perf suite, all 77 fatal capacity failures came from this line.

**Remediation:** different region, different instance type, or a smaller cluster.

### CapacityReservationError — placement group

`sdcm/provision/aws/capacity_reservation.py:156`

```python
raise CapacityReservationError("Failed to find available placement group.")
```

Raised when `use_placement_group` is set but `describe_placement_groups` returns nothing in the `available`
state for the test's `TestId` tag. Nothing to do with capacity — the placement group was never created, was
left in a bad state, or was cleaned up early.

**Remediation:** inspect provisioning order and resource cleanup, not AWS capacity.

Distinguish the two by matching on the message text, not the exception type. Reporting them together sends
people looking for capacity that was never the problem.

### ProvisioningCapacityExhausted

Caught alongside `CapacityReservationError` at `sdcm/sct_provision/aws/layout.py:137`. Both feed the AZ and
region fallback, so an occurrence usually does **not** end the run.

### InsufficientInstanceCapacity

The raw EC2 API error, surfaced through botocore. Normally wrapped by one of the above before it reaches the
test, so it rarely appears as a standalone fatal signature.

## Fallback Behaviour

`sdcm/tester.py:2059` catches `CapacityReservationError` from a region attempt and marks the region
exhausted rather than failing:

```python
try:
    region_exhausted, attempt_error, _ = self._attempt_region_legacy(loader_info, db_info, monitor_info)
except CapacityReservationError as exc:
    region_exhausted, attempt_error = True, exc
```

The run only dies once every candidate region is exhausted. Two consequences for measurement:

1. A capacity error in the log is not a failed run. Only the final status counts.
2. When a run *does* die, the region recorded in Jenkins is the region it *started* in, which may not be the
   last one it tried. Treat the region attribution as "the region this job was aimed at", which is the right
   granularity for deciding where to schedule it anyway.

## Argus Representation

A fatal capacity failure has both of these:

- Run `status == "test_error"`
- A CRITICAL `TestFrameworkEvent` whose message matches the signature

```console
$ argus run events --run-id 15ecbb63-1842-4ed3-8760-d411a37d57f4
[
  {
    "severity": "CRITICAL",
    "event_type": "TestFrameworkEvent",
    "message": "(TestFrameworkEvent Severity.CRITICAL) Failed to provision aws resources: CapacityReservationError: Failed to create capacity reservation in any availability zone."
  }
]
```

`argus run events` returns only CRITICAL and ERROR events, and only for SCT-plugin runs; other plugin types
need `argus run activity`. `argus run activity` is empty for these runs, so events are the only usable
source.

## Counting Rules

| Rule | Why |
|------|-----|
| Require final status `test_error` | Excludes runs the AZ/region fallback rescued |
| Require severity CRITICAL | ERROR-level capacity messages are retried attempts |
| Match the message, not just the exception name | Separates the two `CapacityReservationError` variants |
| Count distinct `build_job_url` as well as runs | Provisioning retries create several runs per Jenkins build |
| Verify the signature mix before reporting | If one signature is 100% of hits, say so; it changes the remediation |
