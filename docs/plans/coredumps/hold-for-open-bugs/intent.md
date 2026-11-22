# Keep the coredumps of open bugs

**Jira:** SCT-1656

## Problem

SCT uploads each Scylla coredump to `gs://scylla-sct-coredumps` (SCT-1655).
A lifecycle rule deletes each object 90 days after the upload.
The rule does not know which coredumps a bug uses.
When a bug stays open for more than 90 days, its coredump is deleted.
A developer then cannot load the coredump in gdb to check an analysis or to
continue the investigation, and the test must hit the crash again.

Before SCT-1655, the coredumps went to `upload.scylladb.com`. That bucket now
deletes objects after 14 days, so the problem is already visible there.

## Who it affects

- Scylla developers who debug a crash that a QA test found.
- QA engineers who file the bugs and answer questions on them.
- Automated coredump analysis (QATOOLS-535, a Maia task that Zeus
  dispatches), which needs the coredump while the bug is open.

## Evidence

Open SCYLLADB bugs that link a coredump in `upload.scylladb.com`, the
bucket with 14-day retention (Jira search on 2026-11-22):

```
SCYLLADB-6336  created 2026-11-16  logstor: node crashes with SIGSEGV in seastar::current_tasktrace() ...
SCYLLADB-6331  created 2026-11-16  logstor: node crash-loops on "lsa - failed to refill emergency reserve" ...
SCYLLADB-6330  created 2026-11-16  logstor: node aborts on tablet split completion ...
SCYLLADB-6297  created 2026-11-14  SEGV in digest_read_resolver::on_timeout ... (2026.2.11)
SCYLLADB-5896  created 2026-11-01  Coredump: on_internal_error "Unable to remove input SSTable" ...
SCYLLADB-5865  created 2026-10-30  logstor: node aborts on SEASTAR_ASSERT(!_q.empty()) ...
SCYLLADB-4083  created 2026-09-01  A coredump crash (all 6 DB nodes) in get_node_state()/cluster_status_table ...
SCYLLADB-3542  created 2026-08-03  Memory allocation failure during a decommission and schema change (add column)
SCYLLADB-1763  created 2026-04-29  rebuild takes too long and prevents replace from starting
```

Five of these bugs are older than 14 days. Their coredumps were uploaded
before the bugs were filed, so they are past the 14-day retention. The GitHub search `repo:scylladb/scylladb is:open
"upload.scylladb.com"` finds 15 more open issues, from 2018 to 2025.

Slack thread that started SCT-1655 and SCT-1656 (Israel Fruchter): "Once the
issue attaches to the coredump report it should be saved until the issue is
getting closed".

## Definition of done

- A coredump in `gs://scylla-sct-coredumps` that an open bug links is not
  deleted, also after 90 days.
- One day or less after the bug closes, the coredump goes back to the
  normal retention. When it is older than 90 days, the lifecycle rule deletes it.
- A coredump that no bug links keeps the retention of SCT-1655: Standard for
  14 days, Coldline after that, deleted at 90 days.
- A coredump that a bug links within its first 90 days is kept. The time
  from the link to the protection is one day or less.
- The protection does not depend on a person who remembers to do a step.
- A person can see which coredumps are kept, and for which bug.

## Out of scope

- Coredumps in `upload.scylladb.com`. QA does not own that bucket.
- Coredumps that customers upload, and the CUSTOMER Jira project.
- A coredump that is already deleted when the bug links it.
- Other test artifacts (logs, Argus data, monitoring snapshots).
