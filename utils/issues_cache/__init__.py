"""Referenced-only GitHub issue cache (QAINFRA-104 / SCT-1172).

Instead of crawling every issue and PR of every repo, cache only the issues that test
code actually references, and mark each one as in use. Run it as
`python -m utils.issues_cache <command>`:

    scan            Scan this repo's branches for issue references and write the ref
                    manifest (`refs.py`). dtest runs its own scanner and publishes its own
                    manifest, so neither repo needs to read the other's source.
    refresh-github  Read the union of all manifests, fetch every GitHub ref, write the
                    reader-compatible CSVs and mark each item as in use (`github.py`).
    compare         Shadow check: compare the new CSVs with the old full cache.

How an item is marked as in use (by `refresh-github`, reconciled every run):

- where the token may label the repo: the `used-by-sct` / `used-by-dtest` label, the
  same names dtest puts on Jira issues;
- elsewhere (labeling needs triage permission): one comment per item, found again by
  the hidden `<!-- sct-issue-cache -->` marker and edited in place, never posted twice.

Manifest format (version 1). dtest writes the same format, so change it in both repos
together. One file per consumer repo, in `s3://<bucket>/issues/refs/<repo>.json`:

    {
      "version": 1,
      "repo": "scylladb/scylla-cluster-tests",
      "label": "used-by-sct",
      "label_description": "Issue state controls SCT test skips (SkipPerIssues)",
      "generated_at": "2026-10-06T00:00:00+00:00",
      "refs": {
        "scylladb/scylladb#1234": [
          {"branch": "master", "file": "sdcm/tester.py"}
        ],
        "jira:SCYLLADB-42": [
          {"branch": "branch-2026.1", "file": "sdcm/nemesis/__init__.py"}
        ]
      }
    }

CSV contract (what `sdcm/utils/issues.py` and dtest's `tools/github_issues.py` read):

- `<owner>_<repo>.csv`: no header, rows `number,state,labels(|-joined),title`;
- `pull-requests/<owner>_<repo>.csv`: must exist, left empty. Readers concatenate the
  two files without a newline, so every row goes into the issues file.

Local testing (stdlib + the `gh` CLI):

    python -m utils.issues_cache scan --repo-dir . --remote upstream --out /tmp/refs/sct.json
    GH_TOKEN=$(gh auth token) python -m utils.issues_cache refresh-github \\
        --refs-dir /tmp/refs --out-dir /tmp/out --labels dry-run
"""
