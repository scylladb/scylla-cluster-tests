# Mini-Plan: Decrypt SSTables Before Collecting

**Date:** 2026-03-26
**Estimated LOC:** ~350
**Related Issue:** SCYLLADB-1243

## Problem

SCT collects corrupted/malformed sstables during tests for post-mortem investigation. Since most
tests now run with encryption at rest enabled, the collected sstables are encrypted and unusable
for debugging. We need to decrypt them before uploading to S3.

## Approach

- Add `strip_encryption_options_from_schema(schema_text)` helper in `sstable_utils.py` that
  removes `scylla_encryption_options` lines from schema.cql content using regex
- Add `decrypt_sstables_on_node(node, snapshot_path, keyspace, table)` function in
  `sstable_utils.py` that:
  1. Reads `schema.cql` from the snapshot directory
  2. Strips `scylla_encryption_options` if present
  3. Writes modified schema to a temp file on the node
  4. Runs `scylla sstable upgrade --all --schema-file=<modified_schema> <sstable>
     --output-dir=<temp>` for each Data.db file in the snapshot
  5. Replaces original snapshot files with decrypted output
  6. On any failure, logs warning and falls back to encrypted files (non-blocking)
- Integrate into `SSTablesCollector.collect_logs()` — call after `nodetool snapshot`, before
  `upload_remote_files_directly_to_s3()`
- Integrate into `upload_sstables_to_s3()` — call after `nodetool snapshot`, before upload
- Integrate into `SstablesValidator._upload_corrupted_files()` — take a snapshot first, then
  decrypt, then upload
- Add unit tests for `strip_encryption_options_from_schema()` logic

## Files to Modify

- `sdcm/utils/sstable/sstable_utils.py` -- Add `strip_encryption_options_from_schema()` and
  `decrypt_sstables_on_node()`
- `sdcm/logcollector.py` -- Call `decrypt_sstables_on_node()` in `SSTablesCollector.collect_logs()`
  after snapshot, before S3 upload
- `sdcm/utils/sstable/s3_uploader.py` -- Call `decrypt_sstables_on_node()` in
  `upload_sstables_to_s3()` after snapshot, before S3 upload
- `sdcm/teardown_validators/sstables.py` -- Add snapshot step and call decryption in
  `_upload_corrupted_files()` before upload
- `unit_tests/test_sstable_decrypt.py` -- (new) Unit tests for schema stripping

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/test_sstable_decrypt.py -v`
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
- [ ] `strip_encryption_options_from_schema` correctly removes `scylla_encryption_options`
- [ ] Decryption failure doesn't block sstable collection (fallback to encrypted)
