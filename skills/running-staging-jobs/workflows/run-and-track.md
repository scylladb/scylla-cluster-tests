# Run and Track Staging Jobs

Step-by-step process for running staging Jenkins jobs and tracking results for an SCT PR.

## Phase 1: Generate Staging Jobs

**Entry**: PR exists on GitHub with branch pushed to remote.

1. Run `python3 ./staging_trigger.py generate --pr <PR_NUMBER>`
2. Verify output lists the expected job paths
3. Note the job short paths for triggering

**Exit**: Staging jobs exist on Jenkins for this PR.

## Phase 2: Determine Trigger Parameters

**Entry**: Know which test types to run (online vs offline, x86 vs ARM).

1. **Online tests**: Use `--set scylla_version=master:latest`
2. **Offline tests**: Look up current unified package from S3:
   ```bash
   aws s3 ls s3://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/ | grep unified
   ```
3. Construct the full S3 URL: `https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/latest/<filename>`
4. Verify the URL returns 200 (not 404) before triggering

**Exit**: Have the correct `--set` parameter for each job.

## Phase 3: Trigger Jobs

**Entry**: Jobs generated, parameters determined.

1. Trigger each job with `staging_trigger.py trigger`:
   ```bash
   python3 ./staging_trigger.py trigger --pr <N> --set <key>=<value> <short-job-path>
   ```
2. Record the build number from output (e.g., `#5`)
3. Note the full Jenkins URL for PR tracking

**Exit**: All jobs queued/running with known build numbers.

## Phase 4: Monitor Progress

**Entry**: Jobs triggered, build numbers known.

1. Poll build status periodically:
   ```bash
   /home/fruch/.local/bin/jenkins build-info --job <full-path> --build <N>
   ```
2. Parse JSON output: check `result` (null=running, SUCCESS, FAILURE) and `building` (true/false)
3. Artifact tests typically complete in 15-25 minutes
4. Batch-check all jobs in a loop for efficiency

**Exit**: All builds reach terminal state.

## Phase 5: Diagnose Failures

**Entry**: One or more builds show `result=FAILURE`.

1. Download console log:
   ```bash
   /home/fruch/.local/bin/jenkins console-log --job <full-path> --build <N> --dest /tmp/opencode
   ```
2. Search for error patterns:
   ```bash
   grep -n "ERROR\|Failed\|Exception\|assert" /tmp/opencode/<logfile> | grep -vi "Argus\|send-email\|CORRUPTED"
   ```
3. Identify root cause category:
   - **Infrastructure** (disk, network, image not found) → fix config/provisioner
   - **Stale parameter** (404 on URL, wrong version) → update trigger parameter
   - **Test bug** (assertion failure in test logic) → fix test code
   - **Scylla bug** (crash, timeout in Scylla) → report upstream
4. Document the failure reason for PR description

**Exit**: Root cause identified for each failure.

## Phase 6: Fix and Retry

**Entry**: Root cause known, fix implemented.

1. Apply fix (code change, config change, or parameter correction)
2. If code/config changed: commit, push, then retrigger
3. If parameter-only: retrigger with corrected `--set` value
4. Retrigger failed jobs only (don't re-run passing jobs)
5. Return to Phase 4 to monitor

**Exit**: All retried builds pass (or new failure identified → repeat Phase 5).

## Phase 7: Update PR Description

**Entry**: All builds passing.

1. Edit PR body with `gh pr edit <N> --body "..."`
2. Include sections:
   - **Staging Runs**: Final passing builds with ✅ and links
   - **Previous Failures**: Document what failed and why (for reviewer context)
3. Use emoji conventions: ✅ pass, ❌ fail, 🕐 running
4. Group by test type (online/offline) and architecture (x86/ARM)

**Exit**: PR description reflects all staging results with links and status.
