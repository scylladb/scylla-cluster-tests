# Workflow: Generate a Test Run Comparison Report

Step-by-step process for producing a Confluence-compatible HTML comparison page from multiple SCT test run IDs.

## Phase 1: Gather Inputs

**Entry criteria:** User provides 2+ test run UUIDs and describes what differs between runs.

1. Collect all run UUIDs from the user
2. Ask what parameter varies between the runs (instance type, version, config flag, etc.)
3. Confirm the expected output format (HTML for Confluence)

**Exit criteria:** Have all run IDs and know the comparison axis.

## Phase 2: Fetch Run Metadata from Argus

**Entry criteria:** Have valid run UUIDs.

1. Use the Python Argus client (via `.venv/bin/python` with `sdcm.keystore.KeyStore`) to authenticate
2. For each run, GET `/api/v1/client/testrun/scylla-cluster-tests/{run_id}/get`
3. Extract from each run response:
   - `status` (passed/failed)
   - `start_time`, `end_time`
   - `cloud_setup.db_node.instance_type`, `cloud_setup.db_node.node_amount`
   - `config_files`
   - `packages` (ScyllaDB version)
   - `build_job_url`, `build_number`
   - `group_id` (test ID for Argus URL)

**Exit criteria:** Have metadata for all runs, confirm they use the same base test config.

## Phase 3: Fetch Latency Results

**Entry criteria:** Have run metadata including S3 log paths.

1. Try the Argus `fetch_results` endpoint first:
   - GET `{base_url}/api/v1/testrun/scylla-cluster-tests/{run_id}/fetch_results`
   - If 200, parse the JSON response directly
2. If fetch_results returns 502 (common), fall back to S3 log extraction:
   a. Use `boto3` to list objects in `cloudius-jenkins-test` bucket under `{run_id}/` prefix
   b. Download all `.sct-*.log.zst` files
   c. Decompress with `zstd -d`
   d. Search for lines containing `latency_results:` using grep
   e. Parse the Python dict with `ast.literal_eval`
3. For each run, identify the latest log entry per phase:
   - The `latency_results` dict accumulates across subtests
   - Use the entry with the most phases present
4. Extract from each phase's `hdr_summary`:
   - `hdr_summary['WRITE--WRITE-rt']['percentile_99']` -> P99 write
   - `hdr_summary['READ--READ-rt']['percentile_99']` -> P99 read
   - `hdr_summary['WRITE--WRITE-rt']['throughput']` -> Throughput write
   - `hdr_summary['READ--READ-rt']['throughput']` -> Throughput read
   - `duration_in_sec` -> Duration

**Exit criteria:** Have P99 write, P99 read, throughput write, throughput read, and duration for every phase of every run.

## Phase 4: Build the HTML Report

**Entry criteria:** Have all metrics organized by run and phase.

1. Start with the HTML/CSS template from [html-template.md](../references/html-template.md)
2. Build the Overall Results box immediately after the `<h1>` title:
   - An info-box summarizing pass/fail outcome for each run with status badges
   - Bullet list with one-line verdict per run (key metric or failure reason)
   - One-sentence conclusion or recommendation
   - Keep to 5-8 lines; detailed analysis belongs in the phase sections
3. Build the Overview section:
   - Add an info-box that prominently shows what parameter varies
   - Fill the parameters table with values common to all runs
4. Build the Test Runs table:
   - One row per run with the varying parameter, test ID, date, status badge
   - Link the "Run" column to `https://argus.scylladb.com/tests/scylla-cluster-tests/{test_id}`
5. Build per-phase tables:
   - Column headers: linked to Argus run pages
   - Rows: P99 write, P99 read, Throughput write, Throughput read, Duration
   - Apply color coding:
     - P99 > 10 ms: `class="worst"` with `<span class="red">`
     - Best value in a row: `class="best"` with `<strong>`
6. Add callout boxes after phases with notable findings:
   - Use `warning-box` for failures (P99 > 10 ms)
   - Use `info-box` for observations
7. Build Duration Comparison table
8. Build Summary table (one row per phase, highlighting best/worst)

**Exit criteria:** Complete HTML file written to `/tmp/opencode/` with all data filled in.

## Phase 5: Validate

**Entry criteria:** HTML file exists.

1. Verify the Overall Results box is present after the title with correct pass/fail badges
2. Verify all P99 values > 10 ms are marked red
3. Verify all column header links contain correct run UUIDs
4. Verify the info-box in Overview clearly states what varies
5. Confirm no c-s metrics, P95, P90, or per-node data leaked into the tables
6. Verify throughput and P99 values match `hdr_summary` source data

**Exit criteria:** Report is ready for the user to paste into Confluence.
