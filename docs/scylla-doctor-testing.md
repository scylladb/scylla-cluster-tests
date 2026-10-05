# Scylla Doctor testing and release gating

SCT tests Scylla Doctor (SD) in two editions:

- **basic**: the publicly downloadable edition. It has collectors only. Regular
  [artifact tests](./artifacts_test.md) run it on every ScyllaDB build they test.
- **full**: the full edition from a private S3 bucket. It has collectors and analyzers. The
  [SD release gating pipeline](#release-gating-pipeline) runs it to approve a new SD build.

Both editions go through the same `ScyllaDoctor` class (`utils/scylla_doctor.py`) and the same
`run_scylla_doctor()` subtest (`artifacts_test.py`). They differ in where the package comes from and
in whether the analysis phase runs.

## Editions compared

| | Basic | Full |
|---|-------|------|
| Selected by | `scylla_doctor_edition: basic` (default) | `scylla_doctor_edition: full` |
| Contents | Collectors | Collectors and analyzers |
| Package source | Public bucket `downloads.scylladb.com`, prefix `downloads/scylla-doctor/tar/` | Private bucket; name and prefix from the KeyStore (`scylla_doctor_full.json`) |
| Download | Plain HTTPS from `https://downloads.scylladb.com/` | S3 pre-signed URL, valid for 300 seconds |
| Version | `scylla_doctor_version` (newest package when empty) | `scylla_doctor_version`, or the exact tarball in `scylla_doctor_full_tarball_url` |
| Analysis phase | No | Yes |
| Extra SD config | Disabled collectors on nonroot installs | Same, plus `SCYLLA_DOCTOR_ANALYZER_CONFIG` (analyzers turned off or tuned for test clusters) |
| Used by | Regular artifact tests | SD release gating; `scylla-doctor-gating` trigger matrix |

`scylla_doctor_version` is pinned in `defaults/test_default.yaml` (currently `1.14.1`) so that a new
SD release cannot break artifact tests unannounced. See
[Scylla Doctor Version Management](./artifacts_test.md#scylla-doctor-version-management).

The SD options are defined in `sdcm/sct_config/mixins/scylla_doctor.py` and described in
[configuration_options.md](./configuration_options.md):

| Option | Default | Meaning |
|--------|---------|---------|
| `run_scylla_doctor` | `true` | Run the SD subtest in the artifact test |
| `scylla_doctor_edition` | `basic` | `basic` or `full` |
| `scylla_doctor_version` | `1.14.1` | SD version to look up in the bucket |
| `scylla_doctor_full_tarball_url` | empty | Full edition only: exact tarball to download |
| `run_scylla_doctor_only` | `false` | Skip every artifact check except node health and SD |
| `use_scylla_doctor_on_failure` | `true` | Not part of artifact testing; see [SD on test failure](#sd-on-test-failure) |

## Where SD is tested

| Path | Edition | What it validates | Triggered by |
|------|---------|-------------------|--------------|
| Regular artifact tests (`jenkins-pipelines/oss/artifacts/`, release branch folders) | basic | A ScyllaDB build, with the pinned SD version | ScyllaDB package/image builds |
| [SD release gating pipeline](#release-gating-pipeline) (`scylla-doctor/sd-release-gating`) | full | A new SD build, on all supported ScyllaDB versions and OS variants | The SD team, by hand |
| `scylla-doctor-gating` trigger matrix (`configurations/triggers/scylla-doctor-gating.yaml`) | full | A ScyllaDB version, with the pinned full SD version, on AMI, AMI ARM and Docker | `scylla-master/sct_triggers/scylla-doctor-gating-trigger` |

## SD test flow

`run_scylla_doctor()` runs these steps on every DB node, for both editions.

1. **Download and install.** `install_scylla_doctor()` calls `download_scylla_doctor()`, which picks
   the basic or full download path (see below), extracts the tarball and finds the Python
   interpreter bundled with ScyllaDB. It then writes `scylla_doctor.conf` with the disabled
   collectors (nonroot installs) and, for the full edition, the analyzer settings.
2. **Record the SD version** in Argus as the `scylla-doctor` package.
3. **Collect vitals** with `--save-vitals`. `lspci`, `ethtool` and `iptables` are installed first if
   missing. The vitals JSON file must be created. Except on Docker, the `scylla_logs_*.tar.gz`
   archive must be created too.
4. **Load the vitals** with `--load-vitals --verbose`.
5. **Check collectors.** A collector with status `1` (failed) fails the test, unless
   `filter_out_failed_collectors()` lists it as a known exception.
6. **Full edition only: run analyzers** with `--load-vitals --output json` and save the report.
7. **Full edition only: check analyzers.** Any status other than `0` (passed) or `1` (skipped)
   fails the test. `analyze_vitals_report_known_issues()` adds a Jira link to known analyzer
   failures, but they still fail the test.

Steps 6 and 7 run only when the edition is `full` **and** the full package was downloaded
(`ScyllaDoctor.is_full_edition`).

`run_scylla_doctor()` returns without running SD, and the subtest passes, in two cases:

- **Docker backend.** SD is not run on Docker yet.
- **`client_encrypt` enabled**, while [field-engineering#2280](https://github.com/scylladb/field-engineering/issues/2280) is open.

### Where the subtest runs in the artifact test

- **Regular artifact test** (`run_scylla_doctor_only: false`): `test_scylla_service()` runs all
  artifact checks (ENA, IO params, snitch, stop/start, cassandra-stress, housekeeping, perftune and
  so on), then runs the `check scylla_doctor results` subtest last, if `run_scylla_doctor` is true.
- **SD-only mode** (`run_scylla_doctor_only: true`): `test_scylla_service()` runs only
  `verify node health` (`node.check_node_health()`) and `check scylla_doctor results`, then returns.
  The release gating pipeline always uses this mode.

### Basic edition download

`download_scylla_doctor()`:

1. Lists `downloads/scylla-doctor/tar/` in the public `downloads.scylladb.com` bucket.
2. Picks the newest package whose name starts with `scylla-doctor-<scylla_doctor_version>`, or
   the newest package overall when the version is empty. If none match, the test fails with
   `Unable to find scylla-doctor package for version <version>`.
3. The DB node downloads it with `curl` from `https://downloads.scylladb.com/<key>`.

### Full edition download

`download_full_scylla_doctor()`:

1. Reads the full edition bucket and prefix from the KeyStore (`scylla_doctor_full.json`).
2. Finds the package in that bucket, using `scylla_doctor_version`, or the newest package when the
   version is empty. **This lookup runs even when a tarball URL is set.** If it finds nothing, the
   test fails with `Unable to find full scylla-doctor package`.
3. If `scylla_doctor_full_tarball_url` is set, replaces the object key, and the bucket when the URL
   names a different one, with the values parsed from the URL.
4. Creates a pre-signed URL valid for 300 seconds, signed for the bucket's region, and the DB node
   downloads the tarball with `curl`. The node needs no AWS credentials. The SCT runner needs read
   access to the bucket.

The bucket region comes from the bucket name suffix when it has one (for example
`...-eu-central-1`), otherwise from `GetBucketLocation`, otherwise `us-east-1`.
A pre-signed URL signed for the wrong region returns an XML error instead of the tarball.

Both download paths check the gzip magic bytes of the downloaded file and fail with
`... file is not a valid gzip tarball` when it is not one.

#### Tarball URL formats

`ScyllaDoctor._parse_s3_url()` accepts these forms of `scylla_doctor_full_tarball_url`:

| Format | Example |
|--------|---------|
| Bucket as host (used by the SD team) | `https://fe-artifacts-297607762119-eu-central-1/scylla_doctor_staging/tar/scylla-doctor-1.11.4.tar.gz` |
| Path style | `https://s3.amazonaws.com/BUCKET/KEY`, `https://s3.REGION.amazonaws.com/BUCKET/KEY` |
| Virtual-hosted style | `https://BUCKET.s3.amazonaws.com/KEY`, `https://BUCKET.s3.REGION.amazonaws.com/KEY` |
| Bare key, no scheme | `scylla_doctor_staging/tar/scylla-doctor-1.11.4.tar.gz` (uses the KeyStore bucket) |

### Running SD tests by hand

Basic edition, as regular artifact tests run it:

```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

Full edition with a specific tarball, SD checks only, as the release gating pipeline runs it:

```sh
export SCT_SCYLLA_DOCTOR_EDITION=full
export SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL=https://fe-artifacts-297607762119-eu-central-1/scylla_doctor_staging/tar/scylla-doctor-1.11.4.tar.gz
export SCT_RUN_SCYLLA_DOCTOR_ONLY=true
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

In Jenkins, set the same values with the `scylla_doctor_edition`, `scylla_doctor_full_tarball_url`
and `run_scylla_doctor_only` parameters of any artifact job ("Scylla Doctor Configuration" section).

### SD on test failure

`use_scylla_doctor_on_failure` is a separate diagnostic feature, not an SD test. When any SCT test
fails, `tester.py` installs SD on the DB nodes and saves `scylla_doctor_<node>_vitals.json`,
`scylla_doctor_<node>_logs.tar.gz` and, with the full edition, `scylla_doctor_<node>_analysis.json`
to the test logs. SD errors there are logged and do not affect the test result.

## Release gating pipeline

The SD release gating pipeline validates a new SD build before it is released. It runs the full
edition in [SD-only mode](#where-the-subtest-runs-in-the-artifact-test) for every combination of
supported ScyllaDB version and operating system, in parallel, and sends a single pass/fail report.

An SD build is approved only when **every** cell of the OS × ScyllaDB version matrix is `SUCCESS`.
`FAILURE`, `UNSTABLE`, `ABORTED` and a job that could not be triggered all fail the gate.

### Components

| File | Role |
|------|------|
| `jenkins-pipelines/scylla-doctor/sd-release-gating.jenkinsfile` | Jenkins entry point; calls `sdReleaseGatingPipeline()` |
| `vars/sdReleaseGatingPipeline.groovy` | Gating pipeline: version discovery, parallel triggering, report, email |
| `jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` | Declares artifact jobs inside the `scylla-doctor` Jenkins folder |
| `vars/artifactsPipeline.groovy` | Artifact test pipeline; turns the SD job parameters into `SCT_*` environment variables |
| `artifacts_test.py` | `test_scylla_service()`: SD-only mode branch and `run_scylla_doctor()` |
| `utils/scylla_doctor.py` | `ScyllaDoctor`: download, install, collect vitals, analyze, verify |
| `utils/get_supported_scylla_base_versions.py` | `fetch_official_supported_versions()`, used for version auto-discovery |

### Flow

```
sd-release-gating (Jenkins: scylla-doctor/sd-release-gating)
  │
  ├─ Validate Parameters     sd_tarball_url must be set
  ├─ Discover Scylla Versions  scylla_versions, or `hydra get-official-supported-versions`
  ├─ Run Artifact Tests      for each OS job × each Scylla version, in parallel:
  │     build scylla-doctor/oss/artifacts/artifacts-<os>-test
  │       scylla_version=<version>
  │       scylla_doctor_full_tarball_url=<sd_tarball_url>
  │       scylla_doctor_edition=full
  │       run_scylla_doctor_only=true
  │       email_recipients=''            (no per-job emails)
  │         │
  │         └─ artifactsPipeline → SCT_* env vars → artifacts_test.py
  │              verify node health → run_scylla_doctor() on every DB node
  │
  ├─ Generate Report         console table, sd-gating-report.html, build description
  └─ Send Notification       HTML email to email_recipients (skipped when empty)
```

The pipeline has a 180-minute timeout. Each triggered job runs with `wait: true, propagate: false`,
so one failing cell does not stop the others. Results are collected and reported at the end.

### Running the gate

Jenkins job: `scylla-doctor/sd-release-gating`.

The first build of a new or regenerated job only loads its parameters and aborts itself.
Run it again with parameters.

#### Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `sd_tarball_url` | **Yes** | — | URL of the SD full edition tarball to gate. The build fails in `Validate Parameters` without it. See [Tarball URL formats](#tarball-url-formats). |
| `scylla_versions` | No | empty | **Space**-separated ScyllaDB versions, e.g. `2025.1 2026.1`. Empty means auto-discovery. |
| `os_filter` | No | empty | **Comma**-separated OS labels from the [OS matrix](#os-matrix), e.g. `centos9,ubuntu2204`. Empty means all. An unknown label is ignored without a warning. |
| `post_behavior_db_nodes` | No | `destroy` | `keep`, `keep-on-failure` or `destroy`. Passed to every artifact job. |
| `provision_type` | No | `spot` | `on_demand`, `spot` or `spot_fleet`. Passed to every artifact job. |
| `email_recipients` | No | `qa@scylladb.com` | Who gets the gating report. Empty skips the email. |
| `requested_by_user` | No | `scylla-doctor` | Passed through to every artifact job. |

To re-run only the failed cells, set `os_filter` and `scylla_versions` to just those values.

### How parameters reach the test

The gating pipeline passes SD settings to the artifact jobs as regular job parameters, not through
`extra_environment_variables`. `artifactsPipeline.groovy` declares them in its
"Scylla Doctor Configuration" section and exports them as SCT environment variables:

| Gating pipeline | Artifact job parameter | SCT environment variable | SCT option |
|-----------------|------------------------|--------------------------|------------|
| `sd_tarball_url` | `scylla_doctor_full_tarball_url` | `SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL` | `scylla_doctor_full_tarball_url` |
| always `full` | `scylla_doctor_edition` | `SCT_SCYLLA_DOCTOR_EDITION` | `scylla_doctor_edition` |
| always `true` | `run_scylla_doctor_only` | `SCT_RUN_SCYLLA_DOCTOR_ONLY` | `run_scylla_doctor_only` |
| each discovered version | `scylla_version` | `SCT_SCYLLA_VERSION` | `scylla_version` |

`run_scylla_doctor` is not passed. It is already `true` in `defaults/test_default.yaml`.

To debug one cell outside the gate, see [Running SD tests by hand](#running-sd-tests-by-hand).

### ScyllaDB version discovery

When `scylla_versions` is empty, the `Discover Scylla Versions` stage runs:

```sh
./docker/env/hydra.sh get-official-supported-versions --only-print-versions true
```

The command reads `supported_versions.json` from the scylladb-docs-homepage repository
(`SUPPORTED_VERSIONS_URL` in `utils/get_supported_scylla_base_versions.py`). It keeps entries with
status `Supported` and prints their release lines (for example `2026.1 2025.4 2025.1`). The pipeline
uses the **last line** of the command output as the version list.

Each release line is passed to the artifact jobs as `scylla_version` (`SCT_SCYLLA_VERSION`), and SCT
resolves it to the newest release of that line. Nothing in the repository has to change when ScyllaDB adds or
drops a supported release.

The stage fails the build if the command prints nothing, or if no versions are found or given.

### OS matrix

`ARTIFACT_TEST_JOBS` in `vars/sdReleaseGatingPipeline.groovy` lists the OS variants. Each entry has
an `os_label` (used in `os_filter` and in the report) and a `job_path`:

| `os_label` | Backend | `os_label` | Backend |
|------------|---------|------------|---------|
| `centos9` | GCE | `amazon2023` | AWS |
| `rocky9` | GCE | `amazon2023-arm` | AWS |
| `ubuntu2204` | GCE | `centos9-arm` | AWS |
| `ubuntu2404` | GCE | `ubuntu2204-arm` | AWS |
| `debian12` | GCE | `ubuntu2404-arm` | AWS |
| `rhel10` | AWS | `ami` | AWS |
| `docker` | Docker | `ami-arm` | AWS |

`oel9` is commented out because no OEL 9 AMI is available.

A full run is 14 OS variants × the number of supported versions. With three supported versions
that is 42 artifact jobs.

#### Where the triggered jobs live

`job_path` is relative to the gating job's folder. For example,
`oss/artifacts/artifacts-centos9-test` resolves to
`scylla-doctor/oss/artifacts/artifacts-centos9-test`.
These are separate jobs from the regular artifact jobs, so gating runs do not mix with the
regular artifact test history.

`jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` creates jobs in that folder that
point to the existing `jenkins-pipelines/oss/artifacts/*.jenkinsfile` files
(see `process_symlinks()` in `utils/build_system/create_test_release_jobs.py`).
It currently declares only `artifacts-ami`, `artifacts-ami-arm` and `artifacts-docker`.
The other jobs in the folder exist in Jenkins but are not declared in the repository.

### Report

The `Generate Report` stage produces:

- **Console table**: plain-text OS × version matrix with a `FAILURES` list (OS, version, status, job link).
- **`sd-gating-report.html`**: the same matrix as HTML, with each cell linking to its job.
  It is archived on the gating build and used as the email body.
- **Build description**: `SD tarball: <url> | PASSED|FAILED | <passed>/<total> passed`.
- **Build result**: `FAILURE` if any cell is not `SUCCESS`.

The `Send Notification` stage emails the HTML report to `email_recipients` with the subject
`SD Release Gating PASSED|FAILED: <tarball file name> (<UTC date time>)`.
The triggered artifact jobs send no emails of their own.

The report gives the status and a link for each failed cell, but not the failure reason.
For a failed artifact job the error is its build description, or `Job finished with <result>`.
Open the job link and its Argus run to find the failed subtest.

### Extending the gate

#### Add or remove an OS variant

1. Make sure a regular artifact pipeline exists in `jenkins-pipelines/oss/artifacts/`.
2. Add an entry to `jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` so the job is
   created in the `scylla-doctor` folder.
3. Add a `[os_label: ..., job_path: 'oss/artifacts/artifacts-<os>-test', backend: ...]` entry to
   `ARTIFACT_TEST_JOBS`.

To remove an OS, delete or comment out its `ARTIFACT_TEST_JOBS` entry, as was done for `oel9`.

#### Pass a new parameter to the artifact jobs

1. Add the SCT option to `sdcm/sct_config/mixins/scylla_doctor.py` and its default to `defaults/test_default.yaml`.
2. Add the job parameter to `vars/artifactsPipeline.groovy` and export it as `SCT_<OPTION>`
   in the `sctScript` block.
3. Add it to the `build(... parameters: [...])` call in `vars/sdReleaseGatingPipeline.groovy`.

All triggered jobs must accept the parameter. Jenkins ignores parameters a job does not declare,
so a job whose parameters were not reloaded silently runs without it.

#### Allow a known SD failure

- Collector: add a case to `ScyllaDoctor.filter_out_failed_collectors()` with a link to the issue.
- Analyzer: `analyze_vitals_report_known_issues()` only labels a failure; it does not allow it.

#### After the gate passes

The gate does not update anything. To use the approved SD version in regular artifact tests, update
`scylla_doctor_version` in `defaults/test_default.yaml` as described in
[Updating Scylla Doctor Version](./artifacts_test.md#updating-scylla-doctor-version).

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| Build #1 aborted at once | Expected. The first build only loads parameters. Run it again. |
| `Unable to find scylla-doctor package for version <version>` | Basic edition: `scylla_doctor_version` does not exist under `downloads/scylla-doctor/tar/` in `downloads.scylladb.com`. |
| `'sd_tarball_url' is required` | Set the `sd_tarball_url` parameter. |
| `Failed to auto-discover Scylla versions` | The supported-versions source could not be read. Set `scylla_versions` explicitly. |
| A cell has `Error triggering job: ...` and no link | The `scylla-doctor/oss/artifacts/artifacts-<os>-test` job does not exist or failed to start. Check the job in Jenkins and `_symlinks.yaml`. |
| `Unable to find full scylla-doctor package` | Full edition: the KeyStore bucket has no package for the configured `scylla_doctor_version`, which happens even when `sd_tarball_url` is set. Set `scylla_doctor_version` to a version in the bucket. |
| `curl` fails with an HTTP 403/404, or `Downloaded full scylla-doctor (...) file is not a valid gzip tarball` | The key in `sd_tarball_url` does not exist, the runner cannot read the bucket, or the pre-signed URL was signed for the wrong region. Check the URL and the bucket region. |
| `Failed collectors: {...}` | SD regression or a new environment-specific failure. If it is a known issue, add it to `filter_out_failed_collectors()` with a link. |
| `Failed analyzers: {...}` | Analyzer reported a warning or failure. Entries with `known issue` are already tracked but still fail. |
| `Vitals result json file ... has not been created` or `Scylla log archive has not been created` | SD did not finish collecting. Check the SD output in the artifact job's `sct.log`. |
