# Scylla Doctor testing and release gating

SCT tests Scylla Doctor (SD) in two editions:

- **basic**: the publicly downloadable edition, with collectors only. Regular
  [artifact tests](./artifacts_test.md) run it on every ScyllaDB build they test.
- **full**: the edition from a private S3 bucket, with collectors and analyzers. The
  [SD release gate](#run-the-sd-release-gate) runs it to approve a new SD build.

**Contents**

- [Run the SD release gate](#run-the-sd-release-gate): when and how to gate a new SD release.
- [How SCT tests SD](#how-sct-tests-sd): editions, options, the test steps, downloads, running by
  hand.
- [Release gating trigger](#release-gating-trigger): how the gate works, its parameters, versions,
  operating systems, report, extending it, known gaps.
- [Troubleshooting](#troubleshooting)

## Run the SD release gate

The SD team runs the gate on every SD release candidate, **before** `releng/update-scylla-doctor`
publishes it. Nothing runs it automatically, and QA does not run it.

1. Open the Jenkins job
   [`scylla-master/sct_triggers/scylla-doctor-gating-trigger`](https://jenkins.scylladb.com/job/scylla-master/job/sct_triggers/job/scylla-doctor-gating-trigger/).
   Do not use `scylla-doctor-gating-trigger-test` next to it: it is an unmaintained leftover.
2. Click **Build with Parameters**, set `scylla_doctor_full_tarball_url` to the candidate tarball,
   for example
   `https://fe-artifacts-297607762119-eu-central-1/scylla_doctor_staging/tar/scylla-doctor-1.11.4.tar.gz`,
   and leave everything else empty.
3. The gate starts one artifact test job per operating system and ScyllaDB version in the Jenkins
   folder [`scylla-doctor/oss/artifacts`](https://jenkins.scylladb.com/job/scylla-doctor/job/oss/job/artifacts/),
   for example `artifacts-rhel10-test`, and waits for all of them.
4. When all jobs finish, the results arrive **by email** from `qa@scylladb.com` to
   `cx-customer-ops-eng@scylladb.com`, with the subject
   `[Trigger Matrix] PASSED|FAILED — scylla-doctor-gating: <tarball file name>, <versions>`.
   The email has a grid of operating system × ScyllaDB version linking each job's build, and a
   Failures section with an Argus link for each failed job. This is the only email: the artifact
   jobs send none of their own. To send it to other addresses, set `email_recipients`
   (comma-separated); it replaces the default address. The same report is on the trigger
   build: its description (`PASSED|FAILED | <passed>/<total> passed | ...`) and the archived
   `trigger-matrix-report.html`.
5. The candidate is **approved** when one gating build with every version and operating system is
   green: the build is `SUCCESS` and the report says `PASSED`.
6. After the release, update `scylla_doctor_version` in `defaults/test_default.yaml`, so regular
   artifact tests use it: see
   [Updating Scylla Doctor Version](./artifacts_test.md#updating-scylla-doctor-version).

With empty parameters the gate runs the full edition, SD checks only, on Ubuntu 24.04, RHEL 10 and
the ARM AMI, for the latest release of every supported ScyllaDB version: 12 artifact jobs with four
supported versions.

Good to know:

- **First build of a new or regenerated job** fails with
  `Required parameters have no value: scylla_doctor_full_tarball_url (jobs: ...)`: it only loads
  the job's parameters. Run it again with parameters.
- **A failed job**: open its Argus link in the report to find the failed collector or analyzer.
  To investigate, re-run only some jobs with `labels_selector` (for example `os-rhel10`) and
  `scylla_version` (for example `2026.1`). A partial run does not approve the candidate.
- **The gate has not had a green build yet.** Analyzers flag the test clusters: see
  [Known gaps](#known-gaps).

## How SCT tests SD

Both editions go through the same `ScyllaDoctor` class (`utils/scylla_doctor.py`) and the same
`run_scylla_doctor()` subtest (`artifacts_test.py`). They differ in where the package comes from and
in whether the analysis phase runs.

### Editions compared

| | Basic | Full |
|---|-------|------|
| Selected by | `scylla_doctor_edition: basic` (default) | `scylla_doctor_edition: full` |
| Contents | Collectors | Collectors and analyzers |
| Package source | Public bucket `downloads.scylladb.com`, prefix `downloads/scylla-doctor/tar/` | Private bucket; name and prefix from the KeyStore (`scylla_doctor_full.json`) |
| Download | Plain HTTPS from `https://downloads.scylladb.com/` | S3 pre-signed URL, valid for 300 seconds |
| Version | `scylla_doctor_version` (newest package when empty) | The exact tarball in `scylla_doctor_full_tarball_url`, otherwise `scylla_doctor_version` |
| Analysis phase | No | Yes |
| Extra SD config | Disabled collectors on nonroot installs | Same, plus [analyzer settings for test clusters](#analyzer-settings) |
| Used by | Regular artifact tests | SD release gate |

`scylla_doctor_version` is pinned in `defaults/test_default.yaml` so that a new SD release cannot
break artifact tests unannounced. See
[Scylla Doctor Version Management](./artifacts_test.md#scylla-doctor-version-management).

### Options

The SD options are defined in `sdcm/sct_config/mixins/scylla_doctor.py` and described in
[configuration_options.md](./configuration_options.md):

| Option | Default | Meaning |
|--------|---------|---------|
| `run_scylla_doctor` | `true` | Run the SD subtest in the artifact test |
| `scylla_doctor_edition` | `basic` | `basic` or `full` |
| `scylla_doctor_version` | pinned in `defaults/test_default.yaml` | SD version to look up in the bucket |
| `scylla_doctor_full_tarball_url` | empty | Full edition only: exact tarball to download |
| `run_scylla_doctor_only` | `false` | Skip every artifact check except node health and SD |
| `use_scylla_doctor_on_failure` | `true` | Not part of artifact testing; see [SD on test failure](#sd-on-test-failure) |

### Analyzer settings

With the full edition, SCT writes `ScyllaDoctor.SCYLLA_DOCTOR_ANALYZER_CONFIG`
(`utils/scylla_doctor.py`) into `scylla_doctor.conf`, so the analyzers suit a single-node test
cluster. The gate therefore does not cover these analyzers, or covers them with test thresholds:

| Analyzer | Setting |
|----------|---------|
| `ScyllaServicesAnalyzer` | turned off |
| `CPUScalingAnalyzer` | turned off |
| `RsyslogAnalyzer` | turned off |
| `ScyllaInternodeCompressionAnalyzer` | turned off |
| `ScyllaSnitchAnalyzer` | turned off |
| `ScyllaUpdateAnalyzer` | turned off |
| `ScyllaClusterSystemKeyspacesReplicationAnalyzer` | turned off |
| `RAMAnalyzer` | lower recommended RAM, total and per logical core |
| `SwapAnalyzer` | RAM to swap ratio `3.5` |
| `StorageRAMRatioAnalyzer` | disk to RAM ratio up to `120` |
| `ScyllaConfigurationConsistencyAnalyzer` | skips some keys in the source and persisted validation |

See the constant for the exact values and the reason for each.

### Where SD is tested

| Path | Edition | What it validates | Triggered by |
|------|---------|-------------------|--------------|
| Regular artifact tests (`jenkins-pipelines/oss/artifacts/`, release branch folders) | basic | A ScyllaDB build, with the pinned SD version | ScyllaDB package/image builds |
| [SD release gate](#run-the-sd-release-gate) (`scylla-master/sct_triggers/scylla-doctor-gating-trigger`) | full | A new SD build, on all supported ScyllaDB versions and one OS per category | The SD team, manually, before each SD release |

Analyzers are not run on new ScyllaDB builds: regular artifact tests run the basic edition, so an
analyzer failure does not block a ScyllaDB release. Analyzers first run against a new ScyllaDB
version at the next SD release, when the gate tests all supported versions.

New ScyllaDB images:

- **AMI**: checked with the basic edition. Every successful `scylla-master/ami` build, and
  `scylla-master/next-machine-image` builds, trigger `scylla-master/artifacts/artifacts-ami-test`,
  which runs the SD subtest.
- **Docker image**: not checked. Every `scylla-master/docker` build triggers
  `scylla-master/artifacts/artifacts-docker-test`, but SD is skipped on Docker, and its SD subtest
  passes without running SD ([SCT-1277](https://scylladb.atlassian.net/browse/SCT-1277)).

The release gate is not connected to image builds.

### SD test flow

`run_scylla_doctor()` runs these steps on every DB node, for both editions.

1. **Download and install.** `install_scylla_doctor()` calls `download_scylla_doctor()`, which picks
   the basic or full download path (see below), extracts the tarball and finds the Python
   interpreter bundled with ScyllaDB. It then writes `scylla_doctor.conf` with the disabled
   collectors (nonroot installs) and, for the full edition, the analyzer settings.
2. **Record the SD version** in Argus as the `scylla-doctor` package.
3. **Collect vitals** with `--save-vitals`. `lspci` and `ethtool` are installed first if missing,
   and `iptables` is installed and its service started. The vitals JSON file must be created.
   Except on Docker, the `scylla_logs_*.tar.gz` archive must be created too.
4. **Check collectors.** A collector with status `1` (failed) fails the test, unless
   `filter_out_failed_collectors()` lists it as a known exception.
5. **Full edition only: run analyzers** with `--load-vitals --output json` and save the report.
6. **Full edition only: check analyzers.** Any status other than `0` (passed) or `1` (skipped)
   fails the test. `analyze_vitals_report_known_issues()` adds a Jira link to known analyzer
   failures, but they still fail the test.

Steps 5 and 6 run whenever the edition is `full`. There is no fallback to the basic edition: if the
full package cannot be downloaded, `download_full_scylla_doctor()` raises and the test fails.

SD runs its analyzers on every call, so with the full edition the collect step (step 3) runs them
too. SCT only uses the vitals file from that call, and checks the analyzers from step 5. The basic
edition has no analyzers.

`run_scylla_doctor()` returns without running SD, and the subtest passes, on the **Docker backend**.
[SCT-1277](https://scylladb.atlassian.net/browse/SCT-1277) removes the skip once SD skips the
collectors that cannot run in a container ([DOCTOR-139](https://scylladb.atlassian.net/browse/DOCTOR-139)).

SD supports client SSL. The `client_encrypt` skip in `run_scylla_doctor()`
([field-engineering#2280](https://github.com/scylladb/field-engineering/issues/2280)) has been inactive
since that issue was closed. No job of the gate sets `client_encrypt`, so the gate does not test SD
over client SSL yet: [SCT-1276](https://scylladb.atlassian.net/browse/SCT-1276) adds such a job and
removes the skip.

#### Where the subtest runs in the artifact test

- **Regular artifact test** (`run_scylla_doctor_only: false`): `test_scylla_service()` runs
  `check scylla_doctor results`, if `run_scylla_doctor` is true, in the middle of the artifact
  checks: after the install, node health, cqlsh and node_exporter checks, and before the time-sync,
  stop/start, restart and perftune checks.
- **SD-only mode** (`run_scylla_doctor_only: true`): `test_scylla_service()` runs only
  `verify node health` (`node.check_node_health()`) and `check scylla_doctor results`, then returns.
  The release gate always uses this mode.

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
2. Picks the package:
   - **`scylla_doctor_full_tarball_url` is set** (the release gate): the bucket and key come from
     the URL. A bare key uses the KeyStore bucket. The bucket is not searched, and
     `scylla_doctor_version` is not used.
   - **No URL:** finds the package in the KeyStore bucket, using `scylla_doctor_version`, or the
     newest package when the version is empty. If it finds nothing, the test fails with
     `Unable to find full scylla-doctor package`.
3. Creates a pre-signed URL valid for 300 seconds, signed for the bucket's region, and the DB node
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
export SCT_SCYLLA_VERSION=2026.1
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

Full edition with a specific tarball, SD checks only, as the release gate runs it:

```sh
export SCT_SCYLLA_VERSION=2026.1
export SCT_SCYLLA_DOCTOR_EDITION=full
export SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL=https://fe-artifacts-297607762119-eu-central-1/scylla_doctor_staging/tar/scylla-doctor-1.11.4.tar.gz
export SCT_RUN_SCYLLA_DOCTOR_ONLY=true
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

From a laptop, also add `--config configurations/network_config/test_communication_public.yaml`
(see [How to run artifacts tests using hydra](./artifacts_test.md#how-to-run-artifacts-tests-using-hydra)).

In Jenkins, set the same values with the `scylla_version`, `scylla_doctor_edition`,
`scylla_doctor_full_tarball_url` and `run_scylla_doctor_only` parameters of any artifact job.

### SD on test failure

`use_scylla_doctor_on_failure` is a separate diagnostic feature, not an SD test. When any SCT test
fails, `tester.py` installs SD on the DB nodes and saves `scylla_doctor_<node>_vitals.json`,
`scylla_doctor_<node>_logs.tar.gz` and, with the full edition, `scylla_doctor_<node>_analysis.json`
to the test logs. SD errors there are logged and do not affect the test result.

## Release gating trigger

How the gate works. To run it, see [Run the SD release gate](#run-the-sd-release-gate).

The gate is the `scylla-doctor-gating` trigger matrix
([#16551](https://github.com/scylladb/scylla-cluster-tests/pull/16551)). It runs the full edition in
[SD-only mode](#where-the-subtest-runs-in-the-artifact-test) on one artifact job per OS category,
for every supported ScyllaDB version, and sends a single pass/fail report. It runs on the shared
[trigger matrix](./testing/trigger-matrix.md) code, the same code that starts tier1, sanity and the
other scheduled triggers.

It replaces the `sd-release-gating` pipeline (`vars/sdReleaseGatingPipeline.groovy`), which is no
longer used. The gate is not part of the SD publish workflow either: `releng/update-scylla-doctor`
only publishes the packages and runs no tests.

An SD build is approved only when **every** triggered artifact job is `SUCCESS`. `FAILURE`,
`UNSTABLE`, `ABORTED`, `TIMEOUT`, `NOT_STARTED` and a job that could not be triggered all fail the
gate.

`scylla-doctor-gating-trigger-test` exists because `utils/build_system/create_test_release_jobs.py`
used to add `-test` to every job name it created. Trigger jobs no longer get the suffix.

### Components

| File | Role |
|------|------|
| `configurations/triggers/scylla-doctor-gating.yaml` | The gate itself: jobs, defaults, required parameters, supported versions only, report recipients |
| `jenkins-pipelines/oss/sct_triggers/scylla-doctor-gating-trigger.jenkinsfile` | Jenkins entry point, generated from the matrix by `utils/build_system/generate_trigger_jenkinsfiles.py`; calls `triggerMatrixPipeline()` |
| `vars/triggerMatrixPipeline.groovy` | Shared trigger pipeline: job parameters, then `hydra trigger-matrix`; sets the build description and archives the report |
| `sdcm/utils/trigger_matrix/` | `sct.py trigger-matrix`: version selection, job filtering, triggering, waiting, report and email |
| `jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` | Declares the artifact jobs of the `scylla-doctor` Jenkins folder |
| `vars/artifactsPipeline.groovy` | Artifact test pipeline; turns the SD job parameters into `SCT_*` environment variables |
| `artifacts_test.py` | `test_scylla_service()`: SD-only mode branch and `run_scylla_doctor()` |
| `utils/scylla_doctor.py` | `ScyllaDoctor`: download, install, collect vitals, analyze, verify |
| `utils/get_supported_scylla_base_versions.py` | `fetch_official_supported_versions()`: the supported ScyllaDB versions |

### Flow

```
scylla-doctor-gating-trigger (Jenkins: scylla-master/sct_triggers/scylla-doctor-gating-trigger)
  │
  └─ triggerMatrixPipeline → hydra trigger-matrix --matrix configurations/triggers/scylla-doctor-gating.yaml
        │
        ├─ pick the ScyllaDB versions: given ones, or every supported one (supported_versions_only)
        ├─ fail unless scylla_doctor_full_tarball_url is set              (required_params)
        ├─ for each version, trigger each enabled job of the matrix, without waiting in between:
        │     scylla-doctor/oss/artifacts/artifacts-<os>-test
        │       scylla_version=<version>
        │       scylla_doctor_full_tarball_url=<tarball>
        │       scylla_doctor_edition=full
        │       run_scylla_doctor_only=true
        │       email_recipients=''                   (no per-job emails)
        │         │
        │         └─ artifactsPipeline → SCT_* env vars → artifacts_test.py
        │              verify node health → run_scylla_doctor() on every DB node
        │
        ├─ fail if no job was triggered
        ├─ wait for every triggered build, polling queued and running builds together
        └─ email the report, save it, set the build description;
           the build fails if any artifact job did not succeed
```

The trigger pipeline has a 6-hour timeout.

### Parameters

The trigger has the parameters of every trigger matrix job; these are the ones the gate uses.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `scylla_doctor_full_tarball_url` | **Yes** | — | URL of the SD full edition tarball to gate. The run fails before triggering anything without it. See [Tarball URL formats](#tarball-url-formats). |
| `scylla_version` | No | empty | Empty: the latest release of every supported ScyllaDB version. Otherwise supported versions only, separated by commas or spaces, e.g. `2026.1,2025.1`. A version line such as `2026.1` runs its latest release. See [ScyllaDB versions](#scylladb-versions). |
| `labels_selector` | No | empty | Pick operating systems by their `os-<os>` label. Alternatives are separated by `/`: `os-rhel10/os-ami-arm`. Empty means all enabled jobs. A selector that matches no job fails the run. See the [OS matrix](#os-matrix). |
| `skip_jobs` | No | empty | Comma-separated jobs to leave out, as the matrix's `job_name` writes them. |
| `post_behavior_db_nodes` | No | empty: the matrix gives `destroy` | `keep`, `keep-on-failure` or `destroy`, passed to every artifact job. |
| `provision_type` | No | empty: the matrix gives `on_demand` | `on_demand`, `spot`, `spot_fleet` or `auto`, passed to every artifact job. |
| `email_recipients` | No | empty: the matrix gives `cx-customer-ops-eng@scylladb.com` | Who gets the report, comma-separated. |
| `requested_by_user` | No | empty: the matrix gives `scylla-doctor` | Owner the artifact jobs and their cloud resources are tagged with. |
| `dry_run` | No | false | Print what would be triggered, trigger nothing. It still emails a `PASSED` report with build #0 for every job, so set `email_recipients` to yourself. |

### How parameters reach the test

The trigger passes SD settings to the artifact jobs as regular job parameters, not through
`extra_environment_variables`. The matrix's `defaults` are given to every job, and the trigger's own
parameters override them. `artifactsPipeline.groovy` declares the SD parameters in its
"Scylla Doctor Configuration" section and exports them as SCT environment variables:

| Comes from | Artifact job parameter | SCT environment variable | SCT option |
|------------|------------------------|--------------------------|------------|
| trigger parameter `scylla_doctor_full_tarball_url` | `scylla_doctor_full_tarball_url` | `SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL` | `scylla_doctor_full_tarball_url` |
| matrix default, always `full` | `scylla_doctor_edition` | `SCT_SCYLLA_DOCTOR_EDITION` | `scylla_doctor_edition` |
| matrix default, always `true` | `run_scylla_doctor_only` | `SCT_RUN_SCYLLA_DOCTOR_ONLY` | `run_scylla_doctor_only` |
| each [ScyllaDB version](#scylladb-versions) | `scylla_version` | `SCT_SCYLLA_VERSION` | `scylla_version` |

`run_scylla_doctor` is not passed. It is already `true` in `defaults/test_default.yaml`.

### ScyllaDB versions

The matrix sets `supported_versions_only: true`, so the gate tests officially supported ScyllaDB
versions only, as the old pipeline did.

- **`scylla_version` empty** (the usual case): `fetch_official_supported_versions()` reads
  `supported_versions.json` from the scylladb-docs-homepage repository (`SUPPORTED_VERSIONS_URL` in
  `utils/get_supported_scylla_base_versions.py`) and keeps the entries with status `Supported`, for
  example `2026.3 2026.2 2026.1 2025.1`. Each one becomes its latest released patch, read from the
  production AMIs in `eu-west-1`, for example `2026.3.4`. The console shows
  `No --scylla-version given: running every supported version`, then the versions.
- **`scylla_version` given**: every version must belong to a supported version line, or the run
  fails before triggering anything. A bare version line (`2026.1`) also runs its latest release:
  left to itself, `2026.1` would run the newest nightly build of the branch.

Nothing in the repository has to change when ScyllaDB adds or drops a supported version.

The matrix is filtered and triggered once per version, and the builds of all versions are waited
for together and reported in one email.

### OS matrix

The matrix runs one operating system per category, as Vincent Reniers (Scylla Doctor) requested:

> We don't need every OS. For example one per category would be fine.
> - Ubuntu 24
> - A RHEL distribution (rhel10)
> - Perhaps an ARM AMI could be useful to test as well since we don't have this in our testing

| Label | Artifact job | Backend | `wait_timeout` | Runs |
|-------|--------------|---------|----------------|------|
| `os-ubuntu2404` | `artifacts-ubuntu2404-test` | GCE | 3600 s | yes |
| `os-rhel10` | `artifacts-rhel10-test` | AWS `eu-west-1` | 7200 s | yes |
| `os-ami-arm` | `artifacts-ami-arm-test` | AWS `eu-west-1`, aarch64 | 7200 s | yes |
| `os-docker` | `artifacts-docker-test` | AWS | | no, `disabled: true` |

Docker is disabled until SD skips by default the collectors that cannot run in a container
([DOCTOR-139](https://scylladb.atlassian.net/browse/DOCTOR-139)); `run_scylla_doctor()` skips SD on
the Docker backend anyway. The old pipeline also ran CentOS 9, Rocky 9, Debian 12, Ubuntu 22.04,
Amazon Linux 2023, the x86_64 AMI and the ARM variants of Amazon Linux 2023, CentOS 9, Ubuntu 22.04
and 24.04; they are no longer gated.

A full run is three artifact jobs per supported version: with four supported versions, 12 jobs.

#### Where the triggered jobs live

The triggered jobs are in the Jenkins folder
[`scylla-doctor/oss/artifacts`](https://jenkins.scylladb.com/job/scylla-doctor/job/oss/job/artifacts/),
separate from the regular artifact jobs, so gating runs do not mix with the regular artifact test
history. Each one runs the regular artifact jenkinsfile,
`jenkins-pipelines/oss/artifacts/artifacts-<os>.jenkinsfile`.

The matrix names them by absolute path, for example `/scylla-doctor/oss/artifacts/artifacts-rhel10-test`,
so they are the same whichever branch folder the trigger runs from.

`jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` declares them, pointing to the
existing jenkinsfiles (see `process_symlinks()` in `utils/build_system/create_test_release_jobs.py`):
`artifacts-ami`, `artifacts-ami-arm`, `artifacts-docker`, `artifacts-ubuntu2404` and
`artifacts-rhel10`. `sct.py create-test-release-jobs <folder>` creates the jobs of
`jenkins-pipelines/scylla-doctor/` under `<folder>`; the gate's jobs are the ones created with the
folder `scylla-doctor`.

### Waiting for the jobs

Every matrix job is a `wait: true` job: the trigger waits for its build and reports its result.
Waiting starts once the jobs of all versions are triggered. Queued and running builds are polled
together, so a build waiting for an agent does not hold back the others, and a failed build is
logged as soon as it finishes. Each job's `wait_timeout` applies twice: a build still in the queue
after it is reported as `NOT_STARTED`, and a build still running after it as `TIMEOUT`.

### Report

At the end of the run the trigger produces:

- **Email** to `email_recipients`, from `qa@scylladb.com`, with the subject
  `[Trigger Matrix] PASSED|FAILED — scylla-doctor-gating: <tarball file name>, <versions>`. The body
  lists the run's parameters, a link to the trigger build, and a grid with a row per artifact job
  and a column per ScyllaDB version; each cell links its build. A Failures section lists every
  failed build with its version, result and build link, plus a link to its Argus run when the
  build has one (not for `NOT_STARTED`, `TIMEOUT` or a cancelled build). When all builds pass, it
  says so instead.
- **`trigger-matrix-report.html`**: the same report, archived on the trigger build.
- **Build description**: `PASSED|FAILED | <passed>/<total> passed | <tarball file name>, <versions>`.
- **Build result**: `FAILURE` if any artifact job is not `SUCCESS`, or could not be triggered.

The triggered artifact jobs send no emails of their own.

A job that could not be triggered at all is not in the report. Only the console lists it, under
`Failed to trigger wait-mode job` and `Failed:`. The report can then say `PASSED` while the build is
`FAILURE`: check the build result, not only the email.

The report gives the result and links for each failed job, but not the failure reason. Open the
job's Argus run to find the failed subtest.

The matrix asks for each build's `scylla_doctor_*_analysis.json` (`collect_results`), but no build
archives such a file: the artifact test leaves `<host>.analysis.json` on the DB node, and the
`scylla_doctor_<node>_analysis.json` saved by [SD on test failure](#sd-on-test-failure) goes to the
test logs, not to the Jenkins artifacts. The analysis is in the job's `sct.log` and its Argus run.

### Extending the gate

#### Add or remove an OS

1. Make sure a regular artifact pipeline exists in `jenkins-pipelines/oss/artifacts/`.
2. Add an entry to `jenkins-pipelines/scylla-doctor/oss/artifacts/_symlinks.yaml` so the job is
   created in the `scylla-doctor` folder.
3. Add a job to `configurations/triggers/scylla-doctor-gating.yaml`, like the existing ones: the
   absolute `job_name`, `backend` (and `arch: "aarch64"` for ARM), `wait`, `wait_timeout`,
   `fail_on_error`, `collect_results`, the `gating` and `os-<os>` labels, and `region` under `params`
   for AWS jobs (GCE jobs take no `region`).

To stop running an OS without removing it, set `disabled: true` on its job, as for Docker.

#### Pass a new parameter to the artifact jobs

1. Add the SCT option to `sdcm/sct_config/mixins/scylla_doctor.py` and its default to `defaults/test_default.yaml`.
2. Add the job parameter to `vars/artifactsPipeline.groovy` and export it as `SCT_<OPTION>`
   in the `sctScript` block.
3. Add it to the `defaults` of `configurations/triggers/scylla-doctor-gating.yaml`, to give it to
   every job. A value that changes per run needs a trigger parameter, like
   `scylla_doctor_full_tarball_url`.

All triggered jobs must accept the parameter. Jenkins ignores parameters a job does not declare,
so a job whose parameters were not reloaded silently runs without it.

#### Allow a known SD failure

- Collector: add a case to `ScyllaDoctor.filter_out_failed_collectors()` with a link to the issue.
- Analyzer: `analyze_vitals_report_known_issues()` only labels a failure; it does not allow it.

### Known gaps

What the gate does not check, or cannot pass, today:

- **No green build yet.** Analyzers warn about the test clusters, and a warning fails the test:
  - `NodeInstanceTypeAnalyzer` warns about any instance type that is not on SD's hardcoded list of
    recommended types. The Ubuntu 24.04 job resolves to `n2d-standard-2` (GCE) and the ARM AMI job
    uses `im4gn.xlarge`; neither is on the list. The RHEL 10 job resolves to `i4i.large`, which is.
  - Other analyzers that have failed gating runs: `DriverVersionAnalyzer`
    ([SCT-420](https://scylladb.atlassian.net/browse/SCT-420)), `ScyllaSSTablesAnalyzer`,
    `AuthenticationAnalyzer`, `ScyllaKeyspacesReplicationAnalyzer` and `NICsAnalyzer`.
- **Skipped results pass.** Collectors with status `2` (skipped) and analyzers with status `1`
  (skipped) do not fail the test. A change that makes a collector skip, and the analyzers that
  depend on it skip too, is not caught. There is no list of what may be skipped on which
  environment. The only per-environment exceptions are the failed collectors allowed by
  `filter_out_failed_collectors()` and the collectors turned off on nonroot installs.
- **One DB node.** Every artifact config runs one DB node (`n_db_nodes: 1`), so the cluster-scope
  collectors (gossip, topology, cluster status and schema) only see a one-node cluster.
- **No nonroot install.** The gate runs only regular installs. The nonroot offline install jobs
  (`jenkins-pipelines/oss/artifacts-offline-install/*-nonroot.jenkinsfile`) are not in the gate, so
  the nonroot SD config (`[DefaultPaths]` and the collectors turned off) is not checked with a new
  SD release.

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| Build #1 fails with `Required parameters have no value: scylla_doctor_full_tarball_url (jobs: ...)` | Expected for a new or regenerated job: the first build only loads parameters. Run it again with parameters. Otherwise, set `scylla_doctor_full_tarball_url`. |
| `No wait-mode job was triggered, so there is no result to report` | `labels_selector`, `skip_jobs` or the version filters left no job to run. Check the labels in the [OS matrix](#os-matrix). |
| `This matrix runs supported Scylla versions only (...); not supported: <version>` | `scylla_version` names a version outside the supported lines. Give supported versions, or leave it empty. |
| `Supported version <line> has no released AMI in eu-west-1` | A supported line has no production AMI yet. Give exact releases in `scylla_version`, such as `2026.1.15`. |
| `No officially supported Scylla versions were found`, or an HTTP error while reading `supported_versions.json` | The supported-versions list has no supported entry, or could not be read. The gate cannot run without it; retry later. |
| A job is `FAILURE` with build number 0 | Its queued build was cancelled in Jenkins. |
| The email says `PASSED` but the build failed | A job could not be triggered, so it is not in the report. See `Failed to trigger wait-mode job` in the console, and check the job in Jenkins and `_symlinks.yaml`. |
| A job is `NOT_STARTED` | Its build waited in the Jenkins queue for longer than the job's `wait_timeout`, usually for an agent. Re-run that job with `labels_selector` and `scylla_version`. |
| A job is `TIMEOUT` | Its build ran longer than the job's `wait_timeout`. |
| A parameter you set had no effect | The trigger job's parameter list is out of date: Jenkins reloads it on each run, and drops parameters it does not know yet. Run it again. |
| `Unable to find scylla-doctor package for version <version>` | Basic edition: `scylla_doctor_version` does not exist under `downloads/scylla-doctor/tar/` in `downloads.scylladb.com`. |
| `Unable to find full scylla-doctor package` | Full edition without `scylla_doctor_full_tarball_url`: the KeyStore bucket has no package for the configured `scylla_doctor_version`. Set `scylla_doctor_version` to a version in the bucket, or set the tarball URL. Runs with a URL, such as the release gate, do not search the bucket. |
| `curl` fails with an HTTP 403/404, or `Downloaded full scylla-doctor (...) file is not a valid gzip tarball` | The key in `scylla_doctor_full_tarball_url` does not exist, the runner cannot read the bucket, or the pre-signed URL was signed for the wrong region. Check the URL and the bucket region. |
| `Failed collectors: {...}` | SD regression or a new environment-specific failure. If it is a known issue, add it to `filter_out_failed_collectors()` with a link. |
| `Failed analyzers: {...}` | Analyzer reported a warning or failure. Entries with `known issue` are already tracked but still fail. See [Known gaps](#known-gaps). |
| `Vitals result json file ... has not been created` or `Scylla log archive has not been created` | SD did not finish collecting. Check the SD output in the artifact job's `sct.log`. |
