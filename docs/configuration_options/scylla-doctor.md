# Scylla Doctor

[← All configuration options](configuration_options.md)

The scylla-doctor diagnostic tool. It is both a subject under test (the artifact tests run it
and assert on its findings) and a diagnostic collected on failure, which is why it is its own
group rather than part of log collection.

**6 options.** Jump to: [run_scylla_doctor](#run_scylla_doctor) · [run_scylla_doctor_only](#run_scylla_doctor_only) · [scylla_doctor_edition](#scylla_doctor_edition) · [scylla_doctor_full_tarball_url](#scylla_doctor_full_tarball_url) · [scylla_doctor_version](#scylla_doctor_version) · [use_scylla_doctor_on_failure](#use_scylla_doctor_on_failure)


## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


## **run_scylla_doctor_only** / SCT_RUN_SCYLLA_DOCTOR_ONLY

When true, the artifact test runs only the Scylla Doctor validation<br>(install, collect vitals, analyze, verify) and skips all other artifact checks<br>such as stop/start, cassandra-stress, etc. Useful for fast SD<br>release gating. Implies run_scylla_doctor=true.

**default:** False

**type:** bool


## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


## **scylla_doctor_full_tarball_url** / SCT_SCYLLA_DOCTOR_FULL_TARBALL_URL

Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the<br>standard version-based S3 lookup and downloads SD directly from this URL.<br>Use for testing unofficial or pre-release SD versions.<br>Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'

**default:** N/A

**type:** str (appendable)


## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.13

**type:** str (appendable)


## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool
