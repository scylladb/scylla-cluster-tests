# Scylla Doctor

[← All configuration options](../configuration_options.md)

The scylla-doctor diagnostic tool. It is both a subject under test (the artifact tests run it
and assert on its findings) and a diagnostic collected on failure, which is why it is its own
group rather than part of log collection.

**4 options.**


<a id="run_scylla_doctor"></a>

## **run_scylla_doctor** / SCT_RUN_SCYLLA_DOCTOR

Flag to run Scylla Doctor tool

**default:** True

**type:** bool


<a id="scylla_doctor_edition"></a>

## **scylla_doctor_edition** / SCT_SCYLLA_DOCTOR_EDITION

Scylla Doctor edition to use. Allowed values: 'basic', 'full'.<br>'basic' fetches the free/open-source edition via HTTP.<br>'full' fetches the full/enterprise edition from a private S3 bucket.

**default:** basic

**type:** Literal['basic', 'full']


<a id="scylla_doctor_version"></a>

## **scylla_doctor_version** / SCT_SCYLLA_DOCTOR_VERSION

Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')<br>to hardcode the version, or leave empty to use the latest available version. For stability,<br>artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.

**default:** 1.10

**type:** str
* appendable


<a id="use_scylla_doctor_on_failure"></a>

## **use_scylla_doctor_on_failure** / SCT_USE_SCYLLA_DOCTOR_ON_FAILURE

Run scylla-doctor on test failure to collect additional diagnostics

**default:** True

**type:** bool
