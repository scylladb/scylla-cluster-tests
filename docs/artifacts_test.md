# What are artifacts tests?

Artifacts tests are kind of smoke tests which verify that packaging artifacts like .rpm/.deb files, cloud or container images produced by ScyllaDB builds can be deployed successfully.

Private repo test is also kind of artifacts test.

# Jenkins pipelines

Artifacts tests triggered by another projects which build .rpm's and .deb's. E.g., tests for CentOS triggered by `centos-rpm` project.

There are pipeline files for Jenkins in the repository:

```
vars
`-- artifactsPipeline.groovy
jenkins-pipelines
|-- artifacts-ami.jenkinsfile
|-- artifacts-centos7.jenkinsfile
|-- artifacts-debian10.jenkinsfile
|-- artifacts-debian11.jenkinsfile
|-- artifacts-docker.jenkinsfile
|-- artifacts-oel76.jenkinsfile
|-- artifacts-ubuntu2004.jenkinsfile
`-- private-repo.jenkinsfile
```

`vars/artifactsPipeline.groovy` is a pipeline call definition which used to run same pipeline with different parameters.

You should use different parameters for .rpm/.deb tests and for AMI test:
- The only required parameter for .rpm/.deb jobs is `scylla_repo`, a path to a ScyllaDB repo (e.g., `https://s3.amazonaws.com/downloads.scylladb.com/rpm/unstable/centos/master/latest/scylla.repo`)
- For AMI job you need two parameters: `scylla_ami_id` and `region_name`
- For Docker job you need two parameters: `scylla_docker_image` (e.g., `scylladb/scylla` or `scylladb/scylla-nightly`) and `scylla_version` (it'll be used as Docker image tag)

To verify Scylla Manager package you need to provide `scylla_repo` and `scylla_mgmt_address` parameters. In this case Scylla Manager will be installed during artifacts test as well as ScyllaDB.

Alternatively, and even recommended, one can use the `manager_version` parameter to just choose the desired version.

Optionally, you can override default cloud instance type by providing `instance_type` parameter. This parameter can be a space-separated list of instance types.

`artifacts-*.jenkinsfile` files can be used to create Jenkins projects.

`private-repo.jenkinsfile` can be used to create a Jenkins project which verifies private repo correctness. You need to provide `scylla_repo` parameter which points to a private repo.

# How to run artifacts tests using `hydra`

If you want to run it locally you should use `ip_ssh_connections: 'public'`:

```sh
export SCT_IP_SSH_CONNECTIONS=public
```

and run one of the following commands:

## CentOS 7
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos7.yaml
```

## CentOS 8
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos8.yaml
```

## Debian 10 (buster)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian10.yaml
```

## Debian 11 (bullseye)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian11.yaml
```

## RHEL 7
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/rhel7.yaml
```

## RHEL 8
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/rhel8.yaml
```

## Oracle Enterprise Linux 7.6
```sh
hydra run-test artifacts_test --backend aws --config test-cases/artifacts/oel76.yaml
```


## Ubuntu 22.04 LTS (jammy)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2204.yaml
```

## Ubuntu 24.04 LTS (noble)
```sh
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2404.yaml
```

## AMI

You can use `hydra` to find Scylla AMI in the desired region (`us-east-1` in example below):
```sh
hydra list-ami-versions -r us-east-1
```

and run the test:

```sh
SCT_AMI_ID_DB_SCYLLA="<ami you've found>" SCT_REGION_NAME=us-east-1 hydra run-test artifacts_test --backend aws --config test-cases/artifacts/ami.yaml
```

## Docker

If you want to run against some official ScyllaDB Docker image, you should go to [tag list](https://hub.docker.com/r/scylladb/scylla/tags) and choose some tag name (i.e., `3.3.rc1`)

```sh
SCT_SCYLLA_VERSION="<tag you've chose>" hydra run-test artifacts_test --backend docker --config test-cases/artifacts/docker.yaml
```

## Unified Package (Relocatable)

You can test using a unified (relocatable) package tarball. This performs an offline installation
without requiring a package repository. Scylla Manager is not included in the unified package.

Using a direct URL:
```sh
export SCT_UNIFIED_PACKAGE="https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/scylla-unified-6.3.0~dev-0.20260101.abcdef123456.x86_64.tar.gz"
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos9.yaml
```

Or automatically resolve the latest unified package:
```sh
export SCT_SCYLLA_VERSION=relocatable:latest
hydra run-test artifacts_test --backend aws --config test-cases/artifacts/centos9.yaml
```

For non-root offline installation:
```sh
export SCT_UNIFIED_PACKAGE="<url to unified package>"
export SCT_NONROOT_OFFLINE_INSTALL=true
hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos9.yaml
```

# Scylla Doctor Version Management

## Overview

Artifact tests use Scylla Doctor to validate the health of deployed Scylla instances. To ensure stability and avoid P0 issues from new Scylla Doctor releases, the version is hardcoded in the configuration rather than always using the latest version.

The configured version is used for both:
- Offline installations (downloading from S3)
- Package manager installations (apt/yum/dnf)

## Current Configuration

The Scylla Doctor version is configured in `defaults/test_default.yaml`:

```yaml
scylla_doctor_version: "1.9"
```

This version is used for all artifact tests by default.

## Updating Scylla Doctor Version

### When to Update

- When a new Scylla Doctor version has been released with important fixes or features
- After thorough validation that the new version works correctly with all test scenarios
- When blocking issues in the current version require an upgrade

### Update Process

1. **Change the version in configuration:**

   Edit `defaults/test_default.yaml` and update the version:
   ```yaml
   scylla_doctor_version: "1.10"  # Update to new version
   ```

2. **Run validation tests:**

   You must run comprehensive artifact tests to verify the new version works correctly:

   ```sh
   # Set Scylla version for testing
   export SCT_SCYLLA_VERSION=master:latest

   # Test on different operating systems
   hydra run-test artifacts_test --backend aws --config test-cases/artifacts/ami.yaml
   hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos9.yaml
   hydra run-test artifacts_test --backend gce --config test-cases/artifacts/ubuntu2204.yaml
   hydra run-test artifacts_test --backend gce --config test-cases/artifacts/debian12.yaml
   hydra run-test artifacts_test --backend docker --config test-cases/artifacts/docker.yaml

   # Test offline installations
   hydra run-test artifacts_test --backend gce --config test-cases/artifacts/rocky9.yaml
   ```

3. **Verify the tests pass:**

   All artifact tests must complete successfully with the new Scylla Doctor version. Pay special attention to:
   - Scylla Doctor installation succeeds
   - All collectors run without errors
   - Vitals JSON file is generated correctly
   - No new false-positive alerts are reported

4. **Create a PR with the version update:**

   Include in the PR description:
   - The new Scylla Doctor version
   - Test results showing successful validation
   - Any known issues or workarounds for the new version
   - Link to Scylla Doctor release notes if available

### Backporting Version Updates

If a version update needs to be backported to a stable branch:

1. **Tag the original PR for backporting:**

   Add the appropriate backport label(s) to the original PR (e.g., `backport/branch-5.4`, `backport/branch-2024.2`).
   The backport PR will be created automatically by the CI system.

2. **Manual testing is required:**

   Once the backport PR is created automatically, you must manually run validation tests on the stable branch to ensure the version works correctly with the Scylla versions tested on that branch.

   ```sh
   # Set appropriate Scylla version for the target branch
   export SCT_SCYLLA_VERSION=<branch-version>:latest

   # Run the same validation tests
   hydra run-test artifacts_test --backend aws --config test-cases/artifacts/ami.yaml
   hydra run-test artifacts_test --backend gce --config test-cases/artifacts/centos9.yaml
   # ... (other test commands)
   ```

3. **Verify and merge the backport PR:**

   After successful manual testing, review and merge the automatically created backport PR.

## Testing Checklist

Before merging any Scylla Doctor version update, ensure:

- [ ] Version updated in `defaults/test_default.yaml`
- [ ] Artifact tests pass on at least 3 different operating systems (CentOS/Rocky, Ubuntu, Debian)
- [ ] Docker artifact test passes
- [ ] AMI artifact test passes on AWS
- [ ] GCE image artifact test passes
- [ ] Offline installation tests pass
- [ ] No new false-positive alerts in test results
- [ ] PR includes test results and validation summary

## Troubleshooting

### Package Not Found

If you get an error like "Unable to find scylla-doctor package for version X.Y":

1. Verify the version exists in S3 bucket: `downloads.scylladb.com/downloads/scylla-doctor/tar/`
2. Check the exact version string format (e.g., "1.9" vs "1.9.0")
3. Ensure you have AWS credentials configured for S3 access

### Version Mismatch

If the wrong version is being used:

1. Check that `scylla_doctor_version` is set in your test configuration
2. Verify the configuration is not overridden by environment variable `SCT_SCYLLA_DOCTOR_VERSION`
3. Check the test logs for the actual version being downloaded

### Collector Failures

If new collectors fail in the updated version:

1. Review the Scylla Doctor release notes for breaking changes
2. **Report JIRA issues** for any collectors that are not working properly
   - Create issues in the scylla-doctor project describing the failure
   - Include test logs and error messages
   - Tag with appropriate priority and affected version
3. **Do not disable collectors** - wait for the JIRA issues to be resolved before updating the scylla-doctor version
4. Only proceed with the version update after all collector issues are fixed

## Override for Testing

To test a specific Scylla Doctor version without changing defaults:

```sh
export SCT_SCYLLA_DOCTOR_VERSION="1.10"
hydra run-test artifacts_test --backend docker --config test-cases/artifacts/docker.yaml
```

To use the latest available version (not recommended for production tests):

```sh
export SCT_SCYLLA_DOCTOR_VERSION=""
hydra run-test artifacts_test --backend docker --config test-cases/artifacts/docker.yaml
```
