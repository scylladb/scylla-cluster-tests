# Tier1 Parallel Pipeline

This document explains the new tier1 parallel pipeline that consolidates all tier1 test triggers into a single Groovy pipeline.

## Overview

The `tier1ParallelPipeline.groovy` provides a unified, maintainable way to trigger tier1 tests across multiple backends (AWS, Azure, GCE) and versions. It replaces the multiple XML-based freestyle jobs that were previously used.

## Location

- **Pipeline Definition**: `vars/tier1ParallelPipeline.groovy`
- **Example Jenkinsfile**: `jenkins-pipelines/oss/tier1-parallel-trigger.jenkinsfile`
- **Unit Tests**: `unit_tests/test_tier1_pipeline.py`

## Features

### 1. Single Source of Truth
All tier1 jobs are defined in one place in the `tier1TestMatrix` within the pipeline. This makes it easy to:
- See all tier1 jobs at a glance
- Add or remove jobs
- Modify job configurations
- Understand version support

### 2. Multi-Backend Support
The pipeline supports:
- **AWS**: Uses AMI images, supports multiple regions
- **Azure**: Uses Azure images
- **GCE**: Uses GCE images

### 3. Weekly Scheduling
Configured to run automatically every Saturday at 6:00 AM UTC for master builds:
```groovy
00 6 * * 6 %scylla_version=master:latest;labels_selector=master-weekly;requested_by_user=timtimb0t
```

### 4. Release Branch Support
The pipeline supports multiple Scylla versions:
- 2024.1, 2024.2
- 2025.1, 2025.2, 2025.3, 2025.4
- master

Version matching uses prefix matching, so `2025.4.1` will match `2025.4`.

### 5. Emergency Skip Jobs Mechanism
The `skip_jobs` parameter provides an emergency hatch to temporarily disable specific jobs:
```groovy
skip_jobs: 'longevity-harry-2h-test,jepsen-all-test'
```

### 6. Automatic Image Fetching
For master builds, the pipeline automatically fetches the latest AMI/image using hydra:
```bash
./docker/env/hydra.sh list-images -c aws -r us-east-1 -o text
```

### 7. ARM64 Architecture Support
The pipeline supports ARM64 architecture for specific jobs. When a job is configured with `arch: 'arm64'`, the pipeline:
1. Fetches the base x86_64 AMI using hydra
2. Uses `find-ami-equivalent` to find the corresponding ARM64 AMI:
```bash
./sct.py find-ami-equivalent --ami-id <x86_ami> --source-region us-east-1 --target-region <target_region> --target-arch arm64 --output-format text
```

Example: The **longevity-twcs-48h-test** runs with ARM64 architecture.

## Tier1 Jobs Included

The pipeline includes the following 11 tier1 tests:

### AWS Tests (8 jobs)
1. **longevity-50gb-3days-test** - us-east-1 (x86_64)
2. **longevity-150gb-asymmetric-cluster-12h-test** - us-east-1 (x86_64)
3. **longevity-twcs-48h-test** - us-east-1 (arm64) ⭐
4. **longevity-multidc-schema-topology-changes-12h-test** - us-east-1 (x86_64)
5. **longevity-mv-si-4days-streaming-test** - eu-west-1 (x86_64)
6. **longevity-schema-topology-changes-12h-test** - eu-west-1 (x86_64)
7. **gemini-1tb-10h-test** - eu-west-1 (x86_64)
8. **longevity-harry-2h-test** - eu-west-1 (x86_64)

### Azure Tests (1 job)
9. **longevity-1tb-5days-azure-test**

### GCE Tests (2 jobs)
10. **longevity-large-partition-200k-pks-4days-gce-test**
11. **jepsen-all-test**

## Parameters

### Required Parameters
- **scylla_version**: Scylla version to test (e.g., `master:latest`, `2025.4`, `2025.4.1`)

### Optional Parameters
- **scylla_repo**: Scylla repo URL (optional, for custom repos)
- **use_job_throttling**: Whether to throttle concurrent builds (default: true)
- **labels_selector**: Filter jobs by label (e.g., `master-weekly`)
- **skip_jobs**: Comma-separated list of jobs to skip (emergency hatch)
- **requested_by_user**: User requesting the build (for tracking)

## Usage

### Basic Usage (Master Branch)

Create a Jenkinsfile:
```groovy
#!groovy

def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

tier1ParallelPipeline()
```

The pipeline will use the scheduled trigger parameters or allow manual triggering with custom parameters.

### Manual Trigger for Release Branch

Trigger the pipeline with parameters:
```groovy
tier1ParallelPipeline(
    scylla_version: '2025.4',
    requested_by_user: 'john_doe'
)
```

### Emergency Skip Jobs

Temporarily skip problematic jobs:
```groovy
tier1ParallelPipeline(
    scylla_version: '2025.4',
    skip_jobs: 'longevity-harry-2h-test,jepsen-all-test',
    requested_by_user: 'john_doe'
)
```

### Custom Scylla Repo

Use a custom Scylla repository:
```groovy
tier1ParallelPipeline(
    scylla_version: 'master:latest',
    scylla_repo: 'http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list',
    requested_by_user: 'john_doe'
)
```

## How It Works

1. **Version Detection**: The pipeline checks if `scylla_version` is `master:latest` and converts it to `master`
2. **Image Fetching**: For master builds on AWS, fetches the latest x86_64 AMI using hydra
3. **ARM Architecture Handling**: If a job specifies `arch: 'arm64'`, the pipeline uses `find-ami-equivalent` to find the corresponding ARM64 AMI
4. **Job Filtering**:
   - Checks if the job version matches the requested version
   - Checks if the job label matches the labels_selector (if provided)
   - Checks if the job is in the skip_jobs list
5. **Parameter Building**: Builds backend-specific parameters for each job
6. **Job Triggering**: Triggers all matching jobs in parallel with `wait: false`

## Job Matrix Structure

Each job in `tier1TestMatrix` has the following structure:

```groovy
[
    job_name: 'scylla-master/tier1/longevity-50gb-3days-test',
    backend: 'aws',
    region: 'us-east-1',
    arch: 'x86_64',  // Optional, defaults to 'x86_64'
    versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
    labels: ['master-weekly'],
    test_config: 'test-cases/longevity/longevity-50GB-3days-authorization-and-tls-ssl.yaml'
]
```

### Adding a New Job

To add a new tier1 job, add an entry to the `tier1TestMatrix`:

```groovy
[
    job_name: 'scylla-master/tier1/my-new-test',
    backend: 'aws',
    region: 'us-west-2',
    versions: ['2025.4', 'master'],
    labels: ['master-weekly'],
    test_config: 'test-cases/longevity/my-new-test.yaml'
]
```

### Adding an ARM64 Job

To add a job that runs on ARM64 architecture, specify the `arch` field:

```groovy
[
    job_name: 'scylla-master/tier1/my-arm-test',
    backend: 'aws',
    region: 'us-east-1',
    arch: 'arm64',  // This triggers ARM64 AMI lookup
    versions: ['2025.4', 'master'],
    labels: ['master-weekly'],
    test_config: 'test-cases/longevity/my-arm-test.yaml'
]
```

The pipeline will automatically find the ARM64 equivalent AMI using `find-ami-equivalent`.

### Removing a Job Temporarily

Use the `skip_jobs` parameter instead of removing from the matrix to preserve history.

### Changing Version Support

Modify the `versions` list for the job in the matrix:

```groovy
versions: ['2025.3', '2025.4', 'master']  // Only support recent versions
```

## Testing

Unit tests are located in `unit_tests/test_tier1_pipeline.py` and validate:
- Pipeline file structure
- Required parameters
- Job matrix completeness
- Backend support
- Version range support
- Skip jobs logic
- Trigger configuration

Run tests:
```bash
python3 -m pytest unit_tests/test_tier1_pipeline.py -v
```

## Migration from XML Triggers

### Old XML Triggers (Deprecated)
These XML-based triggers are replaced by the new pipeline:
- `jenkins-pipelines/master-triggers/sct_triggers/tier1-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-aarch64-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-azure-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-gce-custom-time-trigger.xml`

### New Approach
- Single Groovy pipeline: `vars/tier1ParallelPipeline.groovy`
- Example Jenkinsfile: `jenkins-pipelines/oss/tier1-parallel-trigger.jenkinsfile`

### Benefits
1. **Maintainability**: All jobs in one place, easier to update
2. **Consistency**: Same pattern across all backends
3. **Flexibility**: Easy to add/remove/modify jobs
4. **Visibility**: Clear view of all tier1 jobs
5. **Emergency Control**: Skip jobs without code changes
6. **Version Control**: Better tracking of changes
7. **Testing**: Unit tests ensure correctness

## Troubleshooting

### Job Not Triggering

Check:
1. Version matches one of the supported versions
2. Labels match if `labels_selector` is provided
3. Job not in `skip_jobs` list
4. Backend and region are correctly configured

### AMI Not Found

For master builds, ensure:
1. Hydra is accessible
2. AWS region has available AMIs
3. Network connectivity is working

### Job Failures

1. Check job logs for specific errors
2. Use `skip_jobs` to temporarily disable failing jobs
3. Fix the underlying issue
4. Remove from `skip_jobs` when ready

## Future Enhancements

Potential improvements:
1. Add more labels for different schedules (daily, monthly)
2. Support for ARM64 architecture jobs
3. Integration with Argus for result tracking
4. Automatic retry logic for transient failures
5. Notification integration (Slack, email)

## Contact

For questions or issues, contact the SCT team or refer to the main repository documentation.
