# Matrix Trigger Command

## Overview

The `trigger-matrix` command provides a Python-based interface for triggering tier1 test jobs from a YAML matrix definition. This replaces the embedded Groovy logic with testable, reusable Python code.

## Architecture

### Components

1. **YAML Matrix** (`configurations/triggers/tier1.yaml`)
   - Defines all tier1 jobs with their configurations
   - Single source of truth for job definitions
   - Easy to read and maintain

2. **Python Trigger Logic** (`sdcm/utils/trigger_matrix.py`)
   - Loads matrix from YAML
   - Determines job folder based on version
   - Fetches AMIs for master builds
   - Handles ARM64 architecture requirements
   - Filters jobs by version, labels, and skip list
   - Triggers Jenkins jobs via REST API

3. **CLI Command** (`sct.py trigger-matrix`)
   - Provides command-line interface
   - Accepts parameters for version, repo, labels, etc.
   - Supports dry-run mode for testing

4. **Groovy Pipeline** (`vars/triggerMatrixPipeline.groovy`)
   - Thin wrapper that calls Python command
   - Minimal logic in Groovy
   - Easier to maintain

## Usage

### Basic Usage

Trigger for master version:
```bash
./sct.py trigger-matrix --scylla-version master --labels-selector master-weekly
```

Trigger for release branch:
```bash
./sct.py trigger-matrix --scylla-version 2025.4
```

### Advanced Options

Skip specific jobs:
```bash
./sct.py trigger-matrix --scylla-version 2025.4 --skip-jobs "job1,job2"
```

Use custom Scylla repo:
```bash
./sct.py trigger-matrix --scylla-version master --scylla-repo http://custom-repo.com
```

Specify job folder explicitly:
```bash
./sct.py trigger-matrix --scylla-version 2025.4.1 --job-folder branch-2025.4
```

Dry run (see what would be triggered):
```bash
./sct.py trigger-matrix --scylla-version master --dry-run
```

### Using from Groovy Pipeline

The Groovy pipeline calls the Python command:

```groovy
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

triggerMatrixPipeline(
    scylla_version: '2025.4',
    requested_by_user: 'john_doe'
)
```

## Matrix Definition Format

The YAML matrix file (`configurations/triggers/tier1.yaml`) uses this format:

```yaml
jobs:
  - job_name: '{JOB_FOLDER}/tier1/test-name'
    backend: aws
    region: us-east-1
    arch: x86_64  # Optional, can be 'arm64'
    versions:
      - '2024.1'
      - '2025.4'
      - master
    labels:
      - master-weekly
    test_config: test-cases/path/to/config.yaml
```

### Job Fields

- `job_name`: Jenkins job path with `{JOB_FOLDER}` placeholder
- `backend`: Cloud provider (aws, azure, gce)
- `region`: Cloud region (empty for Azure/GCE)
- `arch`: Architecture (x86_64 or arm64) - optional, defaults to x86_64
- `versions`: List of supported Scylla versions
- `labels`: List of labels for filtering (e.g., master-weekly)
- `test_config`: Path to test configuration YAML

## How It Works

1. **Load Matrix**: Reads job definitions from YAML file
2. **Determine Folder**: Auto-detects job folder from version
   - `master` → `scylla-master`
   - `2025.4` → `branch-2025.4`
3. **Fetch AMI**: For master AWS builds, fetches latest AMI via hydra
4. **Handle ARM64**: If job requires ARM64, calls `find-ami-equivalent`
5. **Filter Jobs**: Applies version, label, and skip filters
6. **Build Parameters**: Constructs Jenkins job parameters
7. **Trigger Jobs**: Calls Jenkins REST API to trigger each job

## Testing

### Python Unit Tests

Located in `unit_tests/test_trigger_matrix.py`:

```bash
# Run tests
python3 -m pytest unit_tests/test_trigger_matrix.py -v
```

Tests cover:
- Matrix loading
- Job folder determination
- Job filtering
- Parameter building

### Dry Run

Test without actually triggering jobs:

```bash
./sct.py trigger-matrix --scylla-version master --dry-run
```

This will show exactly what would be triggered without making any changes.
