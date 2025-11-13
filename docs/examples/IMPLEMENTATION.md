# Labels/Tags Feature Implementation

## Overview

This document describes the implementation of labels/tags support in SCT test definitions. Labels enable better test categorization, filtering, and tracking for Argus and other automation tools.

## Architecture

### Components

1. **LabelsExtractor** (`sdcm/utils/labels_extractor.py`)
   - Core utility class for extracting labels from various sources
   - Supports folder definitions, jenkinsfiles, and Python test files
   - Provides scanning and statistics capabilities

2. **CLI Command** (`sct.py`)
   - `extract-labels` command for scanning repository
   - Multiple output formats: text, JSON, YAML
   - Statistics mode for usage analysis

3. **Documentation** (`docs/job_defines_format.md`)
   - Extended format specification
   - Usage examples and best practices
   - Integration guidelines

4. **Examples** (`docs/examples/`)
   - Working examples for all label formats
   - README with conventions and guidelines

## Label Sources

### 1. Folder Definitions (`_folder_definitions.yaml`)

Labels can be defined at folder level or per-job via overrides:

```yaml
# Folder-level labels (apply to all jobs in folder)
labels:
  - performance
  - regression

# Per-job overrides
overrides:
  specific-job-name:
    labels:
      - custom-label
  pattern-match:  # Regex supported
    labels:
      - another-label
```

**Processing**: Handled by `JenkinsPipelines.load_defines()` in `utils/build_system/create_test_release_jobs.py`. Labels are included in the job description YAML dump, making them visible in Jenkins and parseable by Argus.

### 2. Jenkinsfile Annotations

Labels in jobDescription annotations:

```groovy
/** jobDescription
    Job description text.
    Labels: label1, label2, label3
*/
```

**Processing**: The `get_job_description()` method extracts the jobDescription block, and `LabelsExtractor.extract_jenkinsfile_labels()` parses the Labels line.

### 3. Test Method Docstrings

Labels in Python test method docstrings support two formats:

Inline format:
```python
def test_example(self):
    """
    Test description.
    Labels: performance, critical
    """
```

List format:
```python
def test_example(self):
    """
    Test description.
    Labels:
        - performance
        - critical
    """
```

**Processing**: `LabelsExtractor.extract_test_method_labels()` uses AST parsing to extract docstrings, then regex to find and parse labels.

## CLI Usage

### Basic Commands

```bash
# Extract all labels from repository
hydra extract-labels

# Scan specific directory
hydra extract-labels --path jenkins-pipelines/oss/longevity

# Output formats
hydra extract-labels --output text    # Human-readable (default)
hydra extract-labels --output json    # JSON format
hydra extract-labels --output yaml    # YAML format

# Show statistics
hydra extract-labels --stats
```

### Use Cases

1. **Track Label Changes**
   - Run before/after changes to identify new or modified labels
   - Useful in CI/CD pipelines for review requirements

2. **Label Audit**
   - See all labels used across the repository
   - Identify inconsistent naming or duplicates

3. **Integration Testing**
   - Verify labels are correctly parsed by automation tools
   - Generate label inventories for documentation

## Integration with Existing Systems

### Jenkins Job Creation

The existing `JenkinsPipelines.create_pipeline_job()` already handles labels:

```python
if defines:
    description = f"{text_description}\n\n### JobDefinitions\n{yaml.safe_dump(defines)}"
```

Labels in the defines dict (from `_folder_definitions.yaml`) are automatically included in the job description, making them visible in Jenkins UI and accessible to plugins.

### Argus Integration

Argus can parse labels from:
1. Job descriptions (from folder definitions and overrides)
2. Pipeline metadata
3. Test execution results

The labels appear in the JobDefinitions section of each job description in YAML format, making them easy to parse.

## Label Naming Conventions

Recommended conventions:

- **Format**: lowercase with hyphens (e.g., `long-running`, `smoke-test`)
- **Categories**:
  - Test types: `performance`, `functional`, `regression`, `smoke-test`
  - Duration: `quick`, `long-running`, `multi-day`
  - Priority: `critical`, `high-priority`, `low-priority`
  - Features: `materialized-views`, `cdc`, `alternator`
  - Infrastructure: `multi-cloud`, `large-dataset`, `disk-intensive`

## Testing

### Unit Tests (`unit_tests/test_labels_extractor.py`)

Coverage includes:
- Folder label extraction (basic and with overrides)
- Jenkinsfile label extraction
- Test method label extraction (both inline and list formats)
- Repository scanning
- Statistics generation
- Edge cases (missing files, empty labels, etc.)

### Manual Testing

Example files in `docs/examples/` provide:
- Working demonstrations of all features
- Validation of parsing logic
- Documentation by example

## Future Enhancements

Possible improvements:
1. Label validation (ensure consistent naming)
2. Label inheritance (child folders inherit parent labels)
3. Predefined label sets (e.g., standard test categories)
4. Label-based test filtering in test runner
5. Integration with test result dashboards

## Files Changed

### New Files
- `sdcm/utils/labels_extractor.py` - Label extraction utility
- `unit_tests/test_labels_extractor.py` - Unit tests
- `docs/examples/_folder_definitions.yaml` - Example folder definitions
- `docs/examples/example-with-labels.jenkinsfile` - Example jenkinsfile
- `docs/examples/example_labeled_test.py` - Example test file
- `docs/examples/README.md` - Examples documentation
- `docs/examples/IMPLEMENTATION.md` - This file

### Modified Files
- `docs/job_defines_format.md` - Extended with labels documentation
- `sct.py` - Added `extract-labels` CLI command

### Total Changes
- ~700 lines of new code
- ~300 lines of documentation
- ~400 lines of tests
- All code linted and reviewed
