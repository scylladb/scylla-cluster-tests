# Label/Tag Examples

This directory contains example files demonstrating how to use labels/tags in SCT test definitions.

## Files

- **_folder_definitions.yaml** - Example folder definition with labels at folder level and per-job overrides
- **example-with-labels.jenkinsfile** - Example Jenkins pipeline file with labels in jobDescription annotation
- **example_labeled_test.py** - Example Python test file showing different ways to add labels to test methods

## Usage

These examples demonstrate the three ways to add labels/tags to tests:

1. **Folder-level labels** (in `_folder_definitions.yaml`):
   - Apply to all jobs in the folder by default
   - Can be overridden per job

2. **Jenkinsfile labels** (in `/** jobDescription */` annotation):
   - Specific to that pipeline/job
   - Shown in Jenkins job description

3. **Test method labels** (in test docstrings):
   - Can use inline format: `Labels: label1, label2, label3`
   - Or list format with bullet points
   - Applies to specific test method

## Extracting Labels

Use the hydra CLI command to extract all labels from the repository:

```bash
# Extract all labels
hydra extract-labels

# Extract from specific directory
hydra extract-labels --path jenkins-pipelines/oss

# Output in JSON format
hydra extract-labels --output json

# Show statistics
hydra extract-labels --stats
```

## Label Naming Conventions

Recommended label naming conventions:
- Use lowercase with hyphens: `long-running`, `smoke-test`
- Be specific but not too granular
- Common labels:
  - Test types: `performance`, `functional`, `regression`, `smoke-test`
  - Duration: `quick`, `long-running`, `multi-day`
  - Priority: `critical`, `high-priority`, `low-priority`
  - Features: `materialized-views`, `cdc`, `alternator`
  - Infrastructure: `multi-cloud`, `large-dataset`, `disk-intensive`

## Integration with Argus

Labels are automatically parsed by Argus and can be used for:
- Filtering test results
- Creating test dashboards
- Organizing test reports
- Tracking test coverage by feature/category
