# Jenkins Job Definitions

Due to a growing need of having extra metadata available to jenkins consumers
(both users and automation) a job define system was added to SCT jenkins generator.

It replaces previous `_display_name` file (which used to contain just the display
 name of the folder) with a special .yaml file called `_folder_definitions.yaml`.

Example definitions file:

```yaml
# used by argus to determine argus plugin
job-type: scylla-cluster-tests
# used by argus to determine which parameter wizard to use on execution
job-sub-type: longevity
# replacement for _display_name file, sets the jenkins folder display name
folder-name: Cluster - Longevity Tests
folder-description: Contains all longevity tests
# Optional: labels/tags for categorizing and filtering tests
# Can be parsed by Argus and other automation tools
labels:
  - performance
  - regression
  - nightly
# Per job (regex supported) job overrides, defines as a mapping
overrides:
  100gb: # regex, search for anything matching 100gb
      job-sub-type: artifact
      labels:
        - large-dataset
        - artifact
  longevity-5tb-1day-gce: # specific name
    # overrides sub-type for argus, needed for folders that contain
    # for example both "artifact" and "artifact-offline" tests
      job-sub-type: rolling-upgrade
      labels:
        - upgrade
        - multi-cloud
```

A job description can also be provided as an annotation, like so:

```js
/** jobDescription
    This is a simple job description.
    Can be multi line and indented.
    Will return the description without indent.
*/
```

Once template is generated the defines are applied to the job description
along with job description from the pipeline file, like so:

```
This is a simple job description.
Can be multi line and indented.
Will return the description without indent.

### JobDefinitions
job-sub-type: artifact
job-type: scylla-cluster-tests
labels: performance, regression, nightly
```

If a define file was not found, a previously used mechanism is used for descriptions
```
jenkins-pipelines/oss/longevity/longevity-cdc-100gb-4h.jenkinsfile
```

## Labels and Tags

Labels/tags are used to categorize and filter tests for better organization and automation.
They can be defined at multiple levels:

### Folder-level labels

Labels defined in `_folder_definitions.yaml` apply to all jobs in that folder by default:

```yaml
labels:
  - performance
  - regression
```

### Job-level labels (overrides)

Labels can be overridden for specific jobs using the `overrides` section:

```yaml
overrides:
  my-specific-test:
    labels:
      - custom-label
      - special-case
```

### Test method labels (docstrings)

Labels can also be defined in test method docstrings using Python decorators or special annotations:

```python
class MyTest(ClusterTester):
    def test_example(self):
        """
        Test description here.

        Labels: performance, smoke-test, critical
        """
        pass
```

The format for test method labels is flexible and supports:
- **Inline format**: `Labels: label1, label2, label3`
- **List format**:
  ```
  Labels:
    - label1
    - label2
  ```

### Extracting labels

Use the hydra CLI command to extract all labels from the repository:

```bash
# Extract all labels from tests and folder definitions
hydra extract-labels

# Extract labels from specific directory
hydra extract-labels --path jenkins-pipelines/oss/longevity

# Output in JSON format
hydra extract-labels --output json

# Show label statistics
hydra extract-labels --stats
```

The command will scan:
1. All `_folder_definitions.yaml` files for folder-level and override labels
2. All `.jenkinsfile` files for jobDescription annotations
3. All Python test files (`*_test.py`) for test method docstrings with labels

This allows tracking which tests have which labels and detecting when labels are changed for review purposes.
