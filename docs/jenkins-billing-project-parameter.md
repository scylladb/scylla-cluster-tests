# Jenkins Billing Project Parameter - Dynamic Selection

## Overview

The billing project parameter in Jenkins pipelines now dynamically fetches available projects from the `scylladb/finops` repository. This provides better user experience with a dropdown selection and ensures only valid billing projects are used.

## Features

- **Dynamic Dropdown**: Billing projects are presented as a dropdown menu instead of manual text entry
- **Automatic Updates**: Project list is fetched from `notebooks/projects/projects.yaml` in the finops repository
- **Error Resilience**: Falls back to default project list if the finops repository is unreachable
- **Single Implementation**: All logic is in one reusable file (`vars/getBillingProjectChoices.groovy`)

## How It Works

### For Pipeline Users

When configuring a Jenkins job that uses billing projects:

1. Navigate to the job configuration page
2. Find the "billing_project" parameter
3. Select from the dropdown of available projects
4. The list is automatically populated from the finops repository

**Fallback Projects**: If the finops repository cannot be reached, the following default projects are available:
- `` (empty - default option)
- `no_billing_project`

### For Pipeline Developers

#### Migrating Existing Pipelines

To migrate a pipeline from manual string input to dynamic billing project selection:

**Before:**
```groovy
string(defaultValue: "${pipelineParams.get('billing_project', '')}",
       description: 'Billing project for the test run',
       name: 'billing_project')
```

**After:**
```groovy
choice(choices: getBillingProjectChoices(),
       description: 'Billing project for the test run (dynamically fetched from finops repository)',
       name: 'billing_project')
```

#### Adding to New Pipelines

Simply add the choice parameter in your pipeline's `parameters` block:

```groovy
pipeline {
    agent { ... }

    environment {
        SCT_BILLING_PROJECT = "${params.billing_project}"
    }

    parameters {
        // ... other parameters ...

        choice(choices: getBillingProjectChoices(),
               description: 'Billing project for the test run (dynamically fetched from finops repository)',
               name: 'billing_project')
    }

    // ... rest of pipeline ...
}
```

## Technical Details

### Function Reference

#### `getBillingProjectChoices()`

Fetches and returns a list of billing project names from the finops repository.

**Returns**: `List<String>` - List of billing project names

**Example**:
```groovy
def projects = getBillingProjectChoices()
// Returns: ['', 'project-alpha', 'project-beta', 'project-gamma', ...]
// Or on failure: ['', 'no_billing_project']
```

#### `billingProjectParameter(String defaultValue)`

Returns a parameter configuration map with billing projects. If a default value is provided, it will be placed first in the choices list.

**Parameters**:
- `defaultValue` (optional): Default billing project to pre-select

**Returns**: `Map` with keys: `name`, `choices`, `description`

**Example**:
```groovy
// Without default
def param = billingProjectParameter()

// With default value
def param = billingProjectParameter('2026.1')
```

### Data Source

**Repository**: `scylladb/finops` (private)
**File**: `notebooks/projects/projects.yaml`
**Format**:
```yaml
projects:
  - name: "project-name"
    # ... other properties ignored ...
```

**Authentication**: Uses Jenkins credential `github-api-access-token`

**Implementation Note**: The function uses Jenkins' `httpRequest` plugin along with standard pipeline steps (`readJSON`, `readYaml`, `withCredentials`). This approach uses Jenkins' built-in building blocks for a cleaner and more maintainable implementation.

### Error Handling

The implementation includes comprehensive error handling with detailed debug logging:

1. **Empty API Response**: Falls back to default project list
2. **Missing Content**: Falls back if GitHub API returns no content
3. **Invalid YAML**: Falls back if YAML structure is unexpected
4. **Network Failures**: Catches all exceptions and uses fallback list
5. **Authentication Errors**: Logged with details and fallback list is used
6. **HTTP Errors**: Non-200 status codes handled by httpRequest plugin

All errors and debug information are logged to the Jenkins console using `println` statements for troubleshooting.

## Migration Checklist

For each pipeline that uses `billing_project`:

- [ ] Locate the `billing_project` parameter definition
- [ ] Replace `string` with `choice` parameter
- [ ] Use `getBillingProjectChoices()` for the choices list
- [ ] Update the description to mention dynamic fetching
- [ ] Test the pipeline configuration loads correctly
- [ ] Verify the dropdown appears in the Jenkins UI
- [ ] Confirm the selected value is passed correctly

## Pipelines Using billing_project

All pipelines using the `billing_project` parameter have been migrated to use dynamic selection:

- [x] `vars/longevityPipeline.groovy` - **MIGRATED**
- [x] `vars/perfRegressionParallelPipeline.groovy` - **MIGRATED**
- [x] `vars/perfRegressionParallelPipelinebyRegion.groovy` - **MIGRATED**
- [x] `vars/perfSearchBestConfigParallelPipeline.groovy` - **MIGRATED**
- [x] `vars/managerPipeline.groovy` - **MIGRATED**
- [x] `vars/jepsenPipeline.groovy` - **MIGRATED**

## Troubleshooting

### Problem: Dropdown shows only fallback projects

**Cause**: Unable to fetch projects from finops repository

**Solutions**:
1. Check Jenkins logs for error messages
2. Verify `github-api-access-token` credential exists and is valid
3. Confirm network connectivity to GitHub API
4. Check if finops repository structure has changed

### Problem: Selected project not passed to test

**Cause**: Environment variable not set correctly

**Solutions**:
1. Verify `SCT_BILLING_PROJECT = "${params.billing_project}"` is in the `environment` block
2. Check that parameter name is exactly `billing_project`
3. Confirm the parameter is properly defined in the `parameters` block

### Problem: Pipeline fails to load

**Cause**: Syntax error or missing function

**Solutions**:
1. Ensure `vars/getBillingProjectChoices.groovy` exists in the repository
2. Check for syntax errors in the parameter definition
3. Verify Jenkins has loaded the latest shared library code

## Maintenance

### Updating the Project List

The project list is maintained in the finops repository. To add or remove projects:

1. Edit `notebooks/projects/projects.yaml` in the finops repository
2. Add/remove/modify projects in the `projects` array
3. Ensure each project has a `name` field
4. Commit and push changes
5. Projects will be available in Jenkins on next pipeline configuration load

### Updating Fallback Projects

To modify the fallback project list:

1. Edit `vars/getBillingProjectChoices.groovy`
2. Locate the `fallbackProjects` variable in the `call()` function
3. Update the list as needed
4. Commit and push changes

## Future Enhancements

Potential improvements for future iterations:

- Add caching to reduce API calls
- Support for project descriptions in dropdown
- Integration with Active Choices Reactive parameters for conditional display
- Validation of selected billing project before resource provisioning
- Custom text field option with authorization workflow

## Support

For issues or questions:
- Check Jenkins console logs for error messages
- Review this documentation
- Contact the SCT team in #scylla-test-automation Slack channel
