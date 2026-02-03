# Implementation Plan: Dynamic Billing Project Selection using Jenkins Uno-Choice Plugin

## 1. Problem Statement

Currently, the `billing_project` parameter in Jenkins pipelines is defined as a simple string parameter with a default value from `pipelineParams`. This requires manual entry and doesn't provide a list of valid billing projects to choose from, leading to potential errors and inconsistencies.

The billing project values are stored in a private GitHub repository (`https://github.com/scylladb/finops`) and should be dynamically fetched and presented to users as a selectable list using the Jenkins Uno-Choice plugin (Active Choices).

**Business Need:**
- Reduce manual entry errors for billing project selection
- Ensure only valid billing projects from the finops repository are used
- Provide a better user experience with a dropdown selection
- Centralize billing project management in the finops repository
- Make the solution reusable across all pipeline files

## 2. Current State

### Existing Implementation

The `billing_project` parameter is currently defined in multiple pipeline files:

**Files using billing_project:**
- `vars/longevityPipeline.groovy` (line 23, 143-145)
- `vars/perfRegressionParallelPipeline.groovy` (line 129-131)
- `vars/perfRegressionParallelPipelinebyRegion.groovy` (multiple references)
- `vars/perfSearchBestConfigParallelPipeline.groovy`
- `vars/managerPipeline.groovy` (line 52, parameter definition)
- `vars/jepsenPipeline.groovy`

**Current parameter definition pattern:**
```groovy
string(defaultValue: "${pipelineParams.get('billing_project', '')}",
       description: 'Billing project for the test run',
       name: 'billing_project')
```

**Environment variable usage:**
```groovy
environment {
    SCT_BILLING_PROJECT = "${params.billing_project}"
}
```

**Tag builder usage:**
In `vars/tagBuilder.groovy` (line 14), billing_project is also used as a tag:
```groovy
billing_project:"${GetBillingProjectTag()}"
```

The `GetBillingProjectTag()` function derives billing project from JOB_NAME or GIT_BRANCH, but this is different from the user-selected parameter.

### Limitations of Current Approach
1. No validation of billing project values
2. Manual entry prone to typos
3. No centralized list of valid billing projects
4. Duplication of parameter definition across multiple files
5. No connection to the finops repository

## 3. Goals

1. **Implement a centralized billing project selection function** that can be used across all pipelines
2. **Fetch billing project list dynamically** from the private `scylladb/finops` GitHub repository
3. **Use Jenkins Uno-Choice plugin** to provide an interactive dropdown for users
4. **Single file implementation** - create one reusable Groovy function that all pipelines can call
5. **Maintain backward compatibility** - existing pipelines should continue to work with minimal changes
6. **Handle errors gracefully** - provide fallback if finops repository is unavailable
7. **Support authentication** - properly authenticate to the private GitHub repository

## 4. Implementation Phases

### Phase 1: Create the Shared Billing Project Parameter Function

**Scope:** Create a new shared library function `vars/getBillingProjectChoices.groovy` that fetches billing projects from the finops repository and returns them as Active Choices parameter.

**Definition of Done:**
- [ ] New file `vars/getBillingProjectChoices.groovy` created
- [ ] Function fetches billing project list from `scylladb/finops` repository
- [ ] Function uses GitHub credentials stored in Jenkins
- [ ] Function returns properly formatted Active Choices parameter
- [ ] Error handling with fallback to default list if fetch fails
- [ ] Documentation added to the function

**Dependencies:** None

**Deliverables:**
- `vars/getBillingProjectChoices.groovy` - Main implementation file

**Implementation Details:**
The function will:
1. Use Jenkins GitHub credentials to authenticate
2. Fetch `notebooks/projects/projects.yaml` from the finops repository
3. Parse the YAML file to extract project names from the `projects` array
4. Return an Active Choices Reactive Parameter or Active Choices Parameter
5. Include error handling with fallback to a default list

**Open Questions:**
- What credentials ID should be used for GitHub authentication? (Likely 'qa-github-token' or similar)
- Should this be a static list or reactive parameter based on other selections?

### Phase 2: Create Helper Function for Billing Project Parameter Definition

**Scope:** Create a helper function that returns the complete parameter definition using Active Choices, which can be called from all pipeline files.

**Definition of Done:**
- [ ] Helper function `billingProjectParameter()` created in the same file
- [ ] Function returns proper Active Choices parameter configuration
- [ ] Function accepts optional default value parameter
- [ ] Function includes proper Groovy script for fetching projects
- [ ] Documentation on how to use the function in pipelines

**Dependencies:** Phase 1

**Deliverables:**
- Updated `vars/getBillingProjectChoices.groovy` with parameter helper function

**Implementation Details:**
```groovy
def billingProjectParameter(String defaultValue = '') {
    // Returns Active Choices Parameter definition
    // Uses [$class: 'ChoiceParameter'] or similar Uno-Choice syntax
}
```

### Phase 3: Update One Sample Pipeline to Use the New Function

**Scope:** Update one pipeline file (e.g., `longevityPipeline.groovy`) to use the new billing project parameter function.

**Definition of Done:**
- [ ] Sample pipeline updated to use new function
- [ ] Pipeline successfully builds and displays dropdown
- [ ] Billing projects are fetched from finops repository
- [ ] Selected value is properly passed to SCT_BILLING_PROJECT environment variable
- [ ] Testing with actual Jenkins instance completed

**Dependencies:** Phase 2

**Deliverables:**
- Updated `vars/longevityPipeline.groovy` with new parameter usage

**Implementation Details:**
Replace current string parameter with call to the new function:
```groovy
// Before
string(defaultValue: "${pipelineParams.get('billing_project', '')}",
       description: 'Billing project for the test run',
       name: 'billing_project')

// After
billingProjectParameter(pipelineParams.get('billing_project', ''))
```

### Phase 4: Document Usage and Create Migration Guide

**Scope:** Create documentation for other pipeline maintainers on how to adopt the new billing project parameter.

**Definition of Done:**
- [ ] Documentation added to `docs/` directory
- [ ] Migration guide created for updating existing pipelines
- [ ] Example code provided
- [ ] Common issues and troubleshooting section added

**Dependencies:** Phase 3

**Deliverables:**
- `docs/jenkins-billing-project-parameter.md` - Documentation file

### Phase 5: Update Remaining Pipelines (Optional - Can be done incrementally)

**Scope:** Update all other pipeline files to use the new billing project parameter function.

**Definition of Done:**
- [ ] All pipeline files using billing_project updated
- [ ] All pipelines tested
- [ ] No breaking changes to existing jobs

**Dependencies:** Phase 4

**Deliverables:**
- Updated pipeline files:
  - `vars/perfRegressionParallelPipeline.groovy`
  - `vars/perfRegressionParallelPipelinebyRegion.groovy`
  - `vars/perfSearchBestConfigParallelPipeline.groovy`
  - `vars/managerPipeline.groovy`
  - `vars/jepsenPipeline.groovy`

**Note:** This phase can be done incrementally over time, one pipeline at a time.

## 5. Testing Requirements

### Phase 1 Testing
- **Unit Tests:**
  - Test GitHub API connection with valid credentials
  - Test error handling when repository is unreachable
  - Test parsing of different file formats (if multiple formats supported)
- **Manual Testing:**
  - Verify credentials work in Jenkins environment
  - Test fallback mechanism when fetch fails
  - Verify billing project list is correctly parsed

### Phase 2 Testing
- **Unit Tests:**
  - Test parameter definition generation
  - Test default value handling
- **Manual Testing:**
  - Verify parameter appears correctly in Jenkins UI
  - Test with different default values

### Phase 3 Testing
- **Integration Testing:**
  - Build sample pipeline in Jenkins
  - Verify dropdown shows billing projects
  - Verify selection is passed to environment variable
  - Test with no network access (fallback scenario)
- **Manual Testing:**
  - Run actual test with selected billing project
  - Verify billing_project tag is correctly applied
  - Check logs for proper environment variable values

### Phase 4 Testing
- **Documentation Review:**
  - Verify examples work as documented
  - Peer review of documentation
  - Test migration guide on at least one additional pipeline

### Phase 5 Testing
- **Regression Testing:**
  - Test all updated pipelines
  - Verify no breaking changes
  - Confirm billing projects are correctly applied

## 6. Success Criteria

The implementation will be considered successful when:

1. **Single Implementation File:** All billing project selection logic is in one reusable file (`vars/getBillingProjectChoices.groovy`)

2. **Dynamic Fetching:** Billing projects are successfully fetched from the `scylladb/finops` repository

3. **User Experience:** Jenkins UI displays a dropdown list of billing projects instead of a text input field

4. **Error Resilience:** System gracefully handles network issues or repository unavailability with fallback to default list

5. **Backward Compatibility:** Existing pipeline jobs continue to work without breaking

6. **Reusability:** At least one pipeline successfully uses the new function, with clear path for others to adopt

7. **Documentation:** Clear documentation exists for how to use and maintain the solution

## 7. Risk Mitigation

### Risk: Jenkins Uno-Choice Plugin Not Installed
**Mitigation:**
- Verify plugin is installed before implementation
- Document plugin version requirements
- Provide installation instructions if needed
- Have fallback to standard choice parameter if plugin unavailable

### Risk: finops Repository Structure Changes
**Mitigation:**
- Document expected file format and location
- Implement flexible parsing that can handle format variations
- Use versioning if available in finops repository
- Maintain fallback list of common billing projects

### Risk: Authentication Issues
**Mitigation:**
- Document required Jenkins credentials
- Test authentication in non-production environment first
- Implement clear error messages for auth failures
- Provide fallback to manual entry if auth fails

### Risk: Performance Impact
**Mitigation:**
- Cache billing project list for a reasonable time period
- Implement timeout for GitHub API calls
- Ensure fallback list is immediately available
- Monitor API rate limits

### Risk: Breaking Changes to Existing Pipelines
**Mitigation:**
- Implement incrementally, one pipeline at a time
- Maintain backward compatibility with string parameter interface
- Test thoroughly in non-production environment
- Document rollback procedure
- Keep old parameter definition pattern available during transition

### Risk: Network Connectivity Issues
**Mitigation:**
- Implement retry logic for API calls
- Set appropriate timeouts
- Provide meaningful error messages
- Use fallback list when GitHub is unreachable

## 8. Technical Implementation Notes

### Active Choices Parameter Syntax

The Uno-Choice plugin uses specific parameter types. Example implementation:

```groovy
// Active Choices Parameter (static list, evaluated once at job load)
[$class: 'ChoiceParameter',
 choiceType: 'PT_SINGLE_SELECT',
 description: 'Billing project for the test run',
 name: 'billing_project',
 script: [
     $class: 'GroovyScript',
     script: [
         script: 'return getBillingProjectsFromFinops()',
         sandbox: false
     ]
 ]
]

// Or Active Choices Reactive Parameter (can react to other parameters)
[$class: 'CascadeChoiceParameter',
 choiceType: 'PT_SINGLE_SELECT',
 description: 'Billing project for the test run',
 name: 'billing_project',
 referencedParameters: '',
 script: [
     $class: 'GroovyScript',
     script: [
         script: 'return getBillingProjectsFromFinops()',
         sandbox: false
     ]
 ]
]
```

### Expected finops Repository Structure

The finops repository contains billing projects at `notebooks/projects/projects.yaml` with the following format:

```yaml
projects:
  - name: "project-alpha"
    more_props: whatever
  - name: "project-beta"
    more_props: whatever
  - name: "project-gamma"
    more_props: whatever
```

**Implementation Note:** The parser should extract only the `name` field from each project in the `projects` array, ignoring all other properties.

### GitHub API Access Pattern

```groovy
// Using GitHub credentials from Jenkins
withCredentials([string(credentialsId: 'github-token-id', variable: 'GITHUB_TOKEN')]) {
    def response = sh(
        script: "curl -H 'Authorization: token ${GITHUB_TOKEN}' " +
                "https://api.github.com/repos/scylladb/finops/contents/notebooks/projects/projects.yaml",
        returnStdout: true
    ).trim()
    // Parse response JSON and decode base64 content
    // Then parse YAML to extract project names
    def content = readJSON(text: response).content
    def decodedContent = new String(content.decodeBase64())
    def projects = readYaml(text: decodedContent)
    def billingProjects = projects.projects.collect { it.name }
    return billingProjects
}
```

## 9. Follow-up Items (Post-Implementation)

1. Monitor usage and gather user feedback
2. Consider adding project descriptions in dropdown
3. Evaluate adding validation for selected billing project
4. Consider integrating with cost tracking systems
5. Document process for adding new billing projects to finops repository
6. Set up alerts if billing project fetch fails repeatedly
