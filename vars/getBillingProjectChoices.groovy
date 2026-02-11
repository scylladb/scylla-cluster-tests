#!groovy

/**
 * Fetches billing project list from the scylladb/finops repository.
 *
 * This function retrieves the list of available billing projects from the
 * notebooks/projects/projects.yaml file in the private finops repository.
 * It extracts only the project names and returns them as a list that can
 * be used in Jenkins pipeline parameters.
 *
 * Uses Jenkins httpRequest plugin and standard pipeline steps.
 *
 * @return List of billing project names, or fallback list if fetch fails
 */
def call() {
    def fallbackProjects = ['', 'no_billing_project']

    try {
        // Fetch the YAML file from finops repository using GitHub API
        def apiUrl = 'https://api.github.com/repos/scylladb/finops/contents/billing_projects/projects.yaml'

        def response
        withCredentials([string(credentialsId: 'github-api-access-token', variable: 'GITHUB_TOKEN')]) {
            response = httpRequest(
                url: apiUrl,
                customHeaders: [
                    [name: 'Authorization', value: 'token ' + GITHUB_TOKEN, maskValue: true],
                    [name: 'Accept', value: 'application/vnd.github.v3+json'],
                    [name: 'User-Agent', value: 'Jenkins-SCT']
                ],
                timeout: 30,
                validResponseCodes: '200',
                consoleLogResponseBody: false,
                quiet: true
            )
        }

        if (!response.content) {
            echo "WARNING: Empty response from GitHub API, using fallback list"
            return fallbackProjects
        }

        // Parse the GitHub API response to get the base64 content
        def jsonResponse = readJSON text: response.content

        if (!jsonResponse.content) {
            echo "WARNING: No content field in GitHub API response, using fallback list"
            return fallbackProjects
        }

        // Decode base64 content
        def decodedContent = new String(jsonResponse.content.decodeBase64())

        // Parse YAML content
        def projectsData = readYaml text: decodedContent

        if (projectsData && projectsData.projects) {
            // Extract only the 'name' field from each project
            def projectsList = projectsData.projects.collect { project ->
                project.name ?: ''
            }.findAll { it } // Remove empty strings

            if (projectsList.isEmpty()) {
                echo "WARNING: No valid project names found in YAML file, using fallback list"
                return fallbackProjects
            }

            // Add empty option as the first choice (default)
            projectsList = [''] + projectsList

            echo "Successfully fetched ${projectsList.size() - 1} billing projects from finops repository"
            return projectsList
        } else {
            echo "WARNING: Invalid YAML structure in projects file (missing 'projects' key), using fallback list"
            return fallbackProjects
        }
    } catch (Exception e) {
        echo "ERROR: Exception while fetching billing projects: ${e.class.name}: ${e.message}"
        e.printStackTrace()
        echo "ERROR: Using fallback billing projects list"
        return fallbackProjects
    }
}

/**
 * Returns a choice parameter definition for billing_project that dynamically
 * fetches the list of available projects from the finops repository.
 *
 * This function creates a Jenkins choice parameter with billing projects
 * fetched from the scylladb/finops repository. If a default value is provided
 * and exists in the list, it will be placed first in the choices.
 *
 * @param defaultValue Optional default billing project value
 * @return Map representing a choice parameter configuration
 */
def billingProjectParameter(String defaultValue = '') {
    def projects = call()

    // If a default value is provided and exists in the list, move it to the front
    if (defaultValue && projects.contains(defaultValue)) {
        projects = projects.findAll { it != defaultValue }
        projects = [defaultValue] + projects
    } else if (defaultValue && !projects.contains(defaultValue)) {
        // If default value doesn't exist in the list, add it to the front
        projects = [defaultValue] + projects
    }

    return [
        name: 'billing_project',
        choices: projects,
        description: 'Billing project for the test run (fetched from finops repository)'
    ]
}
