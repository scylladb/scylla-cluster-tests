#!groovy

/**
 * Fetches billing project list from the scylladb/finops repository.
 * 
 * This function retrieves the list of available billing projects from the
 * notebooks/projects/projects.yaml file in the private finops repository.
 * It extracts only the project names and returns them as a list that can
 * be used in Jenkins pipeline parameters.
 * 
 * @return List of billing project names, or fallback list if fetch fails
 */
def call() {
    def fallbackProjects = ['master', 'staging', 'no_billing_project']
    
    try {
        // Use GitHub credentials stored in Jenkins
        def projectsList = []
        
        withCredentials([string(credentialsId: 'github-bot-token', variable: 'GITHUB_TOKEN')]) {
            // Fetch the YAML file from finops repository using GitHub API
            def apiUrl = 'https://api.github.com/repos/scylladb/finops/contents/notebooks/projects/projects.yaml'
            
            def response = sh(
                script: """
                    curl -s -H 'Authorization: token ${GITHUB_TOKEN}' \
                         -H 'Accept: application/vnd.github.v3+json' \
                         '${apiUrl}'
                """,
                returnStdout: true
            ).trim()
            
            if (!response) {
                echo "Warning: Empty response from GitHub API, using fallback list"
                return fallbackProjects
            }
            
            // Parse the GitHub API response to get the base64 content
            def jsonResponse = readJSON text: response
            
            if (!jsonResponse.content) {
                echo "Warning: No content in GitHub API response, using fallback list"
                return fallbackProjects
            }
            
            // Decode base64 content
            def decodedContent = new String(jsonResponse.content.decodeBase64())
            
            // Parse YAML content
            def projectsData = readYaml text: decodedContent
            
            if (projectsData && projectsData.projects) {
                // Extract only the 'name' field from each project
                projectsList = projectsData.projects.collect { project ->
                    project.name ?: ''
                }.findAll { it } // Remove empty strings
                
                if (projectsList.isEmpty()) {
                    echo "Warning: No projects found in YAML file, using fallback list"
                    return fallbackProjects
                }
                
                echo "Successfully fetched ${projectsList.size()} billing projects from finops repository"
                return projectsList
            } else {
                echo "Warning: Invalid YAML structure in projects file, using fallback list"
                return fallbackProjects
            }
        }
    } catch (Exception e) {
        echo "Error fetching billing projects from finops repository: ${e.message}"
        echo "Using fallback billing projects list"
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
