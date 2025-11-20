// Generic Matrix-Based Parallel Pipeline (Simplified)
// This pipeline now delegates to Python logic for better testability and reusability
// Default matrix definition and cron triggers are in configurations/triggers/tier1.yaml
def call(Map pipelineParams = [:]) {
    def builder = getJenkinsLabels("aws", "eu-west-1")

    // Get matrix file from params or use default
    def matrixFile = pipelineParams.get('matrix_file', 'configurations/triggers/tier1.yaml')

    // Get cron configuration - can be overridden via pipelineParams
    // Otherwise, uses the cron_triggers defined in the matrix file
    def cronConfig = pipelineParams.get('parameterizedCron', '')

    pipeline {
        agent {
            label {
                   label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }

        parameters {
            string(name: 'matrix_file', defaultValue: "${matrixFile}", description: 'Path to the matrix configuration file')
            string(name: 'backend', defaultValue: '', description: 'Backend to use (e.g., aws, gce, azure)')
            string(name: 'job_folder', defaultValue: '', description: 'Job folder prefix (e.g., scylla-master, branch-2025.4). If empty, will be determined from scylla_version')

            // ScyllaDB Configuration
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection (Choose only one from below 6 options)')
            string(defaultValue: '', description: 'AMI ID for ScyllaDB', name: 'scylla_ami_id')
            string(defaultValue: '', description: 'GCE image for ScyllaDB', name: 'gce_image_db')
            string(defaultValue: '', description: 'Azure image for ScyllaDB', name: 'azure_image_db')
            string(defaultValue: '', description: 'Cloud path for RPMs, s3:// or gs://', name: 'update_db_packages')
            string(name: 'scylla_version', defaultValue: '', description: 'Version of ScyllaDB')
            string(name: 'scylla_repo', defaultValue: '', description: 'Repository for ScyllaDB')

            // Stress Test Configuration
            separator(name: 'STRESS_TEST', sectionHeader: 'Stress Test Configuration')
            string(name: 'stress_duration', defaultValue: '', description: 'Duration in minutes for stress commands (gemini, c-s, s-b)')

            // Trigger Configuration
            separator(name: 'TRIGGER_CONFIG', sectionHeader: 'Trigger Configuration')
            booleanParam(name: 'use_job_throttling', defaultValue: true, description: 'If true, use job throttling to limit the number of concurrent builds')
            string(name: 'labels_selector', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It points how to trigger the test: weekly. Expected value: master-weekly')
            string(name: 'skip_jobs', defaultValue: '', description: 'Comma-separated list of job names to skip (emergency hatch)')
            booleanParam(name: 'dry_run', defaultValue: false, description: 'If true, perform all logic and print what would be triggered without actually triggering jobs')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
        }
        triggers {
            parameterizedCron (
                "${cronConfig}"
            )
        }

        stages {
            stage('Trigger Matrix of Jobs') {
                steps {
                    withCredentials([usernamePassword(credentialsId: 'jenkins-api-token', usernameVariable: 'JENKINS_USERNAME', passwordVariable: 'JENKINS_PASSWORD')]) {
                        script {
                        // Call Python trigger command with parameters
                        def quoteArg = { String arg ->
                            if (arg == null) {
                                return "''"
                            }
                            // Escape single quotes for POSIX shell safety
                            "'${arg.replace("'", "'\"'\"'")}'"
                        }

                        def sanitize = { String name, def value, def pattern ->
                            if (value == null) {
                                return null
                            }
                            def v = value.toString().trim()
                            if (v.isEmpty()) {
                                return null
                            }
                            if (!(v ==~ pattern)) {
                                error("Invalid value for ${name}")
                            }
                            return v
                        }

                        def pathPattern = ~/^[\w.\-\/]+$/
                        def versionPattern = ~/^[\w.\-+]+$/
                        def namePattern = ~/^[\w.@+\-]+$/
                        def labelPattern = ~/^[\w.\-]+$/
                        def csvNamePattern = ~/^[\w.\-,]+$/
                        def urlishPattern = ~/^[\w@:\-\/.+]+$/
                        def imagePattern = ~/^[\w.\-\/:+]+$/
                        def amiPattern = ~/^[A-Za-z0-9\-]+$/
                        def digitsPattern = ~/^[0-9]+$/

                        def matrixFileSafe = sanitize('matrix_file', params.matrix_file ?: matrixFile, pathPattern)
                        def requestedByUserSafe = sanitize('requested_by_user', params.requested_by_user ?: 'jenkins', namePattern)

                        def cmd = [
                            './docker/env/hydra.sh',
                            'trigger-matrix',
                            '--matrix', matrixFileSafe,
                            '--requested-by-user', requestedByUserSafe
                        ]

                        def scyllaVersionSafe = sanitize('scylla_version', params.scylla_version, versionPattern)
                        if (scyllaVersionSafe) {
                            cmd += ['--scylla-version', scyllaVersionSafe]
                        }
                        def backendSafe = sanitize('backend', params.backend, labelPattern)
                        if (backendSafe) {
                            cmd += ['--backend', backendSafe]
                        }

                        def jobFolderSafe = sanitize('job_folder', params.job_folder, pathPattern)
                        if (jobFolderSafe) {
                            cmd += ['--job-folder', jobFolderSafe]
                        }

                        def scyllaRepoSafe = sanitize('scylla_repo', params.scylla_repo, urlishPattern)
                        if (scyllaRepoSafe) {
                            cmd += ['--scylla-repo', scyllaRepoSafe]
                        }

                        def scyllaAmiIdSafe = sanitize('scylla_ami_id', params.scylla_ami_id, amiPattern)
                        if (scyllaAmiIdSafe) {
                            cmd += ['--scylla-ami-id', scyllaAmiIdSafe]
                        }

                        def gceImageDbSafe = sanitize('gce_image_db', params.gce_image_db, imagePattern)
                        if (gceImageDbSafe) {
                            cmd += ['--gce-image-db', gceImageDbSafe]
                        }

                        def azureImageDbSafe = sanitize('azure_image_db', params.azure_image_db, imagePattern)
                        if (azureImageDbSafe) {
                            cmd += ['--azure-image-db', azureImageDbSafe]
                        }

                        def updateDbPackagesSafe = sanitize('update_db_packages', params.update_db_packages, urlishPattern)
                        if (updateDbPackagesSafe) {
                            cmd += ['--update-db-packages', updateDbPackagesSafe]
                        }

                        def labelsSelectorSafe = sanitize('labels_selector', params.labels_selector, labelPattern)
                        if (labelsSelectorSafe) {
                            cmd += ['--labels-selector', labelsSelectorSafe]
                        }

                        def skipJobsSafe = sanitize('skip_jobs', params.skip_jobs, csvNamePattern)
                        if (skipJobsSafe) {
                            cmd += ['--skip-jobs', skipJobsSafe]
                        }

                        def stressDurationSafe = sanitize('stress_duration', params.stress_duration, digitsPattern)
                        if (stressDurationSafe) {
                            cmd += ['--stress-duration', stressDurationSafe]
                        }

                        if (params.use_job_throttling) {
                            cmd += ['--use-job-throttling']
                        } else {
                            cmd += ['--no-job-throttling']
                        }

                        if (params.dry_run) {
                            cmd += ['--dry-run']
                        }

                        // Quote every argument before execution to avoid shell injection
                        def cmdStr = cmd.collect { quoteArg(it) }.join(' ')
                        println("Executing: ${cmdStr}")

                        sh(script: cmdStr, returnStatus: false)
                        }
                    }
                }
            }
        }
    }
}
