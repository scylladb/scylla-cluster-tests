// vars/triggerMatrixPipeline.groovy
// Thin Groovy pipeline wrapper for Python-driven trigger matrix.
// All logic lives in sdcm/utils/trigger_matrix.py — this pipeline only
// sanitizes inputs, builds the CLI command, and executes it.

def call(Map pipelineParams = [:]) {
    def matrixFile = pipelineParams.get('matrix_file', '')
    def cronSpec = pipelineParams.get('cron', '')

    pipeline {
        agent {
            label {
                label "aws-sct-builder-v2"
                retries 3
            }
        }

        parameters {
            string(name: 'matrix_file', defaultValue: matrixFile,
                   description: 'Path to trigger matrix YAML file')
            string(name: 'scylla_version', defaultValue: '',
                   description: 'Scylla version (e.g., master:latest, 2024.2.5-0.20250221.xxx-1)')
            string(name: 'labels_selector', defaultValue: '',
                   description: 'Comma-separated labels to filter jobs (AND logic)')
            string(name: 'backend', defaultValue: '',
                   description: 'Filter by backend (aws/gce/azure)')
            string(name: 'skip_jobs', defaultValue: '',
                   description: 'Comma-separated job names to skip')
            string(name: 'job_folder', defaultValue: '',
                   description: 'Override auto-detected Jenkins job folder')
            string(name: 'stress_duration', defaultValue: '',
                   description: 'Override stress duration for all jobs')
            string(name: 'region', defaultValue: '',
                   description: 'Override region for all jobs')
            string(name: 'provision_type', defaultValue: '',
                   description: 'spot | on_demand | spot_fleet')
            string(name: 'requested_by_user', defaultValue: '',
                   description: 'User requesting the run')
            booleanParam(name: 'dry_run', defaultValue: false,
                   description: 'Preview mode — do not trigger jobs')
        }

        triggers {
            parameterizedCron(cronSpec)
        }

        options {
            timestamps()
            timeout(time: 30, unit: 'MINUTES')
            buildDiscarder(logRotator(numToKeepStr: '30'))
        }

        stages {
            stage('Trigger Matrix') {
                steps {
                    script {
                        // Input sanitization — allow only safe characters
                        def safePattern = ~/^[a-zA-Z0-9_.:\-\/,\s]*$/
                        def paramChecks = [
                            'matrix_file': params.matrix_file,
                            'scylla_version': params.scylla_version,
                            'labels_selector': params.labels_selector,
                            'backend': params.backend,
                            'skip_jobs': params.skip_jobs,
                            'job_folder': params.job_folder,
                            'stress_duration': params.stress_duration,
                            'region': params.region,
                            'provision_type': params.provision_type,
                            'requested_by_user': params.requested_by_user,
                        ]

                        paramChecks.each { name, value ->
                            if (value && !(value ==~ safePattern)) {
                                error("Invalid characters in parameter '${name}': ${value}")
                            }
                        }

                        if (!params.matrix_file?.trim()) {
                            error("'matrix_file' parameter is required")
                        }
                        if (!params.scylla_version?.trim()) {
                            error("'scylla_version' parameter is required")
                        }

                        // Build CLI command
                        def cmd = "./docker/env/hydra.sh trigger-matrix"
                        cmd += " --matrix '${params.matrix_file}'"
                        cmd += " --scylla-version '${params.scylla_version}'"

                        if (params.job_folder?.trim()) {
                            cmd += " --job-folder '${params.job_folder}'"
                        }
                        if (params.labels_selector?.trim()) {
                            cmd += " --labels-selector '${params.labels_selector}'"
                        }
                        if (params.backend?.trim()) {
                            cmd += " --backend '${params.backend}'"
                        }
                        if (params.skip_jobs?.trim()) {
                            cmd += " --skip-jobs '${params.skip_jobs}'"
                        }
                        if (params.stress_duration?.trim()) {
                            cmd += " --stress-duration '${params.stress_duration}'"
                        }
                        if (params.region?.trim()) {
                            cmd += " --region '${params.region}'"
                        }
                        if (params.provision_type?.trim()) {
                            cmd += " --provision-type '${params.provision_type}'"
                        }
                        if (params.requested_by_user?.trim()) {
                            cmd += " --requested-by-user '${params.requested_by_user}'"
                        }
                        if (params.dry_run) {
                            cmd += " --dry-run"
                        }

                        sh(cmd)
                    }
                }
            }
        }

        post {
            always {
                cleanWs()
            }
        }
    }
}
