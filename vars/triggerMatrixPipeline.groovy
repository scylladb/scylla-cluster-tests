// Tier1 Parallel Pipeline (Simplified)
// This pipeline now delegates to Python logic for better testability and reusability
// Matrix definition is in configurations/triggers/tier1.yaml
def call(Map pipelineParams = [:]) {
    def builder = getJenkinsLabels("aws", "eu-west-1")
    pipeline {
        agent {
            label {
                   label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            JENKINS_USERNAME      = credentials('jenkins-username')
            JENKINS_API_TOKEN     = credentials('jenkins-api-token')
        }

        parameters {
            string(name: 'matrix_file', defaultValue: "${pipelineParams.get('matrix_file', 'configurations/triggers/tier1.yaml')}", description: 'Path to the matrix configuration file')
            string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
            string(name: 'scylla_repo', defaultValue: '', description: 'Scylla repo URL')
            string(name: 'backend', defaultValue: '', description: 'Backend to use (e.g., aws, gce)')
            string(name: 'job_folder', defaultValue: '', description: 'Job folder prefix (e.g., scylla-master, branch-2025.4). If empty, will be determined from scylla_version')
            string(name: 'stress_duration', defaultValue: '', description: 'Duration in minutes for stress commands(gemini, c-s, s-b)')
            booleanParam(name: 'use_job_throttling', defaultValue: true, description: 'if true, use job throttling to limit the number of concurrent builds')
            string(name: 'labels_selector', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It points how to trigger the test: weekly. Expected value: master-weekly')
            string(name: 'skip_jobs', defaultValue: '', description: 'Comma-separated list of job names to skip (emergency hatch)')
            booleanParam(name: 'dry_run', defaultValue: false, description: 'if true, perform all logic and print what would be triggered without actually triggering jobs')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
        }
        triggers {
            parameterizedCron (
                "${pipelineParams.get('parameterizedCron')}"
            )
        }

        stages {
            stage('Trigger Matrix of Jobs') {
                steps {
                    script {
                        // Call Python trigger command with parameters
                        def cmd = [
                            './sct.py',
                            'trigger-matrix',
                            '--matrix', params.matrix_file,
                            '--scylla-version', params.scylla_version ?: 'master',
                            '--requested-by-user', params.requested_by_user ?: 'jenkins'
                        ]

                        if (params.backend) {
                            cmd += ['--backend', params.backend]
                        }

                        if (params.job_folder) {
                            cmd += ['--job-folder', params.job_folder]
                        }

                        if (params.scylla_repo) {
                            cmd += ['--scylla-repo', params.scylla_repo]
                        }

                        if (params.labels_selector) {
                            cmd += ['--labels-selector', params.labels_selector]
                        }

                        if (params.skip_jobs) {
                            cmd += ['--skip-jobs', params.skip_jobs]
                        }

                        if (params.stress_duration) {
                            cmd += ['--stress-duration', params.stress_duration]
                        }

                        if (params.use_job_throttling) {
                            cmd += ['--use-job-throttling']
                        } else {
                            cmd += ['--no-job-throttling']
                        }

                        if (params.dry_run) {
                            cmd += ['--dry-run']
                        }

                        // Execute the command
                        def cmdStr = cmd.join(' ')
                        println("Executing: ${cmdStr}")

                        sh(script: cmdStr, returnStatus: false)
                    }
                }
            }
        }
    }
}
