// vars/triggerMatrixPipeline.groovy
// Thin Groovy pipeline wrapper for Python-driven trigger matrix.
// All logic lives in sdcm/utils/trigger_matrix.py — this pipeline only
// sanitizes inputs, builds the CLI command, and executes it.
//
// Cron schedules are passed via the 'cron' parameter because Jenkins declarative
// pipeline triggers must be resolved before a node is allocated, and the Groovy
// sandbox blocks direct file I/O. Generate the cron spec from the matrix YAML with:
//   python3 -c "from sdcm.utils.trigger_matrix import get_parameterized_cron; print(get_parameterized_cron('path/to/matrix.yaml'))"
//
// NOTE: Either scylla_version or an image param is required. When an image is
//       provided without scylla_version, the Python code resolves the version from
//       the image's tags/labels. The resolved version is used for job folder
//       detection and passed to all triggered jobs.

def call(Map pipelineParams = [:]) {
    def matrixFile = pipelineParams.get('matrix_file', '')
    def cronSpec = pipelineParams.get('cron', '')

    pipeline {
        agent {
            label {
                label "aws-sct-builders-eu-west-1-v3-CI"
                retries 3
            }
        }

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }

        parameters {
            string(name: 'matrix_file', defaultValue: matrixFile,
                   description: 'Path to trigger matrix YAML file')
            string(name: 'scylla_version', defaultValue: '',
                   description: 'Scylla version (e.g., master:latest, 2024.2.5-0.20250221.xxx-1). If not provided, resolved from image params.')
            string(name: 'labels_selector', defaultValue: '',
                   description: 'Comma-separated labels to filter jobs (AND logic)')
            string(name: 'backend', defaultValue: '',
                   description: 'Filter jobs by backend (aws/gce/azure/oci). When image params are set, use this to run only matching backend jobs.')
            string(name: 'skip_jobs', defaultValue: '',
                   description: 'Comma-separated job names to skip')
            string(name: 'job_folder', defaultValue: '',
                   description: 'Override auto-detected Jenkins job folder')
            string(name: 'stress_duration', defaultValue: '',
                   description: 'Override stress duration for all jobs')
            string(name: 'region', defaultValue: '',
                   description: 'Override region for all jobs')
            string(name: 'availability_zone', defaultValue: '',
                   description: 'Override availability zone for all jobs')
            string(name: 'provision_type', defaultValue: '',
                   description: 'spot | on_demand | spot_fleet')
            string(name: 'scylla_ami_id', defaultValue: '',
                   description: 'Scylla AMI ID — passed to AWS jobs. Use with backend=aws to run only AWS jobs.')
            string(name: 'gce_image_db', defaultValue: '',
                   description: 'Scylla GCE image — passed to GCE jobs. Use with backend=gce to run only GCE jobs.')
            string(name: 'azure_image_db', defaultValue: '',
                   description: 'Scylla Azure image — passed to Azure jobs. Use with backend=azure to run only Azure jobs.')
            string(name: 'oci_image_db', defaultValue: '',
                   description: 'Scylla OCI image OCID — passed to OCI jobs. Use with backend=oci to run only OCI jobs.')
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
                            'availability_zone': params.availability_zone,
                            'provision_type': params.provision_type,
                            'scylla_ami_id': params.scylla_ami_id,
                            'gce_image_db': params.gce_image_db,
                            'azure_image_db': params.azure_image_db,
                            'oci_image_db': params.oci_image_db,
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
                        def hasImageParam = params.scylla_ami_id?.trim() || params.gce_image_db?.trim() || params.azure_image_db?.trim() || params.oci_image_db?.trim()
                        if (!params.scylla_version?.trim() && !hasImageParam) {
                            error("Either 'scylla_version' or an image param (scylla_ami_id, gce_image_db, azure_image_db, oci_image_db) is required")
                        }

                        // Build CLI command
                        def cmd = "./docker/env/hydra.sh trigger-matrix"
                        cmd += " --matrix '${params.matrix_file}'"
                        if (params.scylla_version?.trim()) {
                            cmd += " --scylla-version '${params.scylla_version}'"
                        }
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
                        if (params.availability_zone?.trim()) {
                            cmd += " --availability-zone '${params.availability_zone}'"
                        }
                        if (params.provision_type?.trim()) {
                            cmd += " --provision-type '${params.provision_type}'"
                        }
                        if (params.scylla_ami_id?.trim()) {
                            cmd += " --scylla-ami-id '${params.scylla_ami_id}'"
                        }
                        if (params.gce_image_db?.trim()) {
                            cmd += " --gce-image-db '${params.gce_image_db}'"
                        }
                        if (params.azure_image_db?.trim()) {
                            cmd += " --azure-image-db '${params.azure_image_db}'"
                        }
                        if (params.oci_image_db?.trim()) {
                            cmd += " --oci-image-db '${params.oci_image_db}'"
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
