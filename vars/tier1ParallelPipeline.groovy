// Tier1 Parallel Pipeline
// Consolidates all tier1 test triggers into a single Groovy pipeline
// Parameters are passed via Jenkins pipeline parameters, not the pipelineParams Map
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
        }

        parameters {
            string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
            string(name: 'scylla_repo', defaultValue: '', description: 'Scylla repo URL')
            booleanParam(name: 'use_job_throttling', defaultValue: true, description: 'if true, use job throttling to limit the number of concurrent builds')
            string(name: 'labels_selector', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It points how to trigger the test: weekly. Expected value: master-weekly')
            string(name: 'skip_jobs', defaultValue: '', description: 'Comma-separated list of job names to skip (emergency hatch)')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
        }
        triggers {
            parameterizedCron (
                '''
                    00 6 * * 6 %scylla_version=master:latest;labels_selector=master-weekly;requested_by_user=timtimb0t
                '''
            )
        }

        stages {
            stage('Get Scylla Version') {
                steps {
                    script {
                        def scylla_version = params.scylla_version?.trim()
                        def labels_selector = params.labels_selector?.trim()
                        def skip_jobs_list = params.skip_jobs?.trim()?.split(',')?.collect { it.trim() } ?: []

                        if (scylla_version == "master:latest") {
                            scylla_version = "master"
                            if (!labels_selector) {
                                error "Labels selector is not set. Please provide 'labels_selector' value: 'master-weekly'."
                            }
                        }

                        // Define the tier1 test matrix
                        // Each entry defines a test job with its configuration
                        def tier1TestMatrix = [
                            // AWS Tests
                            [
                                job_name: 'scylla-master/tier1/longevity-50gb-3days-test',
                                backend: 'aws',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-50GB-3days-authorization-and-tls-ssl.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/longevity-150gb-asymmetric-cluster-12h-test',
                                backend: 'aws',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-150GB-12h-autorization-LimitedMonkey-cql-stress.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/longevity-twcs-48h-test',
                                backend: 'aws',
                                region: 'us-east-1',
                                arch: 'arm64',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-twcs-48h.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/longevity-multidc-schema-topology-changes-12h-test',
                                backend: 'aws',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-schema-topology-changes-12h.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/longevity-mv-si-4days-streaming-test',
                                backend: 'aws',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-mv-si-4days.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/longevity-schema-topology-changes-12h-test',
                                backend: 'aws',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-schema-topology-changes-12h.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/gemini-1tb-10h-test',
                                backend: 'aws',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/gemini/gemini-1tb-10h.yaml'
                            ],
                            [
                                job_name: 'scylla-master/longevity/longevity-harry-2h-test',
                                backend: 'aws',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-harry-2h.yaml'
                            ],
                            // Azure Tests
                            [
                                job_name: 'scylla-master/tier1/longevity-1tb-5days-azure-test',
                                backend: 'azure',
                                region: '',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-1TB-5days-authorization-and-tls-ssl.yaml'
                            ],
                            // GCE Tests
                            [
                                job_name: 'scylla-master/tier1/longevity-large-partition-200k-pks-4days-gce-test',
                                backend: 'gce',
                                region: '',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/longevity/longevity-large-partition-200k-pks-4days.yaml'
                            ],
                            [
                                job_name: 'scylla-master/tier1/jepsen-all-test',
                                backend: 'gce',
                                region: '',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                labels: ['master-weekly'],
                                test_config: 'test-cases/jepsen/jepsen.yaml'
                            ],
                        ]

                        println("tier1TestMatrix: $tier1TestMatrix")
                        println("Skip jobs list: $skip_jobs_list")

                        def jobs_names = tier1TestMatrix*.job_name.toSet()
                        println("Jobs names: $jobs_names")

                        // Fetch AMI image once for master builds (before looping through jobs)
                        def image_name = null
                        if (scylla_version == "master") {
                            def region = 'us-east-1'  // Default region for image fetching
                            def output = sh(script: "./docker/env/hydra.sh list-images -c aws -r ${region} -o text", returnStdout: true).trim()
                            println("Output from hydra list-images: $output")
                            def image_name_json = output.split('\n')[-1].trim()
                            println("Image name json: $image_name_json")
                            if (!image_name_json){
                                error "Image name is empty. Please check the hydra.sh command output."
                            }

                            image_name = new groovy.json.JsonSlurper().parseText(image_name_json).keySet()[0]
                            println("AMI image name for master: $image_name")
                        }

                        for (job_name in jobs_names) {
                            // Skip if job is in skip list
                            if (skip_jobs_list.contains(job_name)) {
                                println("Skipping job $job_name as it's in the skip_jobs list")
                                continue
                            }

                            println("Job name: $job_name")
                            for (def entry in tier1TestMatrix) {
                                def backend = entry.backend ?: 'aws'
                                def version = null
                                def region = null
                                def test_config = null
                                def image_name_for_job = null
                                def arch = entry.arch ?: 'x86_64'

                                if (entry.job_name == job_name) {
                                    for (def ver in entry.versions) {
                                        if (scylla_version?.trim() == ver || scylla_version?.trim().startsWith(ver + ".")) {
                                            version = params.scylla_version
                                        }
                                        if (version) {
                                            if (labels_selector && !(entry.labels.contains(labels_selector))) {
                                                println("Skipping job $job_name for labels_selector: $labels_selector")
                                                continue
                                            }
                                            region = entry.region
                                            test_config = entry.test_config
                                            println("Found for job $job_name: backend: $backend, region: $region, version: $version, test_config: $test_config, arch: $arch")
                                        } else {
                                            continue
                                        }

                                        if (backend == 'aws' && image_name) {
                                            // If ARM architecture is specified and we have an x86_64 image,
                                            // find the ARM equivalent using find-ami-equivalent
                                            if (arch == 'arm64') {
                                                println("Finding ARM64 equivalent for AMI: $image_name in region: ${region ?: 'us-east-1'}")
                                                def target_region = region ?: 'us-east-1'
                                                def find_ami_output = sh(
                                                    script: "./sct.py find-ami-equivalent --ami-id ${image_name} --source-region us-east-1 --target-region ${target_region} --target-arch arm64 --output-format text",
                                                    returnStdout: true
                                                ).trim()
                                                println("ARM64 AMI equivalent output: $find_ami_output")
                                                if (find_ami_output) {
                                                    // The text format returns just the AMI ID
                                                    def arm_ami = find_ami_output.split('\n')[-1].trim()
                                                    if (arm_ami && arm_ami.startsWith('ami-')) {
                                                        image_name_for_job = arm_ami
                                                        println("Using ARM64 AMI: $image_name_for_job")
                                                    } else {
                                                        println("Warning: Could not find ARM64 equivalent for $image_name, using original")
                                                        image_name_for_job = image_name
                                                    }
                                                } else {
                                                    println("Warning: find-ami-equivalent returned empty, using original AMI")
                                                    image_name_for_job = image_name
                                                }
                                            } else {
                                                image_name_for_job = image_name
                                            }
                                        }
                                    }
                                }

                                if (version && test_config) {
                                    catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                                        println("Building job: $job_name with backend: ${backend}, region: ${region}, image_name_for_job: ${image_name_for_job}, scylla_version: ${version}")
                                        def build_params = [
                                            string(name: 'provision_type', value: 'on_demand'),
                                            booleanParam(name: 'use_job_throttling', value: params.use_job_throttling),
                                            string(name: 'requested_by_user', value: params.requested_by_user),
                                            string(name: 'post_behavior_db_nodes', value: 'destroy'),
                                            string(name: 'post_behavior_monitor_nodes', value: 'destroy'),
                                        ]

                                        // Add backend-specific parameters
                                        if (backend == 'aws') {
                                            build_params += [
                                                string(name: 'scylla_version', value: image_name_for_job ? '' : params.scylla_version),
                                                string(name: 'scylla_ami_id', value: image_name_for_job ?: ''),
                                                string(name: 'region', value: region ?: 'us-east-1'),
                                                string(name: 'availability_zone', value: 'c')
                                            ]
                                        } else if (backend == 'azure') {
                                            build_params += [
                                                string(name: 'scylla_version', value: params.scylla_version),
                                                string(name: 'azure_image_db', value: '')
                                            ]
                                        } else if (backend == 'gce') {
                                            build_params += [
                                                string(name: 'scylla_version', value: params.scylla_version),
                                                string(name: 'gce_image_db', value: ''),
                                                string(name: 'availability_zone', value: 'a')
                                            ]
                                        }

                                        // Add scylla_repo if provided
                                        if (params.scylla_repo) {
                                            build_params += [string(name: 'scylla_repo', value: params.scylla_repo)]
                                        }

                                        build job: job_name, wait: false, parameters: build_params
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
