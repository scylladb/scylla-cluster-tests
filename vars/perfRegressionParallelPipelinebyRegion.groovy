def call(Map pipelineParams) {
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
            string(name: 'base_versions', defaultValue: '', description: 'Base versions')
            string(name: 'new_scylla_repo', defaultValue: 'https://downloads.scylladb.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list', description: 'New Scylla repo')
            booleanParam(name: 'use_job_throttling', defaultValue: true, description: 'if true, use job throttling to limit the number of concurrent builds')
            string(name: 'labels_selector', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It points how to trigger the test: daily, weekly ot once in 3 weeks. Expected values: master-3weeks OR master-weekly OR master-daily')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
        }
        triggers {
            parameterizedCron (
                '''
                    0 8 * * * %scylla_version=master:latest;labels_selector=alternator-daily;requested_by_user=radoslawcybulski
                    0 8 * * 6 %scylla_version=master:latest;labels_selector=alternator-weekly;requested_by_user=radoslawcybulski
                    00 6 * * 0 %scylla_version=master:latest;labels_selector=master-weekly;requested_by_user=juliayakovlev
                    0 23 */21 * * %scylla_version=master:latest;labels_selector=master-3weeks;requested_by_user=juliayakovlev
                    13 6 8-14 * 2 %scylla_version=master:latest;labels_selector=gce-custom-monthly;requested_by_user=valerii.ponomarov
                '''
            )
        }

        stages {
            stage('Get Scylla Version') {
                steps {
                    script {
                        def scylla_version = params.scylla_version?.trim()
                        def labels_selector = params.labels_selector?.trim()
                        if (scylla_version == "master:latest") {
                            scylla_version = "master"
                            if (!labels_selector) {
                                error "Labels selector is not set. Please provide one of a valid 'labels_selector' values: 'master-weekly' OR 'master-daily' OR 'master-3weeks'."
                            }


                        }
                        def testRegionMatrix = [
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [], // Example: ['rc1', 'rc3']
                                sub_tests: ['"test_read_gradual_increase_load"', '"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_write_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_upgrade"'],
                                labels: ['master-weekly'],
                                rolling_upgrade_test: true
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4',],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"', '"test_latency_read_with_nemesis"', '"test_latency_write_with_nemesis"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-rbno-disabled',
                                region: 'eu-west-3',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly'],
                                microbenchmark: true
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly'],
                                microbenchmark: true
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly'],
                                microbenchmark: true
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly'],
                                microbenchmark: true
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/perf-regression-predefined-throughput-steps-sanity-vnodes',
                                region: '',
                                versions: [],
                                pre_release: [],
                                sub_tests: [],
                                labels: ['master-daily']
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/scylla-release-perf-regression-alternator',
                                region: 'eu-north-1',
                                versions: ['master'],
                                sub_tests: ['test_full'],
                                labels: ['alternator-weekly']
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/latte-perf-regression-latency-steady-state-custom-d1-workload1-vnodes',
                                cloud_provider: 'gce',
                                region: 'us-east1',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: ['rc1', 'rc3'],
                                sub_tests: ['"test_latency_steady_state"'],
                                labels: ['gce-custom-monthly']
                            ],
                            // Tablets
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_read_gradual_increase_load"', '"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_write_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/latte-perf-regression-latency-steady-state-custom-d1-workload1-tablets',
                                cloud_provider: 'gce',
                                region: 'us-east1',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: ['rc1', 'rc3'],
                                sub_tests: ['"test_latency_steady_state"'],
                                labels: ['gce-custom-monthly']
                            ],
                            // One in 3 weeks
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-tablets',
                                region: 'eu-west-2',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', 'master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_upgrade"'],
                                labels: ['master-3weeks'],
                                rolling_upgrade_test: true
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', ],
                                pre_release: [],
                                sub_tests: ['"test_latency_read_with_nemesis"', '"test_latency_mixed_with_nemesis"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['2025.1', '2025.2', '2025.3', '2025.4', ],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"', '"test_latency_write_with_nemesis"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['master'],
                                pre_release: [],
                                sub_tests: ['"test_latency_mixed_with_nemesis"'],
                                labels: ['master-3weeks']
                            ],
                        ]
                        println("testRegionMatrix: $testRegionMatrix")
                        def jobs_names = testRegionMatrix*.job_name.toSet()
                        println("Jobs names: $jobs_names")
                        def image_name = null
                        for (job_name in jobs_names) {
                            println("Job name: $job_name")
                            for (def entry in testRegionMatrix) {
                                 def cloud_provider = entry.cloud_provider ?: 'aws'
                                 def version = null
                                 def sub_tests = []
                                 def region = null
                                 def image_name_for_job = null
                                 def rolling_upgrade_test = null
                                 def microbenchmark = null
                                 if (scylla_version == "master" && !image_name){
                                    region = entry.region ?: 'us-east-1'
                                    def output = sh(script: "./docker/env/hydra.sh list-images -c ${cloud_provider} -r ${region} -o text", returnStdout: true).trim()
                                    println("Output from hydra list-images: $output")
                                    def image_name_json = output.split('\n')[-1].trim()
                                    println("Image name json: $image_name_json")
                                    if (!image_name_json){
                                        error "Image name is empty. Please check the hydra.sh command output."
                                    }

                                    image_name = new groovy.json.JsonSlurper().parseText(image_name_json).keySet()[0]
                                    println("Image name: $image_name")
                                 }

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
                                        // NOTE: Check that Scylla version matches specified 'pre-release' parts.
                                        //       Semver structure: <major> "." <minor> "." <patch> "-" <pre-release> "+" <build>
                                        if (entry.pre_release && !entry.pre_release.any { pr -> version.contains("-${pr}") }) {
                                            println("Skipping job $job_name because $version version doesn't match specified pre-releases: ${entry.pre_release}")
                                            continue
                                        }
                                        region = entry.region
                                        sub_tests = entry.sub_tests
                                        println("Found for job $job_name: region : $region, version: $version, sub_tests: $sub_tests")
                                    } else {
                                        continue
                                    }
                                    rolling_upgrade_test = entry.rolling_upgrade_test
                                    microbenchmark = entry.microbenchmark
                                    if (rolling_upgrade_test || microbenchmark) {
                                        image_name_for_job = null
                                    } else {
                                        image_name_for_job = image_name
                                    }
                                }
                            }
                            if (region && version && sub_tests) {
                                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                                    println("Building job: $job_name with sub_test: ${sub_tests}, region: ${region}, image_name_for_job: ${image_name_for_job}, scylla_version: ${version}")
                                    println("Send to job: scylla_version: ${rolling_upgrade_test ? null : (image_name_for_job ? null : params.scylla_version)}; scylla_ami_id: ${image_name_for_job ? image_name_for_job : null}")
                                        build job: job_name, wait: false, parameters: [
                                            string(name: 'scylla_version', value: rolling_upgrade_test ? null : (image_name_for_job ? null : params.scylla_version)),
                                            string(name: 'scylla_ami_id', value: image_name_for_job ? image_name_for_job : null),
                                            string(name: 'base_versions', value: rolling_upgrade_test ? params.base_versions : null),
                                            string(name: 'provision_type', value: 'on_demand'),
                                            string(name: 'new_scylla_repo', value: rolling_upgrade_test ? params.new_scylla_repo : null),
                                            booleanParam(name: 'use_job_throttling', value: params.use_job_throttling),
                                            string(name: 'sub_tests', value: groovy.json.JsonOutput.toJson(sub_tests)),
                                            string(name: 'region', value: region),
                                            string(name: 'requested_by_user', value: params.requested_by_user)
                                        ]
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
