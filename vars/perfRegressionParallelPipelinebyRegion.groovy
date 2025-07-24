def call(Map pipelineParams) {
    pipeline {
        agent any
        parameters {
            string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
            string(name: 'base_versions', defaultValue: '', description: 'Base versions')
            string(name: 'new_scylla_repo', defaultValue: 'https://downloads.scylladb.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list', description: 'New Scylla repo')
            booleanParam(name: 'use_job_throttling', defaultValue: true, description: 'if true, use job throttling to limit the number of concurrent builds')
            string(name: 'labels_selector', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It points how to trigger the test: daily, weekly ot once in 3 weeks. Expected values: master-3weeks OR master-weekly OR master-daily')
        }
        triggers {
            parameterizedCron (
                '''
                    00 6 * * 0 %scylla_version="master:latest";labels_selector=master-weekly
                    0 23 */21 * * %scylla_version="master:latest";labels_selector=master-3weeks
                '''
            )
        }
        stages {
            stage('Get Scylla Version') {
                steps {
                    script {
                        def scylla_version = params.scylla_version?.trim()
                        def labels_selector = params.labels_selector?.trim()
                        println("Scylla version: ${scylla_version}")
                        println("Labels selector: $labels_selector")
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
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_read_gradual_increase_load"', '"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_write_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"', '"test_read_gradual_increase_load"', '"test_write_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-rbno-disabled',
                                region: 'eu-west-3',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/perf-regression-predefined-throughput-steps-sanity-vnodes',
                                region: '',
                                versions: [],
                                sub_tests: [],
                                labels: ['master-daily']
                            ],
                            // Tablets
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_read_gradual_increase_load"', '"test_mixed_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_write_gradual_increase_load"'],
                                labels: ['master-weekly']
                            ],
                            // One in 3 weeks
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-tablets',
                                region: 'eu-west-2',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['2025.1', '2025.2'],
                                sub_tests: ['"test_read_gradual_increase_load"', '"test_mixed_gradual_increase_load"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['2025.1', '2025.2'],
                                sub_tests: ['"test_mixed_gradual_increase_load"', '"test_write_gradual_increase_load"'],
                                labels: ['master-3weeks']
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['master'],
                                sub_tests: ['"test_mixed_gradual_increase_load"'],
                                labels: ['master-3weeks']
                            ],
                        ]
                        println("testRegionMatrix: $testRegionMatrix")
                        def jobs_names = testRegionMatrix*.job_name.toSet()
                        println("Jobs names: $jobs_names")
                        for (job_name in jobs_names) {
                            println("Job name: $job_name")
                            for (def entry in testRegionMatrix) {
                                def version = null
                                def sub_tests = []
                                def region = null
                                if (entry.job_name == job_name) {
                                    println("job_name: ${entry.job_name}, sub_tests: ${entry.sub_tests}")
                                    for (def ver in entry.versions) {
                                        println("Checking version: $ver against scylla_version: $scylla_version")
                                        println("If scylla_version is needed: ${scylla_version?.trim() == ver || scylla_version?.trim().startsWith(ver + ".")}")
                                        if (scylla_version?.trim() == ver || scylla_version?.trim().startsWith(ver + ".")) {
                                            version = params.scylla_version
                                        }
                                        println("Is scylla_version supported: $version")
                                    if (version) {
                                        if (labels_selector && !(entry.labels.contains(labels_selector))) {
                                            println("Skipping job $job_name for labels_selector: $labels_selector")
                                            continue
                                        }
                                        region = entry.region
                                        sub_tests = entry.sub_tests
                                        println("Found for job $job_name: region : $region, version: $version, sub_tests: $sub_tests")
                                        break
                                    }
                                }
                            }
                            if (region && version && sub_tests) {
                                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                                    println("Building job: $job_name with sub_test: ${sub_tests}, region: ${region}")
                                        build job: job_name, wait: false, parameters: [
                                            string(name: 'scylla_version', value: params.scylla_version),
                                            string(name: 'base_versions', value: params.base_versions),
                                            string(name: 'provision_type', value: 'on_demand'),
                                            string(name: 'new_scylla_repo', value: params.new_scylla_repo),
                                            string(name: 'use_job_throttling', value: params.use_job_throttling),
                                            string(name: 'sub_tests', value: groovy.json.JsonOutput.toJson(sub_tests)),
                                            string(name: 'region', value: region)
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
