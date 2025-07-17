def call(Map pipelineParams) {
    pipeline {
        agent any
        parameters {
            string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
            string(name: 'base_versions', defaultValue: '', description: 'Base versions')
            string(name: 'provision_type', defaultValue: '', description: 'Provision type')
            string(name: 'new_scylla_repo', defaultValue: '', description: 'New Scylla repo')
            string(name: 'use_job_throttling', defaultValue: 'false', description: 'Use job throttling')
            string(name: 'matrix_storage', defaultValue: '', description: 'This parameter is used for trigger with Scylla master version only. It point which kind of test to run: vnodes or tablets. Expected values: master-tablets OR master-vnodes OR master-daily')
        }
        triggers {
            parameterizedCron (
                '''
                    00 6 * * 0 %scylla_version="master:latest";matrix_storage=master-vnodes
                    0 23 */21 * * %scylla_version="master:latest";matrix_storage=master-tablets
                '''
            )
        }
        stages {
            stage('Get Scylla Version') {
                steps {
                    script {
                        def scylla_version = params.scylla_version?.trim()
                        def matrix_storage = params.matrix_storage?.trim()
                        println("Scylla version: ${scylla_version}")
                        println("Matrix storage: $matrix_storage")
                        if (scylla_version == "master:latest") {
                            scylla_version = "master"
                            if (!matrix_storage) {
                                error "Test type (vnodes or tablets) is not set. Please provide one of a valid 'matrix_storage' values: 'master-tablets' OR 'master-vnodes'"
                            }
                        }
                        def testRegionMatrix = [
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['test_write_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade',
                                region: 'eu-west-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis',
                                region: 'eu-west-2',
                                versions: ['master'],
                                sub_tests: ['test_mixed_gradual_increase_load', 'test_read_gradual_increase_load', 'test_write_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-rbno-disabled',
                                region: 'eu-west-3',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64-write',
                                region: 'us-east-1',
                                versions: ['2024.1', '2024.2', '2025.1', '2025.2', 'master'],
                                sub_tests: ['microbenchmark'],
                                scheduler: 'master-vnodes'
                            ],
                            [
                                job_name: 'scylla-master/perf-regression/perf-regression-predefined-throughput-steps-sanity-vnodes',
                                region: '',
                                versions: [],
                                sub_tests: [],
                                scheduler: 'master-daily'
                            ],
                            [
                                job_name: 'scylla-staging/yulia/performance/perf-regression-predefined-throughput-steps-sanity-vnodes',
                                region: 'us-east-1',
                                versions: ['master'],
                                sub_tests: [''"test_mixed_gradual_increase_load"''],
                                scheduler: 'master-daily'
                            ],
                            // Tablets
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets',
                                region: 'us-east-1',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['test_write_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            // One in 3 weeks
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-tablets',
                                region: 'eu-west-2',
                                versions: ['2025.1', '2025.2', 'master'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['2025.1', '2025.2'],
                                sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets',
                                region: 'eu-west-3',
                                versions: ['master'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['2025.1', '2025.2'],
                                sub_tests: ['test_mixed_gradual_increase_load', 'test_write_gradual_increase_load'],
                                scheduler: 'master-tablets'
                            ],
                            [
                                job_name: 'scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-elasticity',
                                region: 'eu-north-1',
                                versions: ['master'],
                                sub_tests: ['test_mixed_gradual_increase_load'],
                                scheduler: 'master-tablets'
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
                                        if (matrix_storage && entry.scheduler != matrix_storage) {
                                            println("Skipping job $job_name for matrix_storage: $matrix_storage")
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
                                println("Building job: $job_name with sub_test: ${sub_tests}, region: ${region}")
                                    build job: job_name, parameters: [
                                        string(name: 'scylla_version', value: params.scylla_version),
                                        string(name: 'base_versions', value: params.base_versions),
                                        string(name: 'provision_type', value: params.provision_type),
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
