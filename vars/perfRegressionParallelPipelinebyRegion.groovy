def call(Map pipelineParams) {
    pipeline {
        agent any
        parameters {
            string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
            string(name: 'base_versions', defaultValue: '', description: 'Base versions')
            string(name: 'provision_type', defaultValue: '', description: 'Provision type')
            string(name: 'new_scylla_repo', defaultValue: '', description: 'New Scylla repo')
        }
        stages {
            stage('Get Scylla Version') {
                steps {
                    script {
                        def matrix_version = params.scylla_version?.trim()
                        def job = "release-triggers/performance/perf-regression-trigger"  // not created yet
                        if (matrix_version == "master:latest") {
                            matrix_version = "master"
                            job = "master-triggers/performance/weekly-performance-trigger"
                        }
                        echo "Scylla version: ${matrix_version}"

                        def testRegionMatrix = [
                            [test_name: 'scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes', region: 'us-east-1', versions: ['2024.1', '2024.2', '2025.1', '2025.2'], sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-predefined-throughput-steps-write-vnodes', region: 'us-east-1', versions: ['2024.1', '2024.2', '2025.1', '2025.2'], sub_tests: ['test_write_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade', region: 'eu-west-1', versions: ['2024.1', '2024.2', '2025.1', '2025.2'], sub_tests: ['test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-with-nemesis', region: 'eu-west-2', versions: ['2024.1', '2024.2', '2025.1', '2025.2'], sub_tests: ['test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-with-nemesis-rbno-disabled', region: 'eu-west-3', versions: ['2024.1', '2024.2', '2025.1', '2025.2'], sub_tests: ['test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-predefined-throughput-steps-tablets', region: 'us-east-1', versions: ['2025.1', '2025.2'], sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-predefined-throughput-steps-write-tablets', region: 'us-east-1', versions: ['2025.1', '2025.2'], sub_tests: ['test_write_gradual_increase_load'], scheduler: ''],
                            // One in 3 weeks
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-during-rolling-upgrade-tablets', region: 'eu-west-2', versions: ['2025.1', '2025.2'], sub_tests: ['test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-with-nemesis-tablets', region: 'eu-west-3', versions: ['2025.1', '2025.2'], sub_tests: ['test_read_gradual_increase_load', 'test_mixed_gradual_increase_load'], scheduler: ''],
                            [test_name: 'scylla-enterprise-perf-regression-latency-650gb-elasticity', region: 'eu-north-1', versions: ['2025.1', '2025.2'], sub_tests: ['test_mixed_gradual_increase_load', 'test_write_gradual_increase_load'], scheduler: ''],

                        ]
//                         def testRegionMatrix = [
//                             ["scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes", "test_read_gradual_increase_load"]: [["2025.1", "us-east-1"], ["2025.2", "eu-west-1"]],
//                             ["scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes", "test_mixed_gradual_increase_load"]: [["2025.1", "us-east-1"], ["2025.2", "eu-west-1"]],
//                             ["scylla-enterprise-perf-regression-latency-650gb-with-nemesis", "test_mixed_gradual_increase_load"]: [["2025.1", "eu-west-1"]],
//                             // Add more (test_name, sub_test): [[version, region], ...] pairs as needed
//                         ]/
                        println("Region defined in the job parameters: $params.region")
                        def job_name = ""
                        def test_names = perfRegressionJobs()
                        for (test_name in test_names) {
                            job_name = test_name.tokenize('/')[-1]

                            println("Test name: $test_name")
                            println("Job name: $job_name")
                            def regionFound = null
                            def sub_tests_list = testRegionMatrix.findAll { it.key[0] == test_name }.collect { it.key[1] }
                            if (!sub_tests_list) {
                                continue
                            }
                            def sub_tests_with_regions = []
                            for (sub_test in sub_tests_list) {
                                def regions = testRegionMatrix[[test_name, sub_test]]
                                if (regions) {
                                    for (def entry in regions) {
                                        if (entry[0] == matrix_version) {
                                            sub_tests_with_regions << [sub_test: sub_test, region: entry[1]]
                                        }
                                    }
                                }
                            }
                            if (sub_tests_with_regions) {
                                for (entry in sub_tests_with_regions) {
                                    println("Building job: $test_name with sub_test: ${entry.sub_test}, region: ${entry.region}")
                                    build job: test_name, parameters: [
                                        string(name: 'scylla_version', value: matrix_version),
                                        string(name: 'base_versions', value: params.base_versions),
                                        string(name: 'provision_type', value: params.provision_type),
                                        string(name: 'new_scylla_repo', value: params.new_scylla_repo),
                                        string(name: 'sub_test', value: entry.sub_test),
                                        string(name: 'region', value: entry.region)
                                    ]
                                }
                            }
                        }
                    }
                }
            }
//             stage('Call Test Pipeline') {
//                 steps {
//                     script {
//                         env.JOB_NAME
//                         build job: params.test_job_name, parameters: [
//                             string(name: 'scylla_version', value: params.scylla_version),
//                             string(name: 'base_versions', value: params.base_versions),
//                             string(name: 'provision_type', value: params.provision_type),
//                             string(name: 'new_scylla_repo', value: params.new_scylla_repo)
//                             string(name: 'sub_tests', value: sub_tests_list)
//                             string(name: 'region', value: region)
//                         ]
//                     }
//                 }
//             }
        }
    }
}
