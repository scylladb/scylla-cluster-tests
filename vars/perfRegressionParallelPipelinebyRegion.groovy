pipeline {
    agent any
    parameters {
        string(name: 'scylla_version', defaultValue: '', description: 'Scylla version to test')
        string(name: 'base_versions', defaultValue: '', description: 'Base versions')
        string(name: 'provision_type', defaultValue: '', description: 'Provision type')
        string(name: 'new_scylla_repo', defaultValue: '', description: 'New Scylla repo')
        string(name: 'test_job_name', defaultValue: '', description: 'Test pipeline job name')
    }
    stages {
        stage('Get Scylla Version') {
            steps {
                script {
                    def matrix_version = params.scylla_version?.trim()
                    if (matrix_version == "master:latest") {
                        matrix_version = "master"
                    }
                    echo "Scylla version: ${matrix_version}"

                    def testRegionMatrix = [
                        ["scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes", "test_read_gradual_increase_load"]: [["2025.1", "us-east-1"], ["2025.2", "eu-west-1"]],
                        ["scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes", "test_mixed_gradual_increase_load"]: [["2025.1", "us-east-1"], ["2025.2", "eu-west-1"]],
                        ["scylla-enterprise-perf-regression-latency-650gb-with-nemesis", "test_mixed_gradual_increase_load"]: [["2025.1", "eu-west-1"]],
                        // Add more (test_name, sub_test): [[version, region], ...] pairs as needed
                    ]
                    println("Region defined in the job parameters: $params.region")
                    def test_name = env.JOB_BASE_NAME
                    println("Test name: $test_name")
                    def regionFound = null
                    def sub_tests_list = testRegionMatrix.findAll { it.key[0] == test_name }.collect { it.key[1] } ?; new JsonSlurper().parseText(sub_tests)
                    for (sub_test in sub_tests_list) {
                        def region = testRegionMatrix[[test_name, sub_test]]
                        def region = regions?.find { it[0] == matrix_version }?.getAt(1)
                        if (region) {
                            regionFound = region
                            break
                        }
                    }
                    def test_region = regionFound ?: params.region

                    println("Adjusted region: $test_region").
                }
            }
        }
        stage('Call Test Pipeline') {
            steps {
                script {
                    // ...existing code...
                    build job: params.test_job_name, parameters: [
                        string(name: 'scylla_version', value: params.scylla_version),
                        string(name: 'base_versions', value: params.base_versions),
                        string(name: 'provision_type', value: params.provision_type),
                        string(name: 'new_scylla_repo', value: params.new_scylla_repo)
                        string(name: 'sub_tests', value: sub_tests_list)
                        string(name: 'region', value: region)
                    ]
                }
            }
        }
    }
}
