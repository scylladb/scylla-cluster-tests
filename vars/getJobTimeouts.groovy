#!groovy

List<Integer> call(Map params, String region){
    // handle params which can be a json list
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cmd = """
    #!/bin/bash
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}

    if [[ -n "${params.k8s_scylla_operator_docker_image ? params.k8s_scylla_operator_docker_image : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE=${params.k8s_scylla_operator_docker_image}
    fi
    if [[ -n "${params.k8s_scylla_operator_helm_repo ? params.k8s_scylla_operator_helm_repo : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_HELM_REPO=${params.k8s_scylla_operator_helm_repo}
    fi
    if [[ -n "${params.k8s_scylla_operator_chart_version ? params.k8s_scylla_operator_chart_version : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION=${params.k8s_scylla_operator_chart_version}
    fi
    if [[ -n "${params.scylla_mgmt_agent_version ? params.scylla_mgmt_agent_version : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_VERSION=${params.scylla_mgmt_agent_version}
    fi
    ./docker/env/hydra.sh output-conf -b "${params.backend}"
    """
    def testData = sh(script: cmd, returnStdout: true).trim()
    println(testData)
    testData = testData =~ /test_duration: (\d+)/
    testDuration = testData[0][1].toInteger()
    Integer testStartupTimeout = 20
    Integer testTeardownTimeout = 40
    Integer collectLogsTimeout = 70
    Integer resourceCleanupTimeout = 15
    Integer sendEmailTimeout = 5
    Integer testRunTimeout = testStartupTimeout + testDuration + testTeardownTimeout
    Integer runnerTimeout = testRunTimeout + collectLogsTimeout + resourceCleanupTimeout + sendEmailTimeout
    println("Test duration: $testDuration")
    println("Test run timeout: $testRunTimeout")
    println("Collect logs timeout: $collectLogsTimeout")
    println("Resource cleanup timeout: $resourceCleanupTimeout")
    println("Runner timeout: $runnerTimeout")
    return [testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout]
}
