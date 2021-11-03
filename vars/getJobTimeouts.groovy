#!groovy

List<Integer> call(Map params, String region){
    // handle params which can be a json list
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cmd = """
    #!/bin/bash
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}
    ./docker/env/hydra.sh output-conf -b "${params.backend}"
    """
    def testData = sh(script: cmd, returnStdout: true).trim()
    println(testData)
    testData = testData =~ /test_duration: (\d+)/
    testDuration = testData[0][1].toInteger()
    Integer testStartupTimeout = 20
    Integer testTeardownTimeout = 40
    Integer collectLogsTimeout = 70
    Integer resourceCleanupTimeout = 30
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
