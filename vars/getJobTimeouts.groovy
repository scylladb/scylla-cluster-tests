#!groovy

List<Integer> call(Map params, String region){
    // handle params which can be a json list
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cmd = """
    #!/bin/bash
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}
    ./docker/env/hydra.sh conf -b "${params.backend}" 2>/dev/null | grep test_duration | awk '{print \$2}'
    """
    def testDuration = sh(script: cmd, returnStdout: true).trim()
    println("Test duration: $testDuration")
    testDuration = testDuration.toInteger()
    println("Test duration: $testDuration")
    Integer testRunTimeout = testDuration + Math.max( (int) (testDuration * 0.1), (int) 20)
    println("Test run timeout: $testRunTimeout")
    Integer runnerTimeout = (int) (testRunTimeout * 1.2)
    println("Runner timeout: $runnerTimeout")
    Integer collectLogsTimeout = Math.max( (int) (testDuration * 0.2), (int) 20)
    println("Collect logs timeout: $collectLogsTimeout")
    Integer resourceCleanupTimeout = Math.max( (int) (testDuration * 0.2), (int) 20)
    println("Resource cleanup timeout: $resourceCleanupTimeout")
    return [testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout]
}
