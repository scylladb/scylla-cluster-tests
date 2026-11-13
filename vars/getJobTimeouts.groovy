#!groovy

List<Integer> call(Map params, String region){
    def cmd = """#!/bin/bash

    ./docker/env/hydra.sh output-conf -b "${params.backend}"
    """
    def testData
    withEnv(pipelineEnv(params: params, region: region)) {
        testData = sh(script: cmd, returnStdout: true).trim()
    }
    println(testData)
    if (params.stress_duration == "" || params.stress_duration == null) {
        // Anchor to the line start, otherwise 'spot_max_test_duration' (printed first) matches.
        testData = testData =~ /(?m)^test_duration: (\d+)/
        testDuration = testData[0][1].toInteger()
    } else {
        stressDuration = params.stress_duration.toInteger()
        try {
            prepareDuration = params.prepare_stress_duration.toInteger()
        } catch (e) {
            testData = testData =~ /(?m)^prepare_stress_duration: (\d+)/
            prepareDuration = testData[0][1].toInteger()
        }
        Integer stressEndup = 10
        testDuration = prepareDuration + stressDuration + stressEndup
    }
    Integer testStartupTimeout = 20
    Integer testTeardownTimeout = 40
    // Scale log collection timeout with test duration: base 90 min + 1 min per hour of test.
    // For a 3-day test (4320 min): max(90, 90 + 72) = 162 minutes.
    // Can be overridden per-job via params.collect_logs_timeout.
    Integer collectLogsTimeout = params.collect_logs_timeout ? params.collect_logs_timeout.toInteger()
        : Math.max(90, 90 + (testDuration / 60).toInteger())
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
