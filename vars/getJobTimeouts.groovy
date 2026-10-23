#!groovy

/**
 * Load the configuration once for the build and derive the stage timeouts from it.
 *
 * This is the stage that loads the configuration on behalf of the whole build: when the pipeline
 * sets SCT_CONFIG_CACHE_FILE, the loaded configuration is exported to that file and every later
 * hydra command restores it from there instead of resolving images, repositories and sizes again.
 * The export happens under the union of every stage's SCT_* overrides (exportSctEnv), so the cache
 * holds what each stage would have exported for itself. Minicloud jobs leave SCT_CONFIG_CACHE_FILE
 * empty and load cold in every stage.
 */
List<Integer> call(Map params, String region, Map pipelineParams = [:]){
    def export_cache = configCacheExportOption()
    def cmd = """#!/bin/bash
    ${exportSctEnv(params, region, pipelineParams)}
    ./docker/env/hydra.sh load-config -b "${params.backend}" ${export_cache}
    """
    def testData = sh(script: cmd, returnStdout: true).trim()
    println(testData)
    if (params.stress_duration == "" || params.stress_duration == null) {
        testData = testData =~ /test_duration: (\d+)/
        testDuration = testData[0][1].toInteger()
    } else {
        stressDuration = params.stress_duration.toInteger()
        try {
            prepareDuration = params.prepare_stress_duration.toInteger()
        } catch (e) {
            testData = testData =~ /prepare_stress_duration: (\d+)/
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
