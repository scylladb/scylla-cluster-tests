#!groovy

List<Integer> call(Map params, String region){
    // handle params which can be a json list
    def current_region = initAwsRegionParam(params.region, region)
    def current_oci_region = ""
    if (params.oci_region_name) {
        current_oci_region = initAwsRegionParam(params.oci_region_name, region)
    }
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cmd = """#!/bin/bash
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}
    if [[ -n "${params.region ? params.region : ''}" ]] ; then
        export SCT_REGION_NAME=${current_region}
    fi

    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER='${params.gce_datacenter}'
    fi

    if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
        export SCT_AZURE_REGION_NAME=${groovy.json.JsonOutput.toJson(params.azure_region_name)}
    fi

    if [[ -n "${params.oci_region_name ? params.oci_region_name : ''}" ]] ; then
        export SCT_OCI_REGION_NAME=${current_oci_region}
    fi

    if [[ "${params.backend}" == "xcloud" ]] ; then
        export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
        export SCT_XCLOUD_ENV="${params.xcloud_env}"
    fi
    ./docker/env/hydra.sh output-conf -b "${params.backend}"
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
