#!groovy

// Ask SCT whether the attempt that just failed was lost to a spot reclaim and gets another attempt.
// Returns the instance_provision for the next attempt ('spot' or 'on_demand'), or 'stop'.
// All rules live in sdcm/utils/spot_retry.py; anything unexpected here also means 'stop'.
String call(Map params, int attempt) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def output = ''
    try {
        output = sh(returnStdout: true, script: """#!/bin/bash
        set -e
        export SCT_CLUSTER_BACKEND="${params.backend}"
        export SCT_CONFIG_FILES=${test_config}
        ./docker/env/hydra.sh spot-retry-decision --attempt ${attempt}
        """)
    } catch (error) {
        echo "spot-retry-decision failed, not retrying: ${error}"
        return 'stop'
    }
    def marker = 'SPOT_RETRY_DECISION='
    def decision = 'stop'
    for (String line : output.readLines()) {
        if (line.contains(marker)) {
            decision = line.substring(line.indexOf(marker) + marker.length()).trim()
        }
    }
    echo "Spot retry decision after attempt ${attempt}: ${decision}"
    return decision in ['spot', 'on_demand'] ? decision : 'stop'
}
