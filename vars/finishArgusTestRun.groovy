#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild) {
    def test_config = env.TEST_CONFIG ?: groovy.json.JsonOutput.toJson(params.test_config)
    def test_status = currentBuild.currentResult

    sh """#!/bin/bash
    set -xe
    echo "Finishing Argus test run ..."

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}

    ./docker/env/hydra.sh finish-argus-test-run --jenkins-status "${test_status}"

    echo " Argus test run finished."
    """
}
