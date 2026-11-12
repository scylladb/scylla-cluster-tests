#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild) {
    def test_status = currentBuild.currentResult

    withEnv(pipelineEnv(params: params)) {
    sh """#!/bin/bash
    set -xe
    echo "Finishing Argus test run ..."

    ./docker/env/hydra.sh finish-argus-test-run --jenkins-status "${test_status}"

    echo " Argus test run finished."
    """
    }
}
