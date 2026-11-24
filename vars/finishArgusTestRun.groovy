#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

// `status` overrides the build result, for a caller that finishes a run before the build result is final.
def call(Map params, RunWrapper currentBuild, String status = null) {
    def test_status = status ?: currentBuild.currentResult

    withEnv(pipelineEnv(params: params)) {
    sh """#!/bin/bash
    set -xe
    echo "Finishing Argus test run ..."

    ./docker/env/hydra.sh finish-argus-test-run --jenkins-status "${test_status}"

    echo " Argus test run finished."
    """
    }
}
