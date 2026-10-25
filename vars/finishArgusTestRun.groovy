#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def test_status = currentBuild.currentResult

    sh """#!/bin/bash
    set -xe
    echo "Finishing Argus test run ..."

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}

    if [[ -n "${params.reuse_cluster ?: ''}" ]] ; then
        export SCT_REUSE_CLUSTER="${params.reuse_cluster}"
    fi

    if [[ "${params.backend}" == "xcloud" ]] ; then
        export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
        export SCT_XCLOUD_ENV="${params.xcloud_env}"
    fi
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} finish-argus-test-run --jenkins-status "${test_status}" || \\
        ./docker/env/hydra.sh finish-argus-test-run --jenkins-status "${test_status}" --logdir "`pwd`"
    else
        ./docker/env/hydra.sh finish-argus-test-run --jenkins-status "${test_status}" --logdir "`pwd`"
    fi

    echo " Argus test run finished."
    """
}
