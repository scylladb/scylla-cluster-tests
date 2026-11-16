#!groovy

def call(Map params, String region){

    withEnv(pipelineEnv(params: params, region: region)) {
    sh """#!/bin/bash

    set -xe
    env

    echo "Starting to clean resources ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        CLEANUP_TEST_ID=\${SCT_REUSE_CLUSTER:-\$SCT_TEST_ID}
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} clean-resources --backend "${params.backend}" --post-behavior --test-id \$CLEANUP_TEST_ID
    else
        ./docker/env/hydra.sh clean-resources --backend "${params.backend}" --post-behavior --logdir "`pwd`"
    fi
    echo "Finished cleaning resources."
    """
    }
}
