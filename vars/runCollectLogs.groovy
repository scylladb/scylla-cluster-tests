#! groovy

def call(Map params, String region){
    withEnv(pipelineEnv(params: params, region: region)) {
    sh """#!/bin/bash

    set -xe
    env

    echo "${params.test_config}"

    echo "start collect logs ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} collect-logs --backend "${params.backend}"
    else
        ./docker/env/hydra.sh collect-logs --backend "${params.backend}" --logdir "`pwd`"
    fi
    echo "end collect logs"
    """
    }

    collectBuilderLogs(params)
    collectTestCoredumps()
}
