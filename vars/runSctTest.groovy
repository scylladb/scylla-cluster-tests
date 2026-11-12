#!groovy

def call(Map params, String region, functional_test = false, Map pipelineParams = [:]){
    def test_cmd

    if (functional_test != null && functional_test) {
        test_cmd = "run-pytest"
    } else {
        test_cmd = "run-test"
    }

    try {

    withEnv(pipelineEnv(params: params, region: region, pipelineParams: pipelineParams)) {
    sh """#!/bin/bash
    set -xe
    env

    rm -fv ./latest

    echo "start test ......."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} ${test_cmd} ${params.test_name} --backend ${params.backend}
    else
        ./docker/env/hydra.sh ${test_cmd} ${params.test_name} --backend ${params.backend}  --logdir "`pwd`"
    fi
    echo "end test ....."
    """
    }
    }
    finally
    {
        if (functional_test != null && functional_test) {
            sh """
                RUNNER_IP=\$(cat sct_runner_ip||echo "")
                if [[ -n "\${RUNNER_IP}" ]] ; then
                    ./docker/env/hydra.sh fetch-junit-from-runner \${RUNNER_IP} --backend ${params.backend}
                fi
            """
            junit(testResults:"**/junit.xml", keepProperties:true)
        }
    }
}
