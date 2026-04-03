#! groovy

def call(Integer test_duration){
    if (test_duration <= 480) {
        sh '''#!/bin/bash

            set -xe
            env

            echo "Starting generating parallel timelines report ..."
            RUNNER_IP=$(cat sct_runner_ip||echo "")
            if [[ -n "${RUNNER_IP}" ]] ; then
                ./docker/env/hydra.sh --execute-on-runner ${RUNNER_IP} generate-pt-report
            else
                ./docker/env/hydra.sh generate-pt-report --logdir "`pwd`"
            fi
            echo "Parallel timelines report has been generated"
        '''
    } else {
        echo 'Skipping this stage for tests that are longer than 4 hours'
    }
}
