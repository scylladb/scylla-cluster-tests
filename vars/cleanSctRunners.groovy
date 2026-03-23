#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild){
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    if ( params.backend.equals("xcloud") ) {
        cloud_provider = params.xcloud_provider
    }

    def test_status = currentBuild.currentResult

    def shouldKeepRunner = (
        params.post_behavior_db_nodes == 'keep' &&
        params.post_behavior_loader_nodes == 'keep' &&
        params.post_behavior_monitor_nodes == 'keep'
    )

    sh """#!/bin/bash

    set -xe
    env

    echo "Test status on runCleanupResource is: " + "$test_status"

    echo "Starting to clean runner instances"
    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" || "$cloud_provider" == "azure" || "$cloud_provider" == "oci" ]]; then
        export RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ "${shouldKeepRunner}" == "true" ]] ; then
            echo "All post_behavior_* settings are 'keep'. Preserving SCT runner with keep=alive tag."
            ./docker/env/hydra.sh set-runner-tags \${RUNNER_IP} \
                -t keep alive -t keep_action none
        else
            ./docker/env/hydra.sh clean-runner-instances \
                --test-status "$test_status" --runner-ip \${RUNNER_IP} --backend "$cloud_provider"
        fi

    else
        echo "Not running on AWS, GCP nor Azure. Skipping cleaning runner instances."
    fi

    echo "Finished cleaning runner instances."
    """
}
