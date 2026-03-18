#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild){
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    if ( params.backend.equals("xcloud") ) {
        cloud_provider = params.xcloud_provider
    }

    def test_status = currentBuild.currentResult

    def shouldKeepRunner = ['db_nodes', 'loader_nodes', 'monitor_nodes'].every {
        params."post_behavior_${it}" == 'keep'
    }

    sh """#!/bin/bash

    set -xe
    env

    echo "Test status on runCleanupResource is: " + "$test_status"

    echo "Starting to clean runner instances"
    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" || "$cloud_provider" == "azure" || "$cloud_provider" == "oci" ]]; then
        export RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ "${shouldKeepRunner}" == "true" ]]; then
            echo "All post_behavior_* settings are 'keep'. Preserving SCT runner for cluster reuse."
            # keep=120 (5 days from launch) acts as a safety ceiling;
            # the existing cleanup logic terminates runners once elapsed hours exceed the keep value
            ./docker/env/hydra.sh set-runner-tags \${RUNNER_IP} --backend "$cloud_provider" \
                --tags keep 120 --tags keep_action terminate
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
