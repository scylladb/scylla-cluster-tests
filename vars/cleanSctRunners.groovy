#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild){
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    def test_status = currentBuild.currentResult

    sh """
    #!/bin/bash

    set -xe
    env

    echo "Test status on runCleanupResource is: " + "$test_status"

    echo "Starting to clean runner instances"
    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" || "$cloud_provider" == "azure" ]]; then
        export RUNNER_IP=\$(cat sct_runner_ip||echo "")
        ./docker/env/hydra.sh clean-runner-instances --test-status "$test_status" --runner-ip \${RUNNER_IP}

    else
        echo "Not running on AWS, GCP nor Azure. Skipping cleaning runner instances."
    fi

    echo "Finished cleaning runner instances."
    """
}
