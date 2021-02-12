#!groovy

def call(Map params, Integer test_duration, String region) {
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    println(params)
    sh """
    #!/bin/bash
    set -xe
    env

    if [[ "$cloud_provider" == "aws" ]]; then
        rm -fv sct_runner_ip
        ./docker/env/hydra.sh create-runner-instance --cloud-provider ${cloud_provider} --region ${region} --availability-zone ${params.availability_zone} --test-id \${SCT_TEST_ID} --duration ${test_duration}
    else
        echo "Currently, <$cloud_provider> not supported to. Will run on regular builder."
    fi
    """
}
