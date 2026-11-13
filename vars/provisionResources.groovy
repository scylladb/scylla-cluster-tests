#!groovy

def call(Map params, String region){
    if (params.reuse_cluster) {
        echo "Cluster reuse mode: skipping resource provisioning"
        return
    }

    withEnv(pipelineEnv(params: params, region: region)) {
    sh """#!/bin/bash

    set -xe
    env

    # Handle backend-specific provisioning logic
    if [[ "${params.backend}" == "xcloud" ]] ; then
        echo "Scylla Cloud backend selected: provisioning loader nodes only on ${params.xcloud_provider} cloud provider"
    fi

    if [[ "${params.backend}" == "xcloud" ]] || [[ "${params.backend}" == "aws" ]] || [[ "${params.backend}" == "azure" ]] || [[ "${params.backend}" == "gce" ]] || [[ "${params.backend}" == "oci" ]] ; then
        echo "Starting to resource provision ..."
        RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ -n "\${RUNNER_IP}" ]] ; then
            ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} provision-resources -b "${params.backend}" -t "${params.test_name}"
        else
            ./docker/env/hydra.sh provision-resources -b "${params.backend}" -t "${params.test_name}"
        fi
        echo "Finished resource provision"
    elif [[ "${params.backend}" == *"docker"* ]] ; then
        echo 'Tests are to be executed on Docker backend in SCT-Runner. No additional resources to be provisioned.'
    else
        echo 'Skipping because non-AWS/Azure/GCE/OCI backends are not supported'
    fi
    """
    }
}
