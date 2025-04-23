#! groovy


def call(Map params, String region){
    def current_region = initAwsRegionParam(params.region, region)
    def current_gce_datacenter = ""
    if (params.gce_datacenter) {
        current_gce_datacenter = groovy.json.JsonOutput.toJson(params.gce_datacenter)
    }
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    sh """#!/bin/bash

    set -xe
    env

    echo "${params.test_config}"
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_REGION_NAME=${current_region}
    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${current_gce_datacenter}
    fi
    if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
        export SCT_AZURE_REGION_NAME=${params.azure_region_name}
    fi
    export SCT_CONFIG_FILES=${test_config}

    echo "start collect logs ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} collect-logs --backend "${params.backend}"
    else
        ./docker/env/hydra.sh collect-logs --backend "${params.backend}" --logdir "`pwd`"
    fi
    echo "end collect logs"
    """

    collectTestCoredumps()
}
