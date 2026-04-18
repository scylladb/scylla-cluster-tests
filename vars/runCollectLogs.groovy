#! groovy


def call(Map params, String region){
    def current_region = initAwsRegionParam(params.region, region)
    def current_gce_datacenter = ""
    if (params.gce_datacenter) {
        current_gce_datacenter = groovy.json.JsonOutput.toJson(params.gce_datacenter)
    }
    def current_oci_region = ""
    if (params.oci_region_name) {
        current_oci_region = initAwsRegionParam(params.oci_region_name, region)
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
    if [[ -n "${params.oci_region_name ? params.oci_region_name : ''}" ]] ; then
        export SCT_OCI_REGION_NAME=${current_oci_region}
    fi
    if [[ "${params.backend}" == "xcloud" ]] ; then
        export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
        export SCT_XCLOUD_ENV="${params.xcloud_env}"
    fi

    export SCT_CONFIG_FILES=${test_config}

    if [[ -n "${params.reuse_cluster ?: ''}" ]] ; then
        export SCT_REUSE_CLUSTER="${params.reuse_cluster}"
    fi

    echo "start collect logs ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} collect-logs --backend "${params.backend}"
    else
        ./docker/env/hydra.sh collect-logs --backend "${params.backend}" --logdir "`pwd`"
    fi
    echo "end collect logs"
    """

    collectBuilderLogs(params)
    collectTestCoredumps()
}
