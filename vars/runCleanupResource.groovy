#!groovy

def call(Map params, String region){
    def region = initAwsRegionParam(params.region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    sh """
    #!/bin/bash

    set -xe
    env

    export SCT_CONFIG_FILES=${test_config}
    export SCT_CLUSTER_BACKEND="${params.backend}"

    if [[ -n "${params.region ? params.region : ''}" ]] ; then
        export SCT_REGION_NAME=${region}
    fi
    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${params.gce_datacenter}
    fi

    if [[ -n "${params.post_behavior_db_nodes ? params.post_behavior_db_nodes : ''}" ]] ; then
        export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
    fi
    if [[ -n "${params.post_behavior_loader_nodes ? params.post_behavior_loader_nodes : ''}" ]] ; then
        export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
    fi
    if [[ -n "${params.post_behavior_monitor_nodes ? params.post_behavior_monitor_nodes : ''}" ]] ; then
        export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
    fi
    if [[ -n "${params.post_behavior_k8s_cluster ? params.post_behavior_k8s_cluster : ''}" ]] ; then
        export SCT_POST_BEHAVIOR_K8S_CLUSTER="${params.post_behavior_k8s_cluster}"
    fi

    echo "Starting to clean resources ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} clean-resources --post-behavior --test-id \$SCT_TEST_ID
    else
        ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
    fi
    echo "Finished cleaning resources."
    """
}
