#!groovy

def call(Map params, String region){
    def aws_region = initAwsRegionParam(params.aws_region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    sh """
    #!/bin/bash

    set -xe
    env

    export SCT_CONFIG_FILES=${test_config}
    export SCT_CLUSTER_BACKEND="${params.backend}"

    if [[ -n "${params.aws_region ? params.aws_region : ''}" ]] ; then
        export SCT_REGION_NAME=${aws_region}
    fi

    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

    echo "Starting to clean resources ..."
    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" ]]; then
        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} clean-resources --post-behavior --test-id \$SCT_TEST_ID
        else
            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
            exit 1
        fi
    else
        ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
    fi
    echo "Finished cleaning resources."
    """
}
