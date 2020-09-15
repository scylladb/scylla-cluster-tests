#!groovy

def call(Map params, String region){
    // handle params which can be a json list
    def aws_region = initAwsRegionParam(params.aws_region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = params.backend.trim().toLowerCase()

    sh """
    #!/bin/bash
    set -xe
    env

    rm -fv ./latest

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_REGION_NAME=${aws_region}
    export SCT_CONFIG_FILES=${test_config}
    export SCT_COLLECT_LOGS=false

    if [[ ! -z "${params.scylla_ami_id}" ]] ; then
        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
    elif [[ ! -z "${params.scylla_version}" ]] ; then
        export SCT_SCYLLA_VERSION="${params.scylla_version}"
    elif [[ ! -z "${params.scylla_repo}" ]] ; then
        export SCT_SCYLLA_REPO="${params.scylla_repo}"
    else
        echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO"
        exit 1
    fi

    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
    export SCT_INSTANCE_PROVISION="${params.get('provision_type', '')}"
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )
    if [[ ! -z "${params.update_db_packages}" ]]; then
        export SCT_UPDATE_DB_PACKAGES="${params.update_db_packages}"
    fi

    export SCT_TAG_AMI_WITH_RESULT="${params.tag_ami_with_result}"
    export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"

    if [[ ! -z "${params.scylla_mgmt_repo}" ]] ; then
        export SCT_SCYLLA_MGMT_REPO="${params.scylla_mgmt_repo}"
    fi

    echo "start test ......."
    if [[ "$cloud_provider" == "aws" ]]; then
        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ ! -z "\${SCT_RUNNER_IP}" ]] ; then
            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} run-test ${params.test_name} --backend ${params.backend}
        else
            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
            exit 1
        fi
    else
        ./docker/env/hydra.sh run-test ${params.test_name} --backend ${params.backend}  --logdir "`pwd`"
    fi
    echo "end test ....."
    """
}
