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
    export SCT_CONFIG_FILES=${test_config}
    export SCT_COLLECT_LOGS=false

    if [[ -n "${params.aws_region ? params.aws_region : ''}" ]] ; then
        export SCT_REGION_NAME=${aws_region}
    fi

    if [[ -n "${params.new_version ? params.new_version : ''}" ]] ; then
        export SCT_NEW_VERSION="${params.new_version}"
    fi

    if [[ -n "${params.k8s_scylla_operator_docker_image ? params.k8s_scylla_operator_docker_image : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE=${params.k8s_scylla_operator_docker_image}
    fi
    if [[ -n "${params.k8s_scylla_operator_helm_repo ? params.k8s_scylla_operator_helm_repo : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_HELM_REPO=${params.k8s_scylla_operator_helm_repo}
    fi
    if [[ -n "${params.k8s_scylla_operator_chart_version ? params.k8s_scylla_operator_chart_version : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION=${params.k8s_scylla_operator_chart_version}
    fi

    if [[ -n "${params.scylla_mgmt_agent_version ? params.scylla_mgmt_agent_version : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_VERSION=${params.scylla_mgmt_agent_version}
    fi

    if [[ "${params.backend ? params.backend : ''}" == *"k8s"* ]] ; then
        echo "Kubernetes backend can have empty scylla version"
    elif [[ -n "${params.scylla_ami_id ? params.scylla_ami_id : ''}" ]] ; then
        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
    elif [[ -n "${params.gce_image_db ? params.gce_image_db : ''}" ]] ; then
        export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
    elif [[ -n "${params.scylla_version ? params.scylla_version : ''}" ]] ; then
        export SCT_SCYLLA_VERSION="${params.scylla_version}"
    elif [[ -n "${params.scylla_repo ? params.scylla_repo : ''}" ]] ; then
        export SCT_SCYLLA_REPO="${params.scylla_repo}"
    else
        echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO | SCT_GCE_IMAGE_DB"
        exit 1
    fi

    if [[ -n "${params.oracle_scylla_version ? params.oracle_scylla_version : ''}" ]] ; then
        export SCT_ORACLE_SCYLLA_VERSION="${params.oracle_scylla_version}"
    fi

    if [[ -n "${params.gemini_seed ? params.genini_seed : ''}" ]] ; then
        export SCT_GEMINI_SEED="${params.gemini_seed}"
    fi

    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

    if [[ -n "${params.provision_type ? params.provision_type : ''}" ]] ; then
        export SCT_INSTANCE_PROVISION="${params.provision_type}"
    fi

    if [[ -n "${params.instance_provision_fallback_on_demand ? params.instance_provision_fallback_on_demand : ''}" ]] ; then
        export SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND="${params.instance_provision_fallback_on_demand}"
    fi
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )
    if [[ "${params.update_db_packages || false}" == "true" ]] ; then
        export SCT_UPDATE_DB_PACKAGES="${params.update_db_packages}"
    fi

    if [[ -n "${params.tag_ami_with_result ? params.tag_ami_with_result : ''}" ]] ; then
        export SCT_TAG_AMI_WITH_RESULT="${params.tag_ami_with_result}"
    fi

    if [[ -n "${params.ip_ssh_connections ? params.ip_ssh_connections : ''}" ]] ; then
        export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"
    fi

    if [[ -n "${params.scylla_mgmt_repo ? params.scylla_mgmt_repo : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_REPO="${params.scylla_mgmt_repo}"
    fi

    echo "start test ......."
    if [[ "$cloud_provider" == "aws" ]]; then
        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
        if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
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
