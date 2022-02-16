#!groovy

def call(Map params, String region, functional_test = false, Map pipelineParams = [:]){
    // handle params which can be a json list
    def current_region = initAwsRegionParam(params.region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    def test_cmd

    if (functional_test != null && functional_test) {
        test_cmd = "run-pytest"
    } else {
        test_cmd = "run-test"
    }

    sh """
    #!/bin/bash
    set -xe
    env

    rm -fv ./latest

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}
    export SCT_COLLECT_LOGS=false

    if [[ -n "${params.region ? params.region : ''}" ]] ; then
        export SCT_REGION_NAME=${current_region}
    fi

    if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
        export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
    fi

    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${params.gce_datacenter}
    fi

    if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
        export SCT_AZURE_REGION_NAME=${params.azure_region_name}
    fi

    if [[ -n "${params.new_version ? params.new_version : ''}" ]] ; then
        export SCT_NEW_VERSION="${params.new_version}"
    fi

    if [[ -n "${params.k8s_scylla_operator_docker_image ? params.k8s_scylla_operator_docker_image : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE=${params.k8s_scylla_operator_docker_image}
    fi
    if [[ -n "${params.k8s_scylla_operator_upgrade_docker_image ? params.k8s_scylla_operator_upgrade_docker_image : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE=${params.k8s_scylla_operator_upgrade_docker_image}
    fi
    if [[ -n "${params.k8s_scylla_operator_helm_repo ? params.k8s_scylla_operator_helm_repo : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_HELM_REPO=${params.k8s_scylla_operator_helm_repo}
    fi
    if [[ -n "${params.k8s_scylla_operator_upgrade_helm_repo ? params.k8s_scylla_operator_upgrade_helm_repo : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO=${params.k8s_scylla_operator_upgrade_helm_repo}
    fi
    if [[ -n "${params.k8s_scylla_operator_chart_version ? params.k8s_scylla_operator_chart_version : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION=${params.k8s_scylla_operator_chart_version}
    fi
    if [[ -n "${params.k8s_scylla_operator_upgrade_chart_version ? params.k8s_scylla_operator_upgrade_chart_version : ''}" ]] ; then
        export SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION=${params.k8s_scylla_operator_upgrade_chart_version}
    fi
    if [[ -n "${pipelineParams.k8s_enable_performance_tuning ? pipelineParams.k8s_enable_performance_tuning : ''}" ]] ; then
        export SCT_K8S_ENABLE_PERFORMANCE_TUNING=${pipelineParams.k8s_enable_performance_tuning}
    fi

    if [[ -n "${params.scylla_mgmt_agent_version ? params.scylla_mgmt_agent_version : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_VERSION=${params.scylla_mgmt_agent_version}
    fi

    if [[ -n "${params.scylla_mgmt_agent_address ? params.scylla_mgmt_agent_address : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_ADDRESS=${params.scylla_mgmt_agent_address}
    fi

    if [[ -n "${params.scylla_ami_id ? params.scylla_ami_id : ''}" ]] ; then
        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
    elif [[ -n "${params.gce_image_db ? params.gce_image_db : ''}" ]] ; then
        export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
    elif [[ -n "${params.azure_image_db ? params.azure_image_db : ''}" ]] ; then
        export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"
    elif [[ -n "${params.scylla_version ? params.scylla_version : ''}" ]] ; then
        export SCT_SCYLLA_VERSION="${params.scylla_version}"
    elif [[ -n "${params.scylla_repo ? params.scylla_repo : ''}" ]] ; then
        export SCT_SCYLLA_REPO="${params.scylla_repo}"
    elif [[ "${params.backend ? params.backend : ''}" == *"k8s"* ]] ; then
        echo "Kubernetes backend can have empty scylla version. It will be taken from defaults of the scylla helm chart"
    else
        echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO | SCT_GCE_IMAGE_DB | SCT_AZURE_IMAGE_DB"
        exit 1
    fi

    if [[ -n "${params.oracle_scylla_version ? params.oracle_scylla_version : ''}" ]] ; then
        export SCT_ORACLE_SCYLLA_VERSION="${params.oracle_scylla_version}"
    fi

    if [[ -n "${params.gemini_seed ? params.genini_seed : ''}" ]] ; then
        export SCT_GEMINI_SEED="${params.gemini_seed}"
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

    if [[ -n "${params.scylla_mgmt_address ? params.scylla_mgmt_address : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_ADDRESS="${params.scylla_mgmt_address}"
    fi

    if [[ -n "${params.manager_version ? params.manager_version : ''}" ]] ; then
        export SCT_MANAGER_VERSION="${params.manager_version}"
    fi

    if [[ -n "${params.pytest_addopts ? params.pytest_addopts : ''}" ]] ; then
        export PYTEST_ADDOPTS="${params.pytest_addopts}"
    fi

    echo "start test ......."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} ${test_cmd} ${params.test_name} --backend ${params.backend}
    else
        ./docker/env/hydra.sh ${test_cmd} ${params.test_name} --backend ${params.backend}  --logdir "`pwd`"
    fi
    echo "end test ....."
    """
}
