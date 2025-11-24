#!groovy

def call(Map params, String region, functional_test = false, Map pipelineParams = [:]){
    // handle params which can be a json list
    def current_region = initAwsRegionParam(params.region, region)
    def current_gce_datacenter = ""
    if (params.gce_datacenter) {
        current_gce_datacenter = groovy.json.JsonOutput.toJson(params.gce_datacenter)
    }
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    def perf_extra_jobs_to_compare = params.perf_extra_jobs_to_compare ? groovy.json.JsonOutput.toJson(params.perf_extra_jobs_to_compare) : ""
    def email_recipients = params.email_recipients ? groovy.json.JsonOutput.toJson(params.email_recipients) : ""

    def test_cmd

    if (functional_test != null && functional_test) {
        test_cmd = "run-pytest"
    } else {
        test_cmd = "run-test"
    }

    try {

    sh """#!/bin/bash
    set -xe
    env

    rm -fv ./latest

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}
    export SCT_COLLECT_LOGS=false

    if [[ "${params.backend}" == "xcloud" ]] ; then
        export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
        export SCT_XCLOUD_ENV="${params.xcloud_env}"
    fi

    if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
        export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
    fi

    if [[ -n "${params.stress_duration ? params.stress_duration : ''}" ]] ; then
        export SCT_STRESS_DURATION=${params.stress_duration}
    fi

    if [[ -n "${params.prepare_stress_duration ? params.prepare_stress_duration : ''}" ]] ; then
        export SCT_PREPARE_STRESS_DURATION=${params.prepare_stress_duration}
    fi

    if [[ -n "${params.region ? params.region : ''}" ]] ; then
        export SCT_REGION_NAME=${current_region}
    fi

    if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
        export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
    fi

    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${current_gce_datacenter}
    fi

    if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
        export SCT_AZURE_REGION_NAME=${params.azure_region_name}
    fi

    if [[ -n "${params.new_version ? params.new_version : ''}" ]] ; then
        export SCT_NEW_VERSION="${params.new_version}"
    fi

    if [[ -n "${params.k8s_version ? params.k8s_version : ''}" ]] ; then
        export SCT_EKS_CLUSTER_VERSION="${params.k8s_version}"
        export SCT_GKE_CLUSTER_VERSION="${params.k8s_version}"
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
    if [[ -n "${pipelineParams.k8s_log_api_calls ? pipelineParams.k8s_log_api_calls : ''}" ]] ; then
        export SCT_K8S_LOG_API_CALLS=${pipelineParams.k8s_log_api_calls}
    fi
    if [[ -n "${params.k8s_enable_tls ? params.k8s_enable_tls : ''}" ]] ; then
        export SCT_K8S_ENABLE_TLS=${params.k8s_enable_tls}
    fi
    if [[ -n "${params.k8s_enable_sni ? params.k8s_enable_sni : ''}" ]] ; then
        export SCT_K8S_ENABLE_SNI=${params.k8s_enable_sni}
    fi

    if [[ -n "${params.docker_image ? params.docker_image : ''}" ]] ; then
        export SCT_DOCKER_IMAGE=${params.docker_image}
    fi

    if [[ -n "${params.scylla_mgmt_agent_version ? params.scylla_mgmt_agent_version : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_VERSION=${params.scylla_mgmt_agent_version}
    fi

    if [[ -n "${params.scylla_mgmt_agent_address ? params.scylla_mgmt_agent_address : ''}" ]] ; then
        export SCT_SCYLLA_MGMT_AGENT_ADDRESS=${params.scylla_mgmt_agent_address}
    fi

    if [[ -n "${params.scylla_ami_id ? params.scylla_ami_id : ''}" ]] ; then
        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
    fi
    if [[ -n "${params.gce_image_db ? params.gce_image_db : ''}" ]] ; then
        export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
    fi
    if [[ -n "${params.azure_image_db ? params.azure_image_db : ''}" ]] ; then
        export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"
    fi
    if [[ -n "${params.scylla_version ? params.scylla_version : ''}" ]] ; then
        export SCT_SCYLLA_VERSION="${params.scylla_version}"
    fi
    if [[ -n "${params.scylla_repo ? params.scylla_repo : ''}" ]] ; then
        export SCT_SCYLLA_REPO="${params.scylla_repo}"
    fi
    if [[ -n "${params.new_scylla_repo ? params.new_scylla_repo : ''}" ]] ; then
        export SCT_NEW_SCYLLA_REPO="${params.new_scylla_repo}"
    fi

    if [[ -n "${params.oracle_scylla_version ? params.oracle_scylla_version : ''}" ]] ; then
        export SCT_ORACLE_SCYLLA_VERSION="${params.oracle_scylla_version}"
    fi

    if [[ -n "${params.gemini_seed ? params.gemini_seed : ''}" ]] ; then
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

    if [[ -n "${params.use_preinstalled_scylla ? params.use_preinstalled_scylla : ''}" ]] ; then
        export SCT_USE_PREINSTALLED_SCYLLA="${params.use_preinstalled_scylla}"
    fi
    if [[ -n "${params.disable_raft ? params.disable_raft : ''}" ]] ; then
        export SCT_DISABLE_RAFT=${params.disable_raft}
    fi
    if [[ -n "${params.linux_distro ? params.linux_distro : ''}" ]] ; then
        export SCT_SCYLLA_LINUX_DISTRO=${params.linux_distro}
    fi
    if [[ -n "${params.internode_compression ? params.internode_compression : ''}" ]] ; then
        export SCT_INTERNODE_COMPRESSION=${params.internode_compression}
    fi

    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )
    if [[ "${params.update_db_packages || false}" == "true" ]] ; then
        export SCT_UPDATE_DB_PACKAGES="${params.update_db_packages}"
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

    if [[ -n "${perf_extra_jobs_to_compare}" ]] ; then
        export SCT_PERF_EXTRA_JOBS_TO_COMPARE="${perf_extra_jobs_to_compare}"
    fi

    if [[ -n "${email_recipients}" ]] ; then
        export SCT_EMAIL_RECIPIENTS="${email_recipients}"
    fi

    if [[ -n "${params.stop_on_hw_perf_failure ? params.stop_on_hw_perf_failure : ''}" ]] ; then
        if [[ "${params.stop_on_hw_perf_failure}" == "true" ]] ; then
            export SCT_STOP_ON_HW_PERF_FAILURE="true"
        fi
    fi

    if [[ -n "${params.test_email_title ? params.test_email_title : ''}" ]] ; then
        export SCT_EMAIL_SUBJECT_POSTFIX="${params.test_email_title}"
    fi

    if [[ -n "${pipelineParams.k8s_deploy_monitoring ? pipelineParams.k8s_deploy_monitoring : ''}" ]] ; then
        export SCT_K8S_DEPLOY_MONITORING="${pipelineParams.k8s_deploy_monitoring}"
    fi

    if [[ -n "${pipelineParams.k8s_scylla_utils_docker_image ? pipelineParams.k8s_scylla_utils_docker_image : ''}" ]] ; then
        export SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE="${pipelineParams.k8s_scylla_utils_docker_image}"
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
    finally
    {
        if (functional_test != null && functional_test) {
            sh """
                RUNNER_IP=\$(cat sct_runner_ip||echo "")
                if [[ -n "\${RUNNER_IP}" ]] ; then
                    ./docker/env/hydra.sh fetch-junit-from-runner \${RUNNER_IP} --backend ${params.backend}
                fi
            """
            junit(testResults:"**/junit.xml", keepProperties:true)
        }
    }
}
