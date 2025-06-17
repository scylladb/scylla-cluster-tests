#!groovy

def completed_stages = [:]

def runSctTest(Map params, String region){
    def current_region = initAwsRegionParam(params.region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = params.backend.trim().toLowerCase()

    sh """#!/bin/bash
    set -xe
    env

    rm -fv ./latest

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_REGION_NAME=${current_region}
    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${params.gce_datacenter}
    fi
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

    if [[ ! -z "${params.oracle_scylla_version}" ]] ; then
        export SCT_ORACLE_SCYLLA_VERSION="${params.oracle_scylla_version}"
    else
        echo "need to set SCT_ORACLE_SCYLLA_VERSION"
        exit 1
    fi

    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
    export SCT_INSTANCE_PROVISION="${params.provision_type}"
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

    export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"

    if [[ ! -z "${params.scylla_mgmt_address}" ]] ; then
        export SCT_SCYLLA_MGMT_ADDRESS="${params.scylla_mgmt_address}"
    fi

    if [[ ! -z "${params.manager_version}" ]] ; then
        export SCT_MANAGER_VERSION="${params.manager_version}"
    fi

    echo "start test ......."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ ! -z "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} run-test ${params.test_name} --backend ${params.backend}
    else
        ./docker/env/hydra.sh run-test ${params.test_name} --backend ${params.backend}  --logdir "`pwd`"
    fi
    echo "end test ....."
    """
}

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter)

    pipeline {
        agent {
            label {
                label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_TEST_ID = UUID.randomUUID().toString()
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('oracle_scylla_version', '')}",
                   description: "",
                   name: "oracle_scylla_version")
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'on_demand|spot_fleet|spot',
                   name: 'provision_type')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            string(defaultValue: '',
                   description: 'If empty - the default manager version will be taken',
                   name: 'scylla_mgmt_address')

            string(defaultValue: '',
                   description: 'master_latest|3.1|3.0',
                   name: 'manager_version')

            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')

            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout(pipelineParams.timeout)
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Checkout') {
                steps {
                    script {
                        completed_stages = [:]
                    }
                    dir('scylla-cluster-tests') {
                        checkout scm
                        checkoutQaInternal(params)
                    }
                }
            }
            stage('Create SCT Runner') {
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                createSctRunner(params, pipelineParams.timeout.time , builder.region)
                            }
                        }
                    }
                }
            }
            stage('Run SCT Test') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runSctTest(params, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage("Collect log data") {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runCollectLogs(params, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage('Clean resources') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runCleanupResource(params, builder.region)
                                    completed_stages['clean_resources'] = true
                                }
                            }
                        }
                    }
                }
            }
            stage("Send email with result") {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runSendEmail(params, currentBuild)
                                    completed_stages['send_email'] = true
                                }
                            }
                        }
                    }
                }
            }
            stage('Clean SCT Runners') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    cleanSctRunners(params, currentBuild)
                                }
                            }
                        }
                    }
                }
            }
        }
        post {
            always {
                script {
                    def collect_logs = completed_stages['collect_logs']
                    def clean_resources = completed_stages['clean_resources']
                    def send_email = completed_stages['send_email']
                    sh """
                        echo "$collect_logs"
                        echo "$clean_resources"
                        echo "$send_email"
                    """
                    if (!completed_stages['clean_resources']) {
                        catchError {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                    runCleanupResource(params, builder.region)
                                    }
                                }
                            }
                        }
                    }
                    if (!completed_stages['send_email']) {
                        catchError {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                    runSendEmail(params, currentBuild)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
