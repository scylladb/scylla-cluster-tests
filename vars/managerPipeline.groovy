#!groovy

def call(Map pipelineParams) {

    pipeline {
        agent {
            label {
                label getJenkinsLabels(params.backend, params.aws_region)
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('aws_region', 'eu-west-1')}",
               description: 'us-east-1|eu-west-1',
               name: 'aws_region')


            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_duration',
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

            string(defaultValue: "${pipelineParams.get('tag_ami_with_result', 'false')}",
                   description: 'true|false',
                   name: 'tag_ami_with_result')

            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')
            string(defaultValue: '', description: 'If empty - the default manager version will be taken', name: 'scylla_mgmt_repo')

            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_repo', '')}",
                   description: 'manager agent repo',
                   name: 'scylla_mgmt_agent_repo')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_pkg', '')}",
                   description: 'Url to the scylla manager packages',
                   name: 'scylla_mgmt_pkg')
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
                  dir('scylla-cluster-tests') {
                      checkout scm

                      dir("scylla-qa-internal") {
                        git(url: 'git@github.com:scylladb/scylla-qa-internal.git',
                            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                            branch: 'master')
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

                                    // handle params which can be a json list
                                    def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
                                    def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)

                                    sh """
                                    #!/bin/bash
                                    set -xe
                                    env

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
                                    export SCT_INSTANCE_PROVISION="${pipelineParams.params.get('provision_type', '')}"
                                    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                    export SCT_TAG_AMI_WITH_RESULT="${params.tag_ami_with_result}"
                                    export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"

                                    if [[ ! -z "${params.scylla_mgmt_repo}" ]] ; then
                                        export SCT_SCYLLA_MGMT_REPO="${params.scylla_mgmt_repo}"
                                    fi

                                    if [[ ! -z "${params.scylla_mgmt_agent_repo}" ]] ; then
                                        export SCT_SCYLLA_MGMT_AGENT_REPO="${params.scylla_mgmt_agent_repo}"
                                    fi

                                    if [[ ! -z "${params.scylla_mgmt_pkg}" ]] ; then
                                        export SCT_SCYLLA_MGMT_PKG="${params.scylla_mgmt_pkg}"
                                    fi

                                    echo "start test ......."
                                    ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend ${params.backend}  --logdir /sct
                                    echo "end test ....."
                                    """
                                }
                            }
                        }
                    }
                }
            }
            stage('Collect log data') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
                                    def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)

                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    export SCT_CLUSTER_BACKEND="${params.backend}"
                                    export SCT_REGION_NAME=${aws_region}
                                    export SCT_CONFIG_FILES=${test_config}

                                    echo "start collect logs ..."
                                    ./docker/env/hydra.sh collect-logs --logdir /sct
                                    echo "end collect logs"
                                    """
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
                                    def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
                                    def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)

                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    export SCT_CONFIG_FILES=${test_config}
                                    export SCT_CLUSTER_BACKEND="${params.backend}"
                                    export SCT_REGION_NAME=${aws_region}
                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

                                    echo "start clean resources ..."
                                    ./docker/env/hydra.sh clean-resources --logdir /sct
                                    echo "end clean resources"
                                    """
                                }
                            }
                        }
                    }
                }
            }
            stage('Send email with result') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)

                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    echo "Start send email ..."
                                    ./docker/env/hydra.sh send-email --logdir /sct --email-recipients "${email_recipients}"
                                    echo "Email sent"
                                    """
                                }
                            }
                        }
                    }
                }
            }
        }
        post {
            always {
                archiveArtifacts artifacts: 'scylla-cluster-tests/latest/**'
            }
        }
    }

}
