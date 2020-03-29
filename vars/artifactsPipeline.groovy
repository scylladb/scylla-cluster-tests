#! groovy

def call(Map pipelineParams) {
    pipeline {
        agent {
            label {
                label getJenkinsLabels(params.backend, params.get('region_name', 'eu-west-1'))
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
                   description: 'aws|gce|docker',
                   name: 'backend')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against (for .rpm/.deb tests, should be blank otherwise)',
                   name: 'scylla_repo')
            string(defaultValue: '',
                   description: 'a Scylla AMI to run against (for AMI test, should be blank otherwise)',
                   name: 'scylla_ami_id')
            string(defaultValue: "${pipelineParams.get('region_name', '')}",
                   description: 'AWS region with Scylla AMI (for AMI test, ignored otherwise)',
                   name: 'region_name')
            string(defaultValue: '',
                   description: "a Scylla docker image to run against (for docker backend.) Should be `scylladb/scylla' for official images",
                   name: 'scylla_docker_image')
            string(defaultValue: '',
                   description: 'a Scylla version to run against (mostly for docker backend)',
                   name: 'scylla_version')
             string(defaultValue: "${pipelineParams.get('instance_type', '')}",
                   description: 'a cloud instance type (leave blank for test case defaults)',
                   name: 'instance_type')
           string(defaultValue: "${pipelineParams.get('test_config', 'test-cases/artifacts/centos7.yaml')}",
                   description: 'a config file for the artifacts test',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')
            string(defaultValue: "${pipelineParams.get('instance_provision', 'spot_low_price')}",
                   description: 'on_demand|spot_low_price|spot',
                   name: 'instance_provision')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
        }
        options {
            timestamps()
            timeout(pipelineParams.timeout)
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Checkout') {
               steps {
                  dir('scylla-cluster-tests') {
                      checkout scm
                  }
               }
            }
            stage('Run SCT Test') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    sh """
                                    #!/bin/bash
                                    set -xe
                                    env

                                    export SCT_COLLECT_LOGS=false
                                    export SCT_CONFIG_FILES=${params.test_config}

                                    if [[ ! -z "${params.scylla_ami_id}" ]]; then
                                        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
                                        export SCT_REGION_NAME="${params.region_name}"
                                    elif [[ ! -z "${params.scylla_version}" ]]; then
                                        export SCT_SCYLLA_VERSION="${params.scylla_version}"
                                    elif [[ ! -z "${params.scylla_repo}" ]]; then
                                        export SCT_SCYLLA_REPO="${params.scylla_repo}"
                                    else
                                        echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO"
                                        exit 1
                                    fi

                                    if [[ ! -z "${params.scylla_docker_image}" ]]; then
                                        export SCT_DOCKER_IMAGE="${params.scylla_docker_image}"
                                        if [[ -z "${params.scylla_version}" ]]; then
                                            echo "need to provide SCT_SCYLLA_VERSION for Docker backend"
                                            exit 1
                                        fi
                                    fi

                                    if [[ ! -z "${params.instance_type}" ]]; then
                                        case "${params.backend}" in
                                            "aws")
                                                export SCT_INSTANCE_TYPE_DB="${params.instance_type}"
                                                ;;
                                            "gce")
                                                export SCT_GCE_INSTANCE_TYPE_DB="${params.instance_type}"
                                                ;;
                                        esac
                                    fi

                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                    export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"
                                    export SCT_INSTANCE_PROVISION="${params.instance_provision}"

                                    echo "start test ......."
                                    ./docker/env/hydra.sh run-test artifacts_test --backend ${params.backend} --logdir /sct
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
                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    export SCT_CONFIG_FILES=${params.test_config}

                                    echo "start collect logs ..."
                                    ./docker/env/hydra.sh collect-logs --backend ${params.backend} --logdir /sct
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
                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    export SCT_CONFIG_FILES=${params.test_config}
                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"

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
