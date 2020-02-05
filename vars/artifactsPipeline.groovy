#! groovy

def call(Map pipelineParams) {
    pipeline {
        agent {
            label {
                label getJenkinsLabels(params.backend, 'eu-west-1')
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
                   description: 'aws|gce',
                   name: 'backend')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against',
                   name: 'scylla_repo')
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
            disableConcurrentBuilds()
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
                                    export SCT_SCYLLA_REPO="${params.scylla_repo}"
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
                                    ./docker/env/hydra.sh clean-resources --backend ${params.backend} --logdir /sct
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
