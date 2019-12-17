#!groovy

def call() {
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

            string(defaultValue: 'git@github.com:scylladb/scylla-cluster-tests.git',
                   description: 'sct git repo',
                   name: 'sct_repo')

            string(defaultValue: 'master',
                   description: 'sct git branch',
                   name: 'sct_branch')

            string(defaultValue: "longevity_test.LongevityTest.test_custom_time",
                   description: '',
                   name: 'test_name')
            string(defaultValue: "test-cases/longevity/longevity-100gb-4h.yaml",
                   description: '',
                   name: 'test_config')
            string(defaultValue: "aws",
               description: 'aws|gce',
               name: 'backend')
            string(defaultValue: "eu-west-1",
               description: 'us-east-1|eu-west-1',
               name: 'aws_region')
            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "spot_low_price",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration',
                   name: 'provision_type')

            string(defaultValue: "keep-on-failure",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "destroy",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "keep-on-failure",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            string(defaultValue: '360',
                   description: 'timeout for jenkins job in minutes',
                   name: 'timeout')

            string(defaultValue: '',
                   description: 'cloud path for RPMs, s3:// or gs://',
                   name: 'update_db_packages')

            string(defaultValue: "qa@scylladb.com",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout([time: params.timeout, unit: "MINUTES"])
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Checkout') {
               steps {
                  dir('scylla-cluster-tests') {
                      git(url: params.sct_repo,
                            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                            branch: params.sct_branch)

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
                                    def test_config = groovy.json.JsonOutput.toJson(params.test_config)

                                    sh """
                                    #!/bin/bash
                                    set -xe
                                    env

                                    export SCT_CLUSTER_BACKEND="${params.backend}"
                                    export SCT_REGION_NAME=${aws_region}
                                    export SCT_CONFIG_FILES="${test_config}"

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
                                    if [[ ! -z "${params.update_db_packages}" ]]; then
                                        export SCT_UPDATE_DB_PACKAGES="${params.update_db_packages}"
                                    fi
                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                    export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                    export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                    echo "start test ......."
                                    ./docker/env/hydra.sh run-test ${params.test_name} --backend ${params.backend}  --logdir /sct
                                    echo "end test ....."
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
                                    def email_recipients = groovy.json.JsonOutput.toJson(pipelineParams.get('email_recipients', 'qa@scylladb.com'))

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

                                    export SCT_CLUSTER_BACKEND="${params.backend}"
                                    export SCT_REGION_NAME=${aws_region}
                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

                                    echo "start clean resources ..."
                                    ./docker/env/hydra.sh clean-resources --config-file "${test_config}" --logdir /sct
                                    echo "end clean resources"
                                    """
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
