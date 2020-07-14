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
            SCT_TEST_ID = UUID.randomUUID().toString()
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
               description: 'us-east-1|eu-west-1|eu-west-2',
               name: 'aws_region')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')
            string(defaultValue: '', description: '', name: 'loader_ami_id')
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
            buildDiscarder(logRotator(numToKeepStr: '5'))
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
            stage('Create SCT Runner') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
                                    def cloud_provider = params.backend.trim().toLowerCase()
                                    sh """
                                    if [[ "$cloud_provider" == "aws" ]]; then
                                        ./docker/env/hydra.sh create-runner-instance --cloud-provider ${cloud_provider} --region ${aws_region} --availability-zone ${params.availability_zone} --test-id \${SCT_TEST_ID} --duration ${params.timeout}
                                    else
                                        echo "Currently, <$cloud_provider> not supported to. Will run on regular builder."
                                    fi
                                    """
                                }
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

                                    // handle params which can be a json list
                                    def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
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

                                    if [[ ! -z "${params.loader_ami_id}" ]] ; then
                                        export SCT_AMI_ID_LOADER="${params.loader_ami_id}"
                                    fi

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
                                    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
                                    def cloud_provider = params.backend.trim().toLowerCase()

                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    export SCT_CLUSTER_BACKEND="${params.backend}"
                                    export SCT_REGION_NAME=${aws_region}
                                    export SCT_CONFIG_FILES=${test_config}

                                    echo "start collect logs ..."
                                    if [[ "$cloud_provider" == "aws" ]]; then
                                        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                        if [[ ! -z "\${SCT_RUNNER_IP}" ]] ; then
                                            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} collect-logs
                                        else
                                            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
                                            exit 1
                                        fi
                                    else
                                        ./docker/env/hydra.sh collect-logs --logdir "`pwd`"
                                    fi
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
                                    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
                                    def cloud_provider = params.backend.trim().toLowerCase()

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

                                    echo "Starting to clean resources ..."
                                    if [[ "$cloud_provider" == "aws" ]]; then
                                        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                        if [[ ! -z "\${SCT_RUNNER_IP}" ]] ; then
                                            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} clean-resources --test-id \$SCT_TEST_ID
                                        else
                                            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
                                            exit 1
                                        fi
                                    else
                                        ./docker/env/hydra.sh clean-resources --logdir "`pwd`"
                                    fi
                                    ./docker/env/hydra.sh clean-runner-instances
                                    echo "Finished cleaning resources."
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
                                    def cloud_provider = params.backend.trim().toLowerCase()
                                    def start_time = currentBuild.startTimeInMillis.intdiv(1000)
                                    def test_status = currentBuild.currentResult
                                    sh """
                                    #!/bin/bash

                                    set -xe
                                    env

                                    echo "Sending email..."
                                    if [[ "$cloud_provider" == "aws" ]]; then
                                        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                        if [[ ! -z "\${SCT_RUNNER_IP}" ]] ; then
                                            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} send-email --email-recipients "${email_recipients}"
                                        else
                                            echo "SCT runner IP file is empty. Probably SCT Runner was not created."
                                            ./docker/env/hydra.sh send-email ${test_status} ${start_time} --logdir "`pwd`" --email-recipients "${email_recipients}"
                                            exit 1
                                        fi
                                    else
                                        ./docker/env/hydra.sh send-email ${test_status} ${start_time} --logdir "`pwd`" --email-recipients "${email_recipients}"
                                    fi

                                    echo "Email sent."
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
