#!groovy

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(pipelineParams.backend, pipelineParams.aws_region)
    pipeline {
        agent {
            label {
                label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            choice(choices: ["${pipelineParams.get('backend', 'k8s-gce-minikube')}", 'k8s-gke'],
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('base_versions', '')}",
                   name: 'base_versions')
            string(defaultValue: "${pipelineParams.get('new_version', '')}",
                   name: 'new_version')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_version', '')}",
                   name: 'scylla_mgmt_agent_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_docker_image', '')}",
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration',
                   name: 'provision_type')
            choice(choices: ["${pipelineParams.get('post_behavior_db_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_db_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_loader_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_monitor_nodes')
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
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]
                        def base_version2 = readJSON text: params.base_versions
                        for (version in base_version2) {
                            def base_version = version
                            tasks["Scylla Operator upgrade from ${base_version} to ${new_version}"] = {
                                withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                         "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",]) {
                                    stage("${base_version} -> ${new_version} - Running test") {
                                        catchError(stageResult: 'FAILURE') {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    checkout scm
                                                    def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                    sh """
                                                    #!/bin/bash
                                                    set -xe
                                                    env

                                                    rm -fv ./latest

                                                    export SCT_CLUSTER_BACKEND=gce

                                                    export SCT_CONFIG_FILES=${test_config}
                                                    export SCT_SCYLLA_VERSION=${base_version}

                                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                                    export SCT_INSTANCE_PROVISION="${params.provision_type}"

                                                    echo "start test ......."
                                                    ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend ${params.backend}  --logdir "`pwd`"
                                                    echo "end test ....."
                                                    """
                                                }
                                            }
                                        }
                                    }
                                    stage("${base_version} -> ${new_version} - Collect logs") {
                                        catchError(stageResult: 'FAILURE') {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                    sh """
                                                    #!/bin/bash

                                                    set -xe
                                                    env

                                                    export SCT_CLUSTER_BACKEND=gce
                                                    export SCT_CONFIG_FILES=${test_config}

                                                    echo "start collect logs ..."
                                                    ./docker/env/hydra.sh collect-logs --logdir "`pwd`" --backend gce
                                                    echo "end collect logs"
                                                    """
                                                }
                                            }
                                        }
                                    }
                                    stage("${base_version} -> ${new_version} - Clean resources") {
                                        catchError(stageResult: 'FAILURE') {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    sh """
                                                    #!/bin/bash

                                                    set -xe
                                                    env

                                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                    export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                    export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

                                                    echo "start clean resources ..."
                                                    ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
                                                    echo "end clean resources"
                                                    """
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        parallel tasks
                    }
                }
            }
        }
    }
}
