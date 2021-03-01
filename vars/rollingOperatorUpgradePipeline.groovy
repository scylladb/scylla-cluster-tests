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
            SCT_TEST_ID = UUID.randomUUID().toString()
        }
        parameters {
            choice(choices: ["${pipelineParams.get('backend', 'k8s-gke')}", 'k8s-gke', 'k8s-gce-minikube'],
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('base_versions', '')}",
                   name: 'base_versions')
            string(defaultValue: "${pipelineParams.get('new_version', '')}",
                   name: 'new_version')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_version', '')}",
                   name: 'scylla_mgmt_agent_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_docker_image', '')}",
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_helm_repo', '')}",
                   name: 'k8s_scylla_operator_helm_repo')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_chart_version', '')}",
                   name: 'k8s_scylla_operator_chart_version')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration',
                   name: 'provision_type')
            choice(choices: ["${pipelineParams.get('post_behavior_db_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_db_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_loader_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_monitor_nodes')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com,scylla-operator@scylladb.com')}",
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
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Get test duration') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        timeout(time: 1, unit: 'MINUTES') {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                        checkout scm
                                        (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]
                        def base_versions_list = readJSON text: params.base_versions
                        for (base_version in base_versions_list) {
                            run_params =  readJSON text: groovy.json.JsonOutput.toJson(params)
                            run_params.scylla_version = base_version
                            tasks["Scylla Operator upgrade from ${base_version} to ${new_version}"] = {

                                stage("${base_version} -> ${new_version} - Running test") {
                                    catchError(stageResult: 'FAILURE') {
                                        script {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                        checkout scm
                                                        runSctTest(run_params, builder.region)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                stage("${base_version} -> ${new_version} - Collect logs") {
                                    catchError(stageResult: 'FAILURE') {
                                        script {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                        runCollectLogs(run_params, builder.region)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                stage("${base_version} -> ${new_version} - Clean resources") {
                                    catchError(stageResult: 'FAILURE') {
                                        script {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                                        runCleanupResource(run_params, builder.region)
                                                    }
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
