#!groovy

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(pipelineParams.backend, pipelineParams.region)
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
            choice(choices: ["${pipelineParams.get('backend', 'k8s-gke')}", 'k8s-eks', 'k8s-gke'],
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('region', '')}",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('availability_zone', 'b')}",
               description: 'Availability zone',
               name: 'availability_zone')
            string(defaultValue: "${pipelineParams.get('k8s_version', '')}",
                   description: 'K8S version to be used. Suitable for EKS and GKE, but not local K8S (KinD). '
                   + 'In case of K8S platform upgrade it will be base one, target one will be automatically incremented. Example: "1.28"',
                   name: 'k8s_version')
            string(defaultValue: "${pipelineParams.get('base_versions', '')}",
                   name: 'base_versions')
            string(defaultValue: "${pipelineParams.get('new_version', '')}",
                   name: 'new_version')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_version', '')}",
                   name: 'scylla_mgmt_agent_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_helm_repo', '')}",
                   description: 'Example: https://storage.googleapis.com/scylla-operator-charts/latest',
                   name: 'k8s_scylla_operator_helm_repo')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_chart_version', '')}",
                   description: 'Example: v1.4.0-alpha.0-45-g5adf54a',
                   name: 'k8s_scylla_operator_chart_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_docker_image', '')}",
                   description: 'Example: scylladb/scylla-operator:latest or scylladb/scylla-operator:1.3.0',
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_upgrade_helm_repo', '')}",
                   description: 'Example: https://storage.googleapis.com/scylla-operator-charts/latest',
                   name: 'k8s_scylla_operator_upgrade_helm_repo')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_upgrade_chart_version', '')}",
                   description: 'Example: v1.4.0-alpha.0-34-gf13771d',
                   name: 'k8s_scylla_operator_upgrade_chart_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_upgrade_docker_image', '')}",
                   description: 'Example: scylladb/scylla-operator:latest or scylladb/scylla-operator:1.3.0',
                   name: 'k8s_scylla_operator_upgrade_docker_image')
            string(defaultValue: "${pipelineParams.get('k8s_enable_tls', '')}",
                   description: 'if true, enable operator tls feature',
                   name: 'k8s_enable_tls')
            string(defaultValue: "${pipelineParams.get('k8s_enable_sni', '')}",
                   description: 'if true, install haproxy ingress controller and use it',
                   name: 'k8s_enable_sni')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            choice(choices: ["${pipelineParams.get('post_behavior_db_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_db_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_loader_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_monitor_nodes')
            choice(choices: ["${pipelineParams.get('post_behavior_k8s_cluster', 'destroy')}", 'keep', 'keep-on-failure'],
                   name: 'post_behavior_k8s_cluster')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com,scylla-operator@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                 description: (
                     'Extra environment variables to be set in the test environment, uses the java Properties File Format.\n' +
                     'Example:\n' +
                     '\tSCT_STRESS_IMAGE.cassandra-stress=scylladb/cassandra-stress:3.12.1\n' +
                     '\tSCT_USE_MGMT=false'
                     ),
                 name: 'extra_environment_variables')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage("Preparation") {
                // NOTE: this stage is a workaround for the following Jenkins bug:
                // https://issues.jenkins-ci.org/browse/JENKINS-41929
                when { expression { env.BUILD_NUMBER == '1' } }
                steps {
                    script {
                        if (currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause') != null) {
                            currentBuild.description = ('Aborted build#1 not having parameters loaded. \n'
                              + 'Build#2 is ready to run')
                            currentBuild.result = 'ABORTED'

                            error('Abort build#1 which only loads params')
                        }
                    }
                }
            }
            stage('Get test duration') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        timeout(time: 10, unit: 'MINUTES') {
                            script {
                                loadEnvFromString(params.extra_environment_variables)
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                        checkout scm
                                        dockerLogin(params)
                                        (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('Create Argus Test Run') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        createArgusTestRun(params)
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('Create SCT Runner') {
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                timeout(time: 5, unit: 'MINUTES') {
                                    createSctRunner(params, runnerTimeout , builder.region)
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

                            def phase = "${base_version} -> ${new_version}"
                            if (run_params.test_name.contains('platform_upgrade')) {
                                phase = "platform-upgrade"
                            }
                            tasks["Scylla Operator upgrade - ${phase}"] = {

                                stage("${phase} - Running test") {
                                    catchError(stageResult: 'FAILURE') {
                                        script {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                        checkout scm
                                                        runSctTest(run_params, builder.region, functional_test=false, pipelineParams)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                stage("${phase} - Collect logs") {
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
                                stage("${phase} - Clean resources") {
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
                                stage("${phase} - Send email with result") {
                                    catchError(stageResult: 'FAILURE') {
                                        script {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: 10, unit: 'MINUTES') {
                                                        runSendEmail(run_params, currentBuild)
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
            stage('Finish Argus Test Run') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        finishArgusTestRun(params, currentBuild)
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
