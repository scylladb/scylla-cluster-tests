#!groovy

def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region)

    pipeline {
        agent {
            label {
                label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_TEST_ID           = UUID.randomUUID().toString()
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
                   description: 'gce',
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('region', 'us-east1')}",
               description: 'Region value',
               name: 'region')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '',
                   description: 'a Scylla version to run against',
                   name: 'scylla_version')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against',
                   name: 'scylla_repo')

            string(defaultValue: '',
                   description: 'a link to the git repository with Jepsen Scylla tests',
                   name: 'jepsen_scylla_repo')
            string(defaultValue: '',
                   description: "Jepsen test command(s) (e.g., 'test-all')",
                   name: 'jepsen_test_cmd')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')
            string(defaultValue: "${pipelineParams.get('provision_type', 'on_demand')}",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('instance_provision_fallback_on_demand', 'false')}",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'a config file for the test',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: "${pipelineParams.get('builds_to_keep', '20')}",))
        }
        stages {
            stage('Checkout') {
                steps {
                    script {
                        completed_stages = [:]
                    }
                    dir('scylla-cluster-tests') {
                        timeout(time: 5, unit: 'MINUTES') {
                            checkout scm
                            checkoutQaInternal(params)
                        }
                    }
                }
            }
            stage('Get test duration') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        timeout(time: 10, unit: 'MINUTES') {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
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
            stage('Run SCT Test') {
                steps {
                    script {
                        timeout(time: testRunTimeout, unit: 'MINUTES') {
                            sctScript """
                                rm -fv ./latest

                                export SCT_CONFIG_FILES=${params.test_config}
                                export SCT_COLLECT_LOGS=false

                                if [[ ! -z "${params.scylla_version}" ]]; then
                                    export SCT_SCYLLA_VERSION="${params.scylla_version}"
                                elif [[ ! -z "${params.scylla_repo}" ]]; then
                                    export SCT_SCYLLA_REPO="${params.scylla_repo}"
                                else
                                    echo "need to choose one of SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO"
                                    exit 1
                                fi

                                if [[ -n "${params.jepsen_scylla_repo}" ]]; then
                                    export SCT_JEPSEN_SCYLLA_REPO="${params.jepsen_scylla_repo}"
                                fi

                                if [[ -n "${params.jepsen_test_cmd}" ]]; then
                                    export SCT_JEPSEN_TEST_CMD="${params.jepsen_test_cmd}"
                                fi

                                export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                export SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND="${params.instance_provision_fallback_on_demand ? params.instance_provision_fallback_on_demand : ''}"

                                echo "start test ......."
                                RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                if [[ -n "\${RUNNER_IP}" ]] ; then
                                    ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} run-test jepsen_test --backend gce
                                else
                                    ./docker/env/hydra.sh run-test jepsen_test --backend gce --logdir "`pwd`"
                                fi
                                echo "end test ....."
                            """
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
                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                        runCollectLogs(params, builder.region)
                                    }
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
                                    timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                        runCleanupResource(params, builder.region)
                                        completed_stages['clean_resources'] = true
                                    }
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
                                    timeout(time: 10, unit: 'MINUTES') {
                                        runSendEmail(params, currentBuild)
                                        completed_stages['send_email'] = true
                                    }
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
            stage('Finish Argus Test Run') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        finishArgusTestRun(params, currentBuild)
                                        completed_stages['report_to_argus'] = true
                                    }
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
                                        timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                            runCleanupResource(params, builder.region)
                                        }
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
                                        timeout(time: 10, unit: 'MINUTES') {
                                            runSendEmail(params, currentBuild)
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!completed_stages['report_to_argus']) {
                        catchError {
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
}
