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
            string(defaultValue: "",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '',
                   description: 'a Scylla version to run against',
                   name: 'scylla_version')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against',
                   name: 'scylla_repo')
            string(defaultValue: '',
                   description: 'GCE image for ScyllaDB ',
                   name: 'gce_image_db')

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
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                 description: (
                     'Extra environment variables to be set in the test environment, uses the java Properties File Format.\n' +
                     'Example:\n' +
                     '\tSCT_STRESS_IMAGE.cassandra-stress=scylladb/cassandra-stress:3.13.0\n' +
                     '\tSCT_USE_MGMT=false'
                     ),
                 name: 'extra_environment_variables')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: "${pipelineParams.get('builds_to_keep', '20')}",))
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
            stage('Checkout') {
                steps {
                    script {
                        loadEnvFromString(params.extra_environment_variables)
                        completed_stages = [:]
                    }
                    dir('scylla-cluster-tests') {
                        timeout(time: 5, unit: 'MINUTES') {
                            checkout scm
                            checkoutQaInternal(params)
                        }
                    }
                    dockerLogin(params)
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
                                if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
                                    export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
                                fi

                                if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
                                    export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
                                fi
                                if [[ ! -z "${params.scylla_version}" ]]; then
                                    export SCT_SCYLLA_VERSION="${params.scylla_version}"
                                elif [[ ! -z "${params.scylla_repo}" ]]; then
                                    export SCT_SCYLLA_REPO="${params.scylla_repo}"
                                elif [[ ! -z "${params.gce_image_db}" ]]; then
                                    export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
                                else
                                    echo "need to choose one of SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO | SCT_GCE_IMAGE_DB"
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
                                        completed_stages['collect_logs'] = true
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
                                    completed_stages['clean_sct_runner'] = true
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
                    def run_tests = completed_stages['run_tests']
                    def collect_logs = completed_stages['collect_logs']
                    def clean_resources = completed_stages['clean_resources']
                    def send_email = completed_stages['send_email']
                    def clean_sct_runner = completed_stages['clean_sct_runner']
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
