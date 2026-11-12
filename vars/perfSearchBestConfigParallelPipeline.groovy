#!groovy
import groovy.json.JsonSlurperClassic

def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, null /* azure placeholder */, params.oci_region_name)

    pipeline {
        agent none

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
		}
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | us-east-2 | us-west-2 | eu-west-1 | eu-west-2 | eu-west-3 | eu-north-1 | eu-central-1 | ca-central-1 | random (randomly select region)',
               name: 'region')

            string(defaultValue: "${pipelineParams.get('availability_zone', '')}",
                description: 'Availability zone',
                name: 'availability_zone')

            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')

            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '',
                   description: 'Version of ScyllaDB to run against. Can be a released version (2025.4) or a master (master:latest)',
                   name: 'scylla_version')
            string(defaultValue: '',
                   description: 'ScyllaDB packages repository (Debian/Ubuntu or RHEL-based). e.g. apt: http://downloads.scylladb.com/deb/debian/scylla-2025.4.list',
                   name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'on_demand|spot_fleet|spot|auto (auto: resolved by test duration, threshold spot_max_test_duration)',
                   name: 'provision_type')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_k8s_cluster', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_k8s_cluster')
            string(defaultValue: "${groovy.json.JsonOutput.toJson(pipelineParams.get('sub_tests', ''))}",
                   description: 'subtests in format ["sub_test1", "sub_test2"] or empty',
                   name: 'sub_tests')

            string(defaultValue: "${pipelineParams.get('email_recipients', 'scylla-perf-results@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')

            string(defaultValue: '', description: 'Instance db type', name: 'instance_type_db')
            string(defaultValue: '', description: 'Instance loader type', name: 'instance_type_loader')
            string(defaultValue: '', description: 'Prepared loader AMI (to run with different scylla drivers)', name: 'loader_ami_id')

            string(defaultValue: '', description: 'Start test with number of loaders', name: 'n_loaders')
            string(defaultValue: '', description: 'number of loaders on each round add/remove', name: 'num_loaders_step')

            string(defaultValue: '', description: 'Initial number of threads in c-s command', name: 'stress_threads_start_num')
            string(defaultValue: '', description: 'Add number of threads in c-s command on each round', name: 'add_num_threads_step')

            string(defaultValue: '', description: 'Number of stress process per loader', name: 'n_stress_process')
            string(defaultValue: '', description: 'Number of process add/remove on each round', name: 'stress_process_step')

            string(defaultValue: '', description: 'Stress step duration for c-s command, ex: 15m, 3h, etc. Default value is 15 minutes',
                   name: 'stress_step_duration')
            string(defaultValue: '', description: 'Relative difference between current and best to choose new best result',
                   name: 'max_deviation')

            string(defaultValue: '', description: 'Prepare Cassandra Stress command to run', name: 'prepare_write_cmd')
            string(defaultValue: '', description: 'Write Cassandra Stress command to run', name: 'stress_cmd_w')
            string(defaultValue: '', description: 'Read Cassandra Stress command to run', name: 'stress_cmd_r')
            string(defaultValue: '', description: 'Mixed Cassandra Stress command to run', name: 'stress_cmd_m')
            booleanParam(name: 'use_client_encryption',
                         defaultValue: false,
                         description: 'Enable client encryption')
            booleanParam(name: 'use_prepared_loaders',
                         defaultValue: true,
                         description: 'Run c-s process in docker (False), on prepared instance(True)')

            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_helm_repo', 'https://storage.googleapis.com/scylla-operator-charts/latest')}",
                   description: 'Scylla Operator helm repo',
                   name: 'k8s_scylla_operator_helm_repo')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_chart_version', 'latest')}",
                   description: 'Scylla Operator helm chart version',
                   name: 'k8s_scylla_operator_chart_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_docker_image', '')}",
                   description: 'Scylla Operator docker image',
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: "${pipelineParams.get('gce_project', '')}",
               description: 'Gce project to use',
               name: 'gce_project')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            choice(choices: getBillingProjectChoices(),
                   description: 'Billing project for the test run (dynamically fetched from finops repository)',
                   name: 'billing_project')
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
                agent {
                    label {
                        label builder.label
                    }
                }
                steps {
                    catchError(stageResult: 'FAILURE') {
                        timeout(time: 10, unit: 'MINUTES') {
                            script {
                                loadEnvFromString(params.extra_environment_variables)
                                tagBuilder()
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
            stage('Run SCT Performance Search best config tests') {
                steps {
                    script {
                        def tasks = [:]
                        def sub_tests
                        if (params.sub_tests) {
                            sub_tests = new JsonSlurperClassic().parseText(params.sub_tests)
                        } else {
                            sub_tests = [params.test_name]
                        }
                        for (t in sub_tests) {
                            def perf_test
                            def sub_test = t
                            if (sub_test == params.test_name) {
                                perf_test = sub_test
                            } else {
                                perf_test = "${params.test_name}.${sub_test}"
                            }

                            tasks["sub_test=${sub_test}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                                        stage("Checkout for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                timeout(time: 5, unit: 'MINUTES') {
                                                    script {
                                                        loadEnvFromString(params.extra_environment_variables)
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                checkout scm
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        // Before the runner exists, so an abort here costs nothing. Never fails the build:
                                        // a cost estimate is advisory, and each parallel branch provisions its own cluster,
                                        // so the job's real cost is this figure times the number of branches.
                                        stage("Estimate Test Cost for ${sub_test}") {
                                            catchError(stageResult: 'SUCCESS') {
                                                timeout(time: 5, unit: 'MINUTES') {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            estimateTestCost(params, builder.region)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Create SCT Runner for ${sub_test}") {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: 5, unit: 'MINUTES') {
                                                        createSctRunner(params, runnerTimeout, builder.region)
                                                    }
                                                }
                                            }
                                        }

                                        stage("Run ${sub_test}"){
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: testRunTimeout, unit: 'MINUTES') { dir('scylla-cluster-tests') {
                                                        withEnv(pipelineEnv(params: params,
                                                                            region: builder.region, pipelineParams: pipelineParams)) {
                                                        sh """#!/bin/bash
                                                        set -xe
                                                        env

                                                        rm -fv ./latest

                                                        echo "start test ......."
                                                        SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                                        if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
                                                            ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} run-test ${perf_test} --backend ${params.backend}
                                                        else
                                                            ./docker/env/hydra.sh run-test ${perf_test} --backend ${params.backend}  --logdir "`pwd`"
                                                        fi
                                                        echo "end test ....."
                                                        """
                                                        }
                                                    }}
                                                }
                                            }
                                        }
                                        stage("Collect logs for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                        dir('scylla-cluster-tests') {
                                                            runCollectLogs(params, builder.region)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Clean resources for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                                            runCleanupResource(params, builder.region)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Send email for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        timeout(time: 10, unit: 'MINUTES') {
                                                            runSendEmail(params, currentBuild)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage('Clean SCT Runners') {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        cleanSctRunners(params, currentBuild)
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
