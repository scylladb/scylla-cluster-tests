#!groovy

// Consolidated minicloud pipeline for artifact and longevity/provision tests.
// Based on longevityPipeline — uses sct-runners with KVM-enabled instances.
// MinicloudManager in tester.py handles the full minicloud lifecycle on the
// sct-runner: preflight, container start, health checks, region prep, cleanup.
//
// Minicloud is NOT a separate backend — it emulates AWS/GCE compute APIs
// via QEMU/KVM VMs running inside the minicloud container on the sct-runner.

def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter)

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
            SCT_GCE_PROJECT = "${params.gce_project}"
            SCT_BILLING_PROJECT = "${params.billing_project}"
            // Minicloud endpoint — MinicloudManager uses this to detect minicloud mode
            SCT_MINICLOUD_ENDPOINT_URL = 'http://localhost:5000'
            // KMS must be disabled for minicloud (no real cloud KMS available)
            SCT_ENTERPRISE_DISABLE_KMS = 'true'
            SCT_ENABLE_KMS_KEY_ROTATION = 'false'
            // Minicloud Docker image — forwarded to sct-runner via hydra env
            MINICLOUD_DOCKER = "${params.minicloud_docker}"
            // Region must match between minicloud and SCT
            MINICLOUD_AWS_REGION = "${params.region}"
        }
        parameters {
            // Cloud Provider Configuration
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
                   description: 'aws|gce (minicloud emulates these backends)',
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
                   description: 'AWS region (used for sct-runner placement and minicloud region emulation)',
                   name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "${pipelineParams.get('availability_zone', '')}",
                   description: 'Availability zone',
                   name: 'availability_zone')

            // Minicloud Configuration
            separator(name: 'MINICLOUD_CONFIG', sectionHeader: 'Minicloud Configuration')
            string(defaultValue: "${pipelineParams.get('minicloud_docker', 'scylladb/minicloud:dev')}",
                   description: 'Minicloud Docker image reference (e.g. scylladb/minicloud:dev, registry/minicloud:v1.0)',
                   name: 'minicloud_docker')

            // ScyllaDB Configuration
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration')
            string(defaultValue: '', description: 'AMI ID for ScyllaDB', name: 'scylla_ami_id')
            string(defaultValue: '', description: 'GCE image for ScyllaDB', name: 'gce_image_db')
            string(defaultValue: "${pipelineParams.get('scylla_version', '')}",
                   description: 'Version of ScyllaDB to run against (e.g. 2026.2, master:latest)',
                   name: 'scylla_version')

            // Stress Test Configuration
            separator(name: 'STRESS_TEST', sectionHeader: 'Stress Test Configuration')
            string(defaultValue: '',
                   description: 'Duration in minutes for stress commands (gemini, c-s, s-b)',
                   name: 'stress_duration')
            string(defaultValue: '',
                   description: 'Duration in minutes for prepare commands',
                   name: 'prepare_stress_duration')

            // Provisioning Configuration
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'on_demand')}",
                   description: 'on_demand|spot (on_demand recommended for minicloud runners)',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('instance_provision_fallback_on_demand', '')}",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            // Post Behavior Configuration
            separator(name: 'POST_BEHAVIOR', sectionHeader: 'Post Behavior Configuration')
            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            // SSH Configuration
            separator(name: 'SSH_CONFIG', sectionHeader: 'SSH Configuration')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            // Email and Test Configuration
            separator(name: 'EMAIL_TEST', sectionHeader: 'Email and Test Configuration')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run (e.g. artifacts_test or longevity_test.LongevityTest.test_custom_time)',
                   name: 'test_name')

            // Miscellaneous Configuration
            separator(name: 'MISC_CONFIG', sectionHeader: 'Miscellaneous Configuration')
            string(defaultValue: "${pipelineParams.get('gce_project', '')}",
                   description: 'GCE project to use',
                   name: 'gce_project')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds',
                   name: 'requested_by_user')
            choice(choices: getBillingProjectChoices(),
                   description: 'Billing project for the test run',
                   name: 'billing_project')
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                 description: ('Extra environment variables (java Properties File Format).\n'
                     + 'Example:\n'
                     + '\tSCT_N_DB_NODES=1\n'
                     + '\tSCT_APPEND_SCYLLA_ARGS=--memory 256M'),
                 name: 'extra_environment_variables')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage("Preparation") {
                // NOTE: workaround for https://issues.jenkins-ci.org/browse/JENKINS-41929
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
                        completed_stages = [:]
                        loadEnvFromString(params.extra_environment_variables)
                        tagBuilder()
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
                                    createSctRunner(params, runnerTimeout, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage('Start Minicloud') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 10, unit: 'MINUTES') {
                                        sh """#!/bin/bash
                                        set -xe
                                        env

                                        export SCT_CLUSTER_BACKEND="${params.backend}"
                                        export SCT_CONFIG_FILES=${groovy.json.JsonOutput.toJson(params.test_config)}
                                        export MINICLOUD_AWS_REGION="${params.region}"

                                        RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                        if [[ -n "\${RUNNER_IP}" ]] ; then
                                            ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} start-minicloud -b "${params.backend}"
                                        else
                                            ./docker/env/hydra.sh start-minicloud -b "${params.backend}"
                                        fi
                                        """
                                        completed_stages['start_minicloud'] = true
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('Provision Resources') {
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                timeout(time: 30, unit: 'MINUTES') {
                                    provisionResources(params, builder.region)
                                    completed_stages['provision_resources'] = true
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
                                    timeout(time: testRunTimeout, unit: 'MINUTES') {
                                        runSctTest(params, builder.region)
                                        completed_stages['run_tests'] = true
                                    }
                                }
                            }
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
                                        completed_stages['collect_logs'] = true
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
        }
        post {
            always {
                script {
                    def provision_resources = completed_stages['provision_resources']
                    def run_tests = completed_stages['run_tests']
                    def collect_logs = completed_stages['collect_logs']
                    def clean_resources = completed_stages['clean_resources']
                    def send_email = completed_stages['send_email']
                    def clean_sct_runner = completed_stages['clean_sct_runner']
                    sh """
                        echo "'provision_resources' stage completed: $provision_resources"
                        echo "'run_tests' stage completed: $run_tests"
                        echo "'collect_logs' stage completed: $collect_logs"
                        echo "'clean_resources' stage completed: $clean_resources"
                        echo "'send_email' stage completed: $send_email"
                        echo "'clean_sct_runner' stage completed: $clean_sct_runner"
                    """
                    if (!completed_stages['collect_logs']) {
                        catchError {
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
                    if (!completed_stages['clean_sct_runner']) {
                        catchError {
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
            }
        }
    }
}
