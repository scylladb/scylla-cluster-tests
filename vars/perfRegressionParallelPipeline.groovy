#!groovy
import groovy.json.JsonSlurper

def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.aws_region)

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
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('aws_region', 'eu-west-1')}",
               description: 'Supported: us-east-1|eu-west-1|eu-west-2|eu-north-1|random (randomly select region)',
               name: 'aws_region')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '',
                   description: 'cloud path for RPMs, s3:// or gs://',
                   name: 'update_db_packages')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('instance_provision_fallback_on_demand', 'false')}",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            string(defaultValue: "${groovy.json.JsonOutput.toJson(pipelineParams.get('sub_tests'))}",
                   description: 'subtests in format ["sub_test1", "sub_test2"] or empty',
                   name: 'sub_tests')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')

            string(defaultValue: "${pipelineParams.get('email_recipients', 'scylla-perf-results@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Run SCT Performance Tests') {
                steps {
                    script {
                        def tasks = [:]
                        def test_params = [:]
                        def sub_tests
                        if (params.sub_tests) {
                            sub_tests = new JsonSlurper().parseText(params.sub_tests)
                        } else {
                            sub_tests = [pipelineParams.test_name]
                        }
                        stage('Checkout') {
                            dir('scylla-cluster-tests') {
                                timeout(time: 5, unit: 'MINUTES') {
                                    checkout scm
                                    dir("scylla-qa-internal") {
                                        git(url: 'git@github.com:scylladb/scylla-qa-internal.git',
                                            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                                            branch: 'master')
                                    }
                                }
                            }
                        }
                        stage('Get test duration') {
                            timeout(time: 1, unit: 'MINUTES') {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                        (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
                                    }
                                }
                            }
                        }
                        for (t in sub_tests) {
                            def perf_test
                            def sub_test = t
                            if (sub_test == pipelineParams.test_name) {
                                perf_test = sub_test
                            } else {
                                perf_test = "${pipelineParams.test_name}.${sub_test}"
                            }
                            test_params["${sub_test}"] = params + ["test_name":"${perf_test}", "test_id":UUID.randomUUID().toString()]
                            print("test_params=$test_params")
                            def completed_stages = [:]
                            completed_stages["${perf_test}"] = [:]
                            tasks["sub_test=${sub_test}"] = {
                                stage("Create SCT Runner for ${sub_test}") {
                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            timeout(time: 5, unit: 'MINUTES') {
                                                createSctRunner(test_params["${sub_test}"], runnerTimeout , builder.region)
                                            }
                                        }
                                    }
                                }
                                stage("Run ${sub_test}"){
                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                runSctTest(test_params["${sub_test}"], builder.region)
                                            }
                                        }
                                    }
                                }
                                stage("Collect logs of ${sub_test}") {
                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                runCollectLogs(test_params["${sub_test}"], builder.region)
                                            }
                                        }
                                    }
                                }
                                stage("Clean resources of ${sub_test}") {
                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                                runCleanupResource(test_params["${sub_test}"], builder.region)
                                                completed_stages["${perf_test}"]['clean_resources'] = true
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
