#!groovy
import groovy.json.JsonSlurper

def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]
def base_versions_list = []

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter)

    pipeline {
        agent none

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_GCE_PROJECT = "${params.gce_project}"
        }
        parameters {
            // Cloud Provider Configuration
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')
            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'us-east-1|eu-west-1',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('availability_zone', 'a')}",
                description: 'Availability zone',
                name: 'availability_zone')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            // ScyllaDB Configuration
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection (Choose only one from below 6 options)')
            string(defaultValue: '', description: 'AMI ID for ScyllaDB', name: 'scylla_ami_id')
            string(defaultValue: '', description: 'Version of ScyllaDB', name: 'scylla_version')
            string(defaultValue: "${pipelineParams.get('base_versions', '')}",
                   description: 'Base version in which the upgrade will start from.\nFormat should be for example -> 4.5,4.6 (or single version, or \'\' to use the auto mode)',
                   name: 'base_versions')
            string(defaultValue: '',
                   description: 'ScyllaDB repository e.g. http://downloads.scylladb.com/deb/debian/scylla-2025.2.list',
                   name: 'new_scylla_repo')
            string(defaultValue: '',
                   description: 'ScyllaDB repository e.g. http://downloads.scylladb.com/deb/debian/scylla-2025.2.list',
                   name: 'scylla_repo')
            string(defaultValue: '',
                   description: 'cloud path for RPMs, s3:// or gs://',
                   name: 'update_db_packages')

            // Provisioning Configuration
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'on_demand')}",
                   description: 'on_demand|spot_fleet|spot',
                   name: 'provision_type')

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
            string(defaultValue: "${pipelineParams.get('post_behavior_k8s_cluster', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_k8s_cluster')
            // Performance Test Configuration
            separator(name: 'PERF_TEST', sectionHeader: 'Performance Test Configuration')
            string(defaultValue: "false",
                   description: 'Stop test if perf hardware test values exceed the set limits',
                   name: 'stop_on_hw_perf_failure')
            string(defaultValue: "${groovy.json.JsonOutput.toJson(pipelineParams.get('sub_tests', ''))}",
                   description: 'subtests in format ["sub_test1", "sub_test2"] or empty',
                   name: 'sub_tests')

            // Email and Test Configuration
            separator(name: 'EMAIL_TEST', sectionHeader: 'Email and Test Configuration')
            string(defaultValue: "${pipelineParams.get('test_email_title', '')}",
                   description: 'String added to test email subject',
                   name: 'test_email_title')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'scylla-perf-results@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')

            // Kubernetes Configuration
            separator(name: 'K8S_CONFIG', sectionHeader: 'Kubernetes Configuration')
            string(defaultValue: "${pipelineParams.get('k8s_version', '')}",
                   description: 'K8S version to be used. Suitable for EKS and GKE, but not local K8S (KinD). '
                   + 'In case of K8S platform upgrade it will be base one, target one will be automatically incremented. Example: "1.28"',
                   name: 'k8s_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_helm_repo', 'https://storage.googleapis.com/scylla-operator-charts/latest')}",
                   description: 'Scylla Operator helm repo',
                   name: 'k8s_scylla_operator_helm_repo')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_chart_version', 'latest')}",
                   description: 'Scylla Operator helm chart version',
                   name: 'k8s_scylla_operator_chart_version')
            string(defaultValue: "${pipelineParams.get('k8s_scylla_operator_docker_image', '')}",
                   description: 'Scylla Operator docker image',
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: "${pipelineParams.get('k8s_enable_tls', '')}",
                   description: 'if true, enable operator tls feature',
                   name: 'k8s_enable_tls')
            string(defaultValue: "${pipelineParams.get('k8s_enable_sni', '')}",
                   description: 'if true, install haproxy ingress controller and use it',
                   name: 'k8s_enable_sni')

            // Miscellaneous Configuration
            separator(name: 'MISC_CONFIG', sectionHeader: 'Miscellaneous Configuration')
            string(defaultValue: "${pipelineParams.get('gce_project', '')}",
                   description: 'Gce project to use',
                   name: 'gce_project')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            string(defaultValue: "${pipelineParams.get('perf_extra_jobs_to_compare', '')}",
                   description: 'jobs to compare performance results with, for example if running in staging, '
                                + 'we still can compare with official jobs',
                   name: 'perf_extra_jobs_to_compare')
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                 description: (
                     'Extra environment variables to be set in the test environment, uses the java Properties File Format.\n' +
                     'Example:\n' +
                     '\tSCT_STRESS_IMAGE.cassandra-stress=scylladb/cassandra-stress:3.13.0\n' +
                     '\tSCT_USE_MGMT=false'
                     ),
                 name: 'extra_environment_variables')
            booleanParam(defaultValue: false,
                         description: 'if true, use job throttling to limit the number of concurrent builds',
                         name: 'use_job_throttling')
            string(defaultValue: null,
                description: 'if set would override the default job throttling category',
                name: 'job_throttle_category')

            // BYO ScyllaDB Configuration
            separator(name: 'BYO_SCYLLA', sectionHeader: 'BYO ScyllaDB Configuration')
            string(defaultValue: '',
                   description: (
                       'Custom "scylladb" repo to use. Leave empty if byo is not needed. ' +
                       'If set then it must be proper GH repo. Example: git@github.com:personal-username/scylla.git\n' +
                       'and, in case of an "rolling upgrade", need to define "base_versions" param explicitly.'),
                   name: 'byo_scylla_repo')
            string(defaultValue: '',
                   description: 'Branch of the custom "scylladb" repo. Leave empty if byo is not needed.',
                   name: 'byo_scylla_branch')
            string(defaultValue: '/scylla-master/byo/byo_build_tests_dtest',
                   description: 'Used when byo scylladb repo+branch is provided. Default "/scylla-master/byo/byo_build_tests_dtest"',
                   name: 'byo_job_path')
            string(defaultValue: 'scylla',
                   description: '"scylla" or "scylla-enterprise". Default is "scylla".',
                   name: 'byo_default_product')
            string(defaultValue: 'next',
                   description: 'Default branch to be used for scylla and other repositories. Default is "next".',
                   name: 'byo_default_branch')
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
                                wrap([$class: 'BuildUser']) {
                                    loadEnvFromString(params.extra_environment_variables)
                                    dir('scylla-cluster-tests') {
                                        checkout scm
                                        dockerLogin(params)
                                        (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
                                        ArrayList base_versions_list = params.base_versions.contains('.') ? params.base_versions.split('\\,') : []
                                        def new_repo = params.new_scylla_repo
                                        supportedVersions = ''
                                        new_params = params.collectEntries { param -> [param.key, param.value] }
                                        if (new_repo) {
                                            // NOTE: master and enterprise will have 1 single version, but for releases,
                                            // we will choose automatically only the last one
                                            supportedVersions = supportedUpgradeFromVersions(
                                                base_versions_list,
                                                'ubuntu-focal',
                                                new_repo,
                                                params.backend
                                            ).last()
                                            new_params["scylla_version"] = supportedVersions
                                        }
                                        println("the supported version is $supportedVersions")
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('BYO Scylladb [optional]') {
                agent {
                    label {
                        label builder.label
                    }
                }
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 240, unit: 'MINUTES') {
                                        byoScylladb(params, true)
                                    }
                                }
                            }
                        }
                    }
                }
                post{
                    failure {
                        script{
                            sh "exit 1"
                        }
                    }
                    unstable {
                        script{
                            sh "exit 1"
                        }
                    }
                }
            }
            stage('Run SCT Performance Tests') {
                steps {
                    script {
                        def tasks = [:]
                        def params_mapping = [:]
                        def sub_tests
                        if (params.sub_tests) {
                            sub_tests = new JsonSlurper().parseText(params.sub_tests)
                        } else {
                            sub_tests = [params.test_name]
                        }
                        // select the step function to use for throttling, if not throttling, it's a no-op
                        def throttle_closure = params.use_job_throttling ? this.&throttle : { labels, closure -> closure() }
                        def job_throttle_category = params.job_throttle_category ?: "SCT-perf-${builder.region}"
                        for (t in sub_tests) {
                            def perf_test
                            def sub_test = t
                            if (sub_test == params.test_name) {
                                perf_test = sub_test
                            } else {
                                perf_test = "${params.test_name}.${sub_test}"
                            }

                            // Create params_mapping for each sub_test
                            params_mapping[sub_test] = params.collectEntries { param -> [param.key, param.value] }
                            params_mapping[sub_test].put('test_name', perf_test)

                            // Add supportedVersions if available
                            if (supportedVersions) {
                                params_mapping[sub_test].put('scylla_version', supportedVersions)
                            }

                            tasks["sub_test=${sub_test}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",
                                             "SCT_GCE_PROJECT=${env.SCT_GCE_PROJECT ?: ''}",]) {
                                        stage("Checkout for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                timeout(time: 5, unit: 'MINUTES') {
                                                    script {
                                                        wrap([$class: 'BuildUser']) {
                                                            loadEnvFromString(params_mapping[sub_test].extra_environment_variables)
                                                            dir('scylla-cluster-tests') {
                                                                checkout scm
                                                                checkoutQaInternal(params_mapping[sub_test])
                                                            }
                                                        dockerLogin(params_mapping[sub_test])
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage('Create Argus Test Run') {
                                            catchError(stageResult: 'FAILURE') {
                                                script {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            timeout(time: 5, unit: 'MINUTES') {
                                                                createArgusTestRun(params_mapping[sub_test])
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Create SCT Runner for ${sub_test}") {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    timeout(time: 10, unit: 'MINUTES') {
                                                        createSctRunner(params_mapping[sub_test], runnerTimeout, builder.region)
                                                    }
                                                }
                                            }
                                        }

                                        stage("Provision Resources for ${sub_test}") {
                                            script {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        timeout(time: 30, unit: 'MINUTES') {
                                                            if (params_mapping[sub_test].backend == 'aws' || params_mapping[sub_test].backend == 'azure') {
                                                                provisionResources(params_mapping[sub_test], builder.region)
                                                            } else {
                                                                sh """
                                                                    echo 'Skipping because non-AWS/Azure backends are not supported'
                                                                """
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        stage("Run ${sub_test}"){
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                        dir('scylla-cluster-tests') {
                                                            runSctTest(params_mapping[sub_test], builder.region, false, pipelineParams)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Collect logs for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                        dir('scylla-cluster-tests') {
                                                            runCollectLogs(params_mapping[sub_test], builder.region)
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
                                                            runCleanupResource(params_mapping[sub_test], builder.region)
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
                                                            runSendEmail(params_mapping[sub_test], currentBuild)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage('Clean SCT Runners') {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        cleanSctRunners(params_mapping[sub_test], currentBuild)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        throttle_closure([job_throttle_category]) {
                            parallel tasks
                        }
                    }
                }
            }
        }
    }
}
