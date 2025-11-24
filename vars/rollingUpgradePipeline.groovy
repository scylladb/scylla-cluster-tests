#!groovy

List supportedVersions = []
def params_mapping = [:] // this would hold the params per split of this pipeline
def completed_stages = [:]
(testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, params.azure_region_name)

    // since this is a boolean param, we need to handle its default value upfront, we can't do it in the parameters section
    // we'll keep it as boolean to simplify its usage later on
    def base_version_all_sts_versions = pipelineParams.get('base_version_all_sts_versions', false)

    pipeline {
        agent none

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_GCE_PROJECT = "${params.gce_project}"
        }
        parameters {
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: '',
                   description: 'a Azure Image to run against',
                   name: 'azure_image_db')
            string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure location',
                   name: 'azure_region_name')
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
               description: 'aws|gce|azure',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection')
            string(defaultValue: "${pipelineParams.get('scylla_ami_id', '')}", description: 'AMI ID for ScyllaDB ', name: 'scylla_ami_id')
            string(defaultValue: "${pipelineParams.get('gce_image_db', '')}", description: 'GCE image for ScyllaDB ', name: 'gce_image_db')
	        string(defaultValue: "${pipelineParams.get('azure_image_db', '')}", description: 'Azure image for ScyllaDB ', name: 'azure_image_db')

            string(defaultValue: '', description: '', name: 'new_scylla_repo')
            booleanParam(defaultValue: base_version_all_sts_versions,
                         description: 'Whether to include all supported STS versions as base versions',
                         name: 'base_version_all_sts_versions')
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'on_demand|spot_fleet|spot',
                   name: 'provision_type')
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
            string(defaultValue: '', description: 'scylla option: internode_compression', name: 'internode_compression')
            separator(name: 'EMAIL_TEST', sectionHeader: 'Email and Test Configuration')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')
            string(defaultValue: "${pipelineParams.get('base_versions', '')}",
                   description: 'Base version in which the upgrade will start from.\nFormat should be for example -> 4.5,4.6 (or single version, or \'\' to use the auto mode)',
                   name: 'base_versions')
            string(defaultValue: "${pipelineParams.get('gce_project', '')}",
                   description: 'Gce project to use',
                   name: 'gce_project')

            // NOTE: Optional parameters for BYO ScyllaDB stage
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
            string(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                   description: 'Extra environment variables to inject (format: KEY1=VAL1\nKEY2=VAL2)',
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
            stage('Get supported Scylla versions and test duration') {
                agent {
                    label {
                        label builder.label
                    }
                }
                steps {
                    catchError(stageResult: "FAILURE") {
                        timeout(time: 10, unit: 'MINUTES') {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    loadEnvFromString(params.extra_environment_variables)
                                    dir('scylla-cluster-tests') {
                                        checkout scm
                                        checkoutQaInternal(params)
                                        dockerLogin(params)

                                        completed_stages = [:]

                                        ArrayList base_versions_list = params.base_versions.contains('.') ? params.base_versions.split('\\,') : []
                                        supportedVersions = supportedUpgradeFromVersions(
                                            base_versions_list,
                                            pipelineParams.linux_distro,
                                            params.new_scylla_repo,
                                            params.backend,
                                            params.base_version_all_sts_versions
                                        )
                                        (testDuration,
                                         testRunTimeout,
                                         runnerTimeout,
                                         collectLogsTimeout,
                                         resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
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
                                        byoScylladb(params, false)
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
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]
                        params_mapping = [:]
                        for (version in supportedVersions) {
                            def base_version = version
                            params_mapping[base_version] = params.collectEntries { param -> [param.key, param.value] }
                            params_mapping[base_version].put('scylla_version', base_version)
                            // since scylla-pkg might pass this one, we are not supporting it here, as we always starts by version
                            params_mapping[base_version].remove('scylla_repo')

                            // those params are not in the job params, so user can`t change them
                            // but they are coming from the pipelineParams, i.e. hardcoded per use case
                            params_mapping[base_version]['use_preinstalled_scylla'] = pipelineParams.use_preinstalled_scylla
                            params_mapping[base_version]['disable_raft'] = pipelineParams.disable_raft
                            params_mapping[base_version]['linux_distro'] = pipelineParams.linux_distro
                            params_mapping[base_version]['internode_compression'] = pipelineParams.internode_compression

                            completed_stages[base_version] = [:]

                            tasks["${base_version}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                                        stage("Split for ${base_version}") {
                                            try {
                                                stage("Checkout for ${base_version}") {
                                                    catchError(stageResult: 'FAILURE') {
                                                        timeout(time: 5, unit: 'MINUTES') {
                                                            script {
                                                                loadEnvFromString(params.extra_environment_variables)
                                                                wrap([$class: 'BuildUser']) {
                                                                    dir('scylla-cluster-tests') {
                                                                        checkout scm
                                                                        checkoutQaInternal(params_mapping[base_version])
                                                                    }
                                                                }
                                                                dockerLogin(params_mapping[base_version])
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
                                                                        createArgusTestRun(params_mapping[base_version])
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Create SCT Runner for ${base_version}") {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            timeout(time: 5, unit: 'MINUTES') {
                                                                createSctRunner(params_mapping[base_version], runnerTimeout, builder.region)
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Provision Resources for ${base_version}") {
                                                    script {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 30, unit: 'MINUTES') {
                                                                    if (params.backend == 'aws' || params.backend == 'azure') {
                                                                        provisionResources(params_mapping[base_version], builder.region)
                                                                    } else {
                                                                        sh """
                                                                            echo 'Skipping because non-AWS/Azure backends are not supported'
                                                                        """
                                                                    }
                                                                    completed_stages[base_version]['provision_resources'] = true
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Upgrade from ${base_version}") {
                                                    catchError(stageResult: 'FAILURE') {
                                                        wrap([$class: 'BuildUser']) {
                                                            timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                                dir('scylla-cluster-tests') {
                                                                    runSctTest(params_mapping[base_version], builder.region, false, pipelineParams)
                                                                    completed_stages[base_version]['run_tests'] = true
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Collect logs for Upgrade from ${base_version}") {
                                                    catchError(stageResult: 'FAILURE') {
                                                        wrap([$class: 'BuildUser']) {
                                                            timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                                dir('scylla-cluster-tests') {
                                                                    runCollectLogs(params_mapping[base_version], builder.region)
                                                                    completed_stages[base_version]['collect_logs'] = true
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Clean resources for Upgrade from ${base_version}") {
                                                    catchError(stageResult: 'FAILURE') {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                                                    runCleanupResource(params_mapping[base_version], builder.region)
                                                                    completed_stages[base_version]['clean_resources'] = true
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Send email for Upgrade from ${base_version}") {
                                                    def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)
                                                    catchError(stageResult: 'FAILURE') {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 10, unit: 'MINUTES') {
                                                                    runSendEmail(params_mapping[base_version], currentBuild)
                                                                    completed_stages[base_version]['send_email'] = true
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage('Clean SCT Runners') {
                                                    catchError(stageResult: 'FAILURE') {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                cleanSctRunners(params_mapping[base_version], currentBuild)
                                                                completed_stages[base_version]['clean_sct_runner'] = true
                                                            }
                                                        }
                                                    }
                                                }
                                                stage('Finish Argus Test Run') {
                                                    catchError(stageResult: 'FAILURE') {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                    timeout(time: 5, unit: 'MINUTES') {
                                                                        finishArgusTestRun(params_mapping[base_version], currentBuild)
                                                                        completed_stages[base_version]['report_to_argus'] = true
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } finally {
                                                def provision_resources = completed_stages[base_version]['provision_resources']
                                                def run_tests = completed_stages[base_version]['run_tests']
                                                def collect_logs = completed_stages[base_version]['collect_logs']
                                                def clean_resources = completed_stages[base_version]['clean_resources']
                                                def send_email = completed_stages[base_version]['send_email']
                                                def clean_sct_runner = completed_stages[base_version]['clean_sct_runner']
                                                sh """
                                                    echo "'provision_resources' stage is completed: $provision_resources"
                                                    echo "'run_tests' stage is completed: $run_tests"
                                                    echo "'collect_logs' stage is completed: $collect_logs"
                                                    echo "'clean_resources' stage is completed: $clean_resources"
                                                    echo "'send_email' stage is completed: $send_email"
                                                    echo "'clean_sct_runner' stage is completed: $clean_sct_runner"
                                                """
                                                if (!completed_stages[base_version]['clean_resources']) {
                                                    catchError {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                    timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                                                        runCleanupResource(params_mapping[base_version], builder.region)
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                if (!completed_stages[base_version]['clean_sct_runner']) {
                                                    catchError {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                  cleanSctRunners(params_mapping[base_version], currentBuild)
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                if (!completed_stages[base_version]['send_email']) {
                                                    catchError {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                    timeout(time: 10, unit: 'MINUTES') {
                                                                        runSendEmail(params_mapping[base_version], currentBuild)
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                if (!completed_stages[base_version]['report_to_argus']) {
                                                    catchError {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                    timeout(time: 5, unit: 'MINUTES') {
                                                                        finishArgusTestRun(params_mapping[base_version], currentBuild)
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
                        }
                        parallel tasks
                    }
                }
            }
        }
    }
}
