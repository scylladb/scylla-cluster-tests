#!groovy

def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, params.azure_region_name)
    def functional_test = pipelineParams.functional_test

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
        }
        parameters {
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce|azure|docker|xcloud',
               name: 'backend')

            choice(name: 'xcloud_provider',
                   choices: ['aws', 'gce'],
                   description: 'Cloud provider for Scylla Cloud backend (only used when backend=xcloud). Supported providers: aws, gce',)

            string(defaultValue: "${pipelineParams.get('xcloud_env', 'lab')}",
                   description: 'Scylla Cloud environment (only used when backend=xcloud). Supported environments: lab',
                   name: 'xcloud_env')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure location',
                   name: 'azure_region_name')
            string(defaultValue: "${pipelineParams.get('availability_zone', 'a')}",
               description: 'Availability zone',
               name: 'availability_zone')

            // Stress Test Configuration
            separator(name: 'STRESS_TEST', sectionHeader: 'Stress Test Configuration')
            string(defaultValue: "",
               description: 'Duration in minutes for stress commands(gemini, c-s, s-b)',
               name: 'stress_duration')

            string(defaultValue: "",
               description: ('Time duration in minutes for preparing dataset with commands prepare_*_cmd, if empty value, default value is 5h = 300 minutes.' +
                             'Prepare commands could finish earlier and have not to run full prepare_stress_duration time'),
               name: 'prepare_stress_duration')

            // ScyllaDB Configuration
	    separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection (Choose only one from below 6 options)')
	    string(defaultValue: '', description: 'AMI ID for ScyllaDB ', name: 'scylla_ami_id')
	    string(defaultValue: '', description: 'GCE image for ScyllaDB ', name: 'gce_image_db')
	    string(defaultValue: '', description: 'Azure image for ScyllaDB ', name: 'azure_image_db')
	    string(defaultValue: '', description: 'cloud path for RPMs, s3:// or gs:// ', name: 'update_db_packages')
            string(defaultValue: "${pipelineParams.get('scylla_version', '')}",
                   description: 'Version of ScyllaDB',
                   name: 'scylla_version')
            string(name: 'scylla_repo', defaultValue: '', description: 'Repository for ScyllaDB')

            // Provisioning Configuration
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot|on_demand|spot_fleet',
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
            string(defaultValue: "${pipelineParams.get('post_behavior_k8s_cluster', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_k8s_cluster')

            // SSH Configuration
            separator(name: 'SSH_CONFIG', sectionHeader: 'SSH Configuration')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            // Manager Configuration
            separator(name: 'MANAGER_CONFIG', sectionHeader: 'Manager Configuration')
            string(defaultValue: "${pipelineParams.get('manager_version', '')}",
                   description: 'master_latest|3.2|3.1',
                   name: 'manager_version')

            string(defaultValue: '',
                   description: 'If empty - the default manager version will be taken',
                   name: 'scylla_mgmt_address')

            string(defaultValue: '', description: 'Version of Management Agent', name: 'scylla_mgmt_agent_version')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_address', '')}",
                   description: 'If empty - the default scylla manager agent repo will be taken',
                   name: 'scylla_mgmt_agent_address')
            // Email and Test Configuration
            separator(name: 'EMAIL_TEST', sectionHeader: 'Email and Test Configuration')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')

            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')

            string(defaultValue: '', description: 'run gemini job with specific gemini seed number',
                   name: "gemini_seed")

            string(defaultValue: "${pipelineParams.get('pytest_addopts', '')}",
                   description: (
                        '"pytest_addopts" is used by "run_pytest" hydra command. \n' +
                        'Useful for K8S functional tests which run using pytest. \n' +
                        'PyTest runner allows to provide any options using "PYTEST_ADDOPTS" ' +
                        'env var which gets set here if value is provided. \n' +
                        'Example: "--maxfail=1" - it will stop test run after first failure.'),
                   name: 'pytest_addopts')

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

            string(defaultValue: "${pipelineParams.get('docker_image', '')}",
                   description: 'Scylla docker image repo',
                   name: 'docker_image')

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
            // BYO ScyllaDB Configuration
            separator(name: 'BYO_SCYLLA', sectionHeader: 'BYO ScyllaDB Configuration')
            string(defaultValue: '',
                   description: (
                       'Custom "scylladb" repo to use. Leave empty if byo is not needed. \n' +
                       'If set then it must be proper GH repo. Example: git@github.com:personal-username/scylla.git'),
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
            stage('Checkout') {
                steps {
                    script {
                        completed_stages = [:]
                        loadEnvFromString(params.extra_environment_variables)
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
            stage('BYO Scylladb [optional]') {
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
            stage('Provision Resources') {
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                timeout(time: 30, unit: 'MINUTES') {
                                    if (params.backend == 'xcloud') {
                                        echo "Scylla Cloud backend selected: provisioning loader nodes only on ${params.xcloud_provider} cloud provider"
                                    }
                                    if (params.backend == 'xcloud' || params.backend == 'aws' || params.backend == 'azure') {
                                        provisionResources(params, builder.region)
                                    } else if (params.backend.contains('docker')) {
                                        sh """
                                            echo 'Tests are to be executed on Docker backend in SCT-Runner. No additional resources to be provisioned.'
                                        """
                                    } else {
                                        sh """
                                            echo 'Skipping because non-AWS/Azure backends are not supported'
                                        """
                                    }
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
                                        runSctTest(params, builder.region, functional_test, pipelineParams)
                                        completed_stages['run_tests'] = true
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage("Parallel timelines report") {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        runGeneratePTReport(testDuration)
                                        completed_stages['generate_parallel_timelines_report'] = true
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
                    def provision_resources = completed_stages['provision_resources']
                    def run_tests = completed_stages['run_tests']
                    def collect_logs = completed_stages['collect_logs']
                    def clean_resources = completed_stages['clean_resources']
                    def send_email = completed_stages['send_email']
                    def clean_sct_runner = completed_stages['clean_sct_runner']
                    sh """
                        echo "'provision_resources' stage is completed: $provision_resources"
                        echo "'run_tests' stage is completed: $run_tests"
                        echo "'collect_logs' stage is completed: $collect_logs"
                        echo "'clean_resources' stage is completed: $clean_resources"
                        echo "'send_email' stage is completed: $send_email"
                        echo "'clean_sct_runner' stage is completed: $clean_sct_runner"
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
