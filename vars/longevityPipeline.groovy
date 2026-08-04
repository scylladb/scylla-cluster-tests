#!groovy

def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {

    // Captured locals, read from pipelineParams because this runs at pipeline-definition time where
    // `params` is still empty on build #1. Never read params.minicloud in a when{} guard:
    // getJenkinsLabels merges its `overrides` into an un-def'd global `params`, so it would appear
    // to work and then not.
    def minicloudEnabled = pipelineParams.get('minicloud', false)

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter,
                                   params.azure_region_name, params.oci_region_name)
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
            SCT_BILLING_PROJECT = "${params.billing_project}"
        }
        parameters {
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce|azure|oci|docker|xcloud',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('xcloud_provider', 'aws')}",
                   description: 'Cloud provider for Scylla Cloud backend (only used when backend=xcloud). Supported providers: aws, gce',
                   name: 'xcloud_provider')

            string(defaultValue: "${pipelineParams.get('xcloud_env', 'lab')}",
                   description: 'Scylla Cloud environment (only used when backend=xcloud). Supported environments: lab, staging, prod',
                   name: 'xcloud_env')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | us-east-2 | us-west-2 | eu-west-1 | eu-west-2 | eu-west-3 | eu-north-1 | eu-central-1 | ca-central-1 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure location',
                   name: 'azure_region_name')
            string(defaultValue: "${pipelineParams.get('oci_region_name', 'us-phoenix-1')}",
                   description: 'Oracle location',
                   name: 'oci_region_name')
            string(defaultValue: "${pipelineParams.get('availability_zone', '')}",
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
	    separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection (Choose only one from below 7 options)')
	    string(defaultValue: '', description: 'AMI ID for ScyllaDB ', name: 'scylla_ami_id')
	    string(defaultValue: '', description: 'GCE image for ScyllaDB ', name: 'gce_image_db')
	    string(defaultValue: '', description: 'Azure image for ScyllaDB ', name: 'azure_image_db')
	    string(defaultValue: '', description: 'Oracle image for ScyllaDB ', name: 'oci_image_db')
	    string(defaultValue: '', description: 'cloud path for RPMs, s3:// or gs:// ', name: 'update_db_packages')
            string(defaultValue: "${pipelineParams.get('scylla_version', '')}",
                   description: 'Version of ScyllaDB to run against. Can be a released version (2025.4) or a master (master:latest)',
                   name: 'scylla_version')
            string(defaultValue: '',
                   description: 'ScyllaDB packages repository (Debian/Ubuntu or RHEL-based). e.g. apt: http://downloads.scylladb.com/deb/debian/scylla-2025.4.list',
                   name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('unified_package', '')}",
                   description: 'Url to the unified package of scylla version to install scylla',
                   name: 'unified_package')
            booleanParam(defaultValue: "${pipelineParams.get('nonroot_offline_install', false)}",
                   description: 'Install Scylla without required root priviledge',
                   name: 'nonroot_offline_install')

            // Provisioning Configuration
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('instance_provision_fallback_on_demand', '')}",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            // Minicloud Configuration
            // Only has an effect when the jenkinsfile opts in with `minicloud: true`; see
            // vars/startMinicloud.groovy. Everything else minicloud needs - KMS off, a
            // KVM-capable instance_type_runner - is test-case configuration, not a job knob.
            separator(name: 'MINICLOUD_CONFIG', sectionHeader: 'Minicloud Configuration')
            string(defaultValue: "${pipelineParams.get('minicloud_docker', '')}",
                   description: 'Minicloud Docker image reference, e.g. ghcr.io/scylladb/minicloud:<tag>. ' +
                                'Empty leaves the image at its renovate-managed default (defaults/docker_images/minicloud/). ' +
                                'Ignored unless the jenkinsfile sets `minicloud: true`',
                   name: 'minicloud_docker')

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
            string(defaultValue: "${pipelineParams.get('post_behavior_vector_store_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_vector_store_nodes')
            string(defaultValue: "${pipelineParams.get('n_vector_store_nodes', '')}",
                   description: 'Number of Vector Search nodes to deploy.',
                   name: 'n_vector_store_nodes')

            // Cluster Reuse
            separator(name: 'CLUSTER_REUSE', sectionHeader: 'Cluster Reuse')
            string(defaultValue: '',
                   description: 'Test ID of an existing cluster to reuse. When set, provisioning is skipped and the existing cluster is used.',
                   name: 'reuse_cluster')

            // SSH Configuration
            separator(name: 'SSH_CONFIG', sectionHeader: 'SSH Configuration')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            // Manager Configuration
            separator(name: 'MANAGER_CONFIG', sectionHeader: 'Manager Configuration')
            string(defaultValue: "${pipelineParams.get('manager_version', '')}",
                   description: 'master_latest|3.11|3.10',
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
            booleanParam(defaultValue: false,
                   description: 'Build images for both architectures (ARM and x86). Useful for reusing BYO artifacts across multiple test runs.',
                   name: 'byo_build_both_arch')
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
                        // Opt in from the jenkinsfile with `minicloud: true`. Must come after
                        // extra_environment_variables is loaded (a per-run override wins) and
                        // before anything talks to a cloud API: env writes here are global for
                        // the rest of the build, so 'Provision Resources' and 'Run SCT Test'
                        // both reach minicloud instead of the real cloud.
                        startMinicloud.exportEnv(params, pipelineParams)
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
                                timeout(time: params.reuse_cluster ? 10 : 5, unit: 'MINUTES') {
                                    createSctRunner(params, runnerTimeout , builder.region)
                                }
                            }
                        }
                    }
                }
            }
            // Must run before 'Provision Resources': that stage calls the EC2/GCE API, which in
            // minicloud mode means localhost:5000, so the container has to be up first.
            //
            // Deliberately not wrapped in catchError: a minicloud that failed to start leaves
            // SCT_MINICLOUD_ENDPOINT_URL pointing at nothing, and 'Provision Resources' would
            // fall through to the real cloud and spend real money. Fail the build instead.
            stage('Start Minicloud') {
                when { expression { startMinicloud.active(pipelineParams) } }
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                timeout(time: 15, unit: 'MINUTES') {
                                    startMinicloud(params)
                                    completed_stages['start_minicloud'] = true
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
                                timeout(time: params.backend == 'azure' ? 90 : 30, unit: 'MINUTES') {
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
                                        runSctTest(params, builder.region, functional_test, pipelineParams)
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
                        echo "'provision_resources' stage is completed: $provision_resources"
                        echo "'run_tests' stage is completed: $run_tests"
                        echo "'collect_logs' stage is completed: $collect_logs"
                        echo "'clean_resources' stage is completed: $clean_resources"
                        echo "'send_email' stage is completed: $send_email"
                        echo "'clean_sct_runner' stage is completed: $clean_sct_runner"
                    """
                    if (!completed_stages['collect_logs']) {
                        catchError {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                        // ?: - collectLogsTimeout is still 0 if 'Get test duration'
                                        // never ran, and timeout(time: 0) aborts immediately.
                                        timeout(time: collectLogsTimeout ?: 30, unit: 'MINUTES') {
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
                                        timeout(time: resourceCleanupTimeout ?: 30, unit: 'MINUTES') {
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
