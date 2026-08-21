#!groovy

List supportedVersions = []
def params_mapping = [:] // this would hold the params per split of this pipeline
def completed_stages = [:]
(testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {
    // Captured locals - see the same block in longevityPipeline for why these come from
    // pipelineParams and must never be read back off `params` in a guard.
    def minicloudEnabled = pipelineParams.get('minicloud', false)
    // Rolling upgrade keeps both topologies, like longevity: a nested-virtualization cloud
    // sct-runner by default, a KVM-capable Jenkins agent with `local_agent: true`.
    // A build parameter as well as a jenkinsfile knob, so an existing job can be moved to a lab
    // agent for one run without a new jenkinsfile: tick `local_agent` and the KVM label is
    // resolved for you. Read off `params` first - it is populated from build #2 onward, which is
    // where a per-build choice can exist at all - and it falls back to the jenkinsfile default on
    // build #1, the params-loading build these pipelines abort anyway. Safe to read here, unlike
    // params.minicloud: getJenkinsLabels only ever merges a `minicloud` key into the global
    // binding, never this one.
    // Gated on minicloudEnabled: without minicloud there is nothing to run on a KVM agent, and
    // an ungated `local_agent` on a normal build would skip the runner stages while the label
    // stayed the ordinary cloud builder - the test would then run on the builder itself.
    def localAgent = minicloudEnabled && (params.local_agent != null
                                          ? params.local_agent.toString().toBoolean()
                                          : pipelineParams.get('local_agent', false))

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter,
                                   params.azure_region_name, params.oci_region_name,
                                   (minicloudEnabled && localAgent) ? [minicloud: true] : null)

    // since this is a boolean param, we need to handle its default value upfront, we can't do it in the parameters section
    // we'll keep it as boolean to simplify its usage later on
    def base_version_all_sts_versions = pipelineParams.get('base_version_all_sts_versions', false)

    pipeline {
        agent none

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_GCE_PROJECT = "${params.gce_project}"
            SCT_BILLING_PROJECT = "${params.billing_project}"
        }
        parameters {
            separator(name: 'CLOUD_PROVIDER', sectionHeader: 'Cloud Provider Configuration')
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
               description: 'aws|gce|azure',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | us-east-2 | us-west-2 | eu-west-1 | eu-west-2 | eu-west-3 | eu-north-1 | eu-central-1 | ca-central-1 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure Cloud region',
                   name: 'azure_region_name')
            string(defaultValue: "${pipelineParams.get('oci_region_name', 'us-phoenix-1')}",
                   description: 'Oracle Cloud region',
                   name: 'oci_region_name')
            string(defaultValue: "",
               description: 'Availability zone',
               name: 'availability_zone')
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection')
            string(defaultValue: "${pipelineParams.get('scylla_ami_id', '')}", description: 'AMI ID for ScyllaDB ', name: 'scylla_ami_id')
            string(defaultValue: "${pipelineParams.get('gce_image_db', '')}", description: 'GCE image for ScyllaDB ', name: 'gce_image_db')
            string(defaultValue: "${pipelineParams.get('azure_image_db', '')}", description: 'Azure image for ScyllaDB ', name: 'azure_image_db')
            string(defaultValue: "${pipelineParams.get('oci_image_db', '')}", description: 'Oracle image for ScyllaDB ', name: 'oci_image_db')

            string(defaultValue: '',
                   description: 'ScyllaDB packages repository (Debian/Ubuntu or RHEL-based). e.g. apt: http://downloads.scylladb.com/deb/debian/scylla-2025.4.list',
                   name: 'new_scylla_repo')
            booleanParam(defaultValue: base_version_all_sts_versions,
                         description: 'Whether to include all supported STS versions as base versions',
                         name: 'base_version_all_sts_versions')
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'on_demand|spot_fleet|spot',
                   name: 'provision_type')
            // Minicloud Configuration
            // Only has an effect when the jenkinsfile opts in with `minicloud: true`; see
            // vars/startMinicloud.groovy. Everything else minicloud needs - KMS off, a
            // KVM-capable instance_type_runner - is test-case configuration, not a job knob.
            separator(name: 'MINICLOUD_CONFIG', sectionHeader: 'Minicloud Configuration')
            booleanParam(defaultValue: "${pipelineParams.get('local_agent', false)}",
                   description: 'Run minicloud on a KVM-capable Jenkins agent instead of ' +
                                'provisioning an sct-runner. Needs an agent serving the ' +
                                'minicloud label (or set jenkins_label). ' +
                                'Ignored unless the jenkinsfile sets `minicloud: true`',
                   name: 'local_agent')
            string(defaultValue: "${pipelineParams.get('minicloud_docker', '')}",
                   description: 'Minicloud Docker image reference, e.g. ghcr.io/scylladb/minicloud:<tag>. ' +
                                'Empty leaves the image at its renovate-managed default (defaults/docker_images/minicloud/). ' +
                                'Ignored unless the jenkinsfile sets `minicloud: true`',
                   name: 'minicloud_docker')
            string(defaultValue: "${pipelineParams.get('minicloud_lightweight_memory', '')}",
                   description: 'RAM per emulated guest, e.g. 6GiB. Empty keeps the test-case/defaults value. ' +
                                'Multiplies across every guest in the test, so the agent or sct-runner has to fit the product',
                   name: 'minicloud_lightweight_memory')
            string(defaultValue: "${pipelineParams.get('minicloud_lightweight_vcpus', '')}",
                   description: 'vCPUs per emulated guest (one Scylla shard each). Empty keeps the test-case/defaults value',
                   name: 'minicloud_lightweight_vcpus')
            string(defaultValue: "${pipelineParams.get('minicloud_container_memory', '')}",
                   description: 'Cap the minicloud container itself, e.g. 32GiB. Empty means no docker limit. ' +
                                'When set, this is also the budget the preflight guest-memory gate measures against',
                   name: 'minicloud_container_memory')
            string(defaultValue: '',
                   description: 'Pin this build to a specific Jenkins agent label, e.g. a lab machine. ' +
                                'Empty picks the usual builder for the backend/region',
                   name: 'jenkins_label')
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
            // Cluster Reuse
            separator(name: 'CLUSTER_REUSE', sectionHeader: 'Cluster Reuse')
            string(defaultValue: '',
                   description: 'Test ID of an existing cluster to reuse. When set, provisioning is skipped and the existing cluster is used.',
                   name: 'reuse_cluster')
            separator(name: 'EMAIL_TEST', sectionHeader: 'Email and Test Configuration')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            string(defaultValue: "${pipelineParams.get('billing_project', '')}",
                   description: 'Billing project for the test run',
                   name: 'billing_project')
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
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
                   description: 'Extra environment variables to inject (format: KEY1=VAL1\nKEY2=VAL2)',
                   name: 'extra_environment_variables')

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
                                    // tag only when not running on a local minicloud agent:
                                    // local agents have no IMDS, so tagBuilder() would fail there.
                                    if (!(minicloudEnabled && localAgent)) {
                                        tagBuilder()
                                    }
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
                                                                // Opt in from the jenkinsfile with `minicloud: true`. Must come
                                                                // after extra_environment_variables is loaded (a per-run override
                                                                // wins) and before anything talks to a cloud API.
                                                                startMinicloud.exportEnv(params, pipelineParams)
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
                                                if (minicloudEnabled && localAgent) {
                                                    stage("Minicloud Agent Preflight for ${base_version}") {
                                                        dir('scylla-cluster-tests') {
                                                            timeout(time: 10, unit: 'MINUTES') {
                                                                minicloudReclaim()
                                                                minicloudPreflight()
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
                                                // On a local agent the test runs right here, so there is
                                                // nothing to provision - and minicloudReclaim is then the
                                                // only thing clearing a stale ./sct_runner_ip out of a
                                                // persistent workspace.
                                                if (!localAgent) {
                                                    stage("Create SCT Runner for ${base_version}") {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 5, unit: 'MINUTES') {
                                                                    createSctRunner(params_mapping[base_version], runnerTimeout, builder.region)
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                // Must run before 'Provision Resources': that stage calls the
                                                // EC2/GCE API, which in minicloud mode means localhost:5000 on
                                                // the runner, so the container has to be up first.
                                                if (startMinicloud.active(pipelineParams)) {
                                                    stage("Start Minicloud for ${base_version}") {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 10, unit: 'MINUTES') {
                                                                    startMinicloud(params_mapping[base_version])
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                stage("Provision Resources for ${base_version}") {
                                                    script {
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 30, unit: 'MINUTES') {
                                                                    if (params.backend == 'aws' || params.backend == 'azure' || params.backend == 'gce' || params.backend == 'oci') {
                                                                        provisionResources(params_mapping[base_version], builder.region)
                                                                    } else {
                                                                        sh """
                                                                            echo 'Skipping because non-AWS/Azure/GCE backends are not supported'
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
                                                                    completed_stages[base_version]['collect_logs'] = true
                                                                    runCollectLogs(params_mapping[base_version], builder.region)
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
                                                stage("Send email for Upgrade from ${base_version}") {
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
                                                // Nothing was provisioned on a local agent, so there is
                                                // nothing to reclaim.
                                                if (!localAgent) {
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
                                                if (!completed_stages[base_version]['collect_logs']) {
                                                    catchError {
                                                        script {
                                                            wrap([$class: 'BuildUser']) {
                                                                dir('scylla-cluster-tests') {
                                                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                                                        runCollectLogs(params_mapping[base_version], builder.region)
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
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
                                                // `!localAgent` and not just the completed_stages check: the
                                                // stage above is skipped on a local agent, which would leave
                                                // this recovery path hunting for a runner that never existed.
                                                if (!localAgent && !completed_stages[base_version]['clean_sct_runner']) {
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
                                                // Deliberately the last teardown step: `docker rm -f` kills
                                                // every guest with the container, and the log collection
                                                // above needs them alive.
                                                if (minicloudEnabled) {
                                                    catchError {
                                                        script {
                                                            dir('scylla-cluster-tests') {
                                                                timeout(time: 10, unit: 'MINUTES') {
                                                                    stopMinicloud(params_mapping[base_version], currentBuild)
                                                                    // Leave the agent clean for
                                                                    // the next job on it.
                                                                    minicloudReclaim(atEnd: true)
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
