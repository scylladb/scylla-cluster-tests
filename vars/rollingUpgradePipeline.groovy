#!groovy

List supportedVersions = []
(testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, params.azure_region_name)

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
            string(defaultValue: '', description: 'AMI ID for ScyllaDB ', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'new_scylla_repo')

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

                                        ArrayList base_versions_list = params.base_versions.contains('.') ? params.base_versions.split('\\,') : []
                                        supportedVersions = supportedUpgradeFromVersions(
                                            base_versions_list,
                                            pipelineParams.linux_distro,
                                            params.new_scylla_repo,
                                            params.backend
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

                        for (version in supportedVersions) {
                            def base_version = version

                            tasks["${base_version}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                                        stage("Checkout for ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                timeout(time: 5, unit: 'MINUTES') {
                                                    script {
                                                        loadEnvFromString(params.extra_environment_variables)
                                                        wrap([$class: 'BuildUser']) {
                                                            dir('scylla-cluster-tests') {
                                                                checkout scm
                                                                checkoutQaInternal(params)
                                                            }
                                                        }
                                                        dockerLogin(params)
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
                                                                createArgusTestRun(params)
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
                                                        createSctRunner(params, runnerTimeout, builder.region)
                                                    }
                                                }
                                            }
                                        }
                                        stage("Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: testRunTimeout, unit: 'MINUTES') {
                                                        dir('scylla-cluster-tests') {
                                                            def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                            def cloud_provider = getCloudProviderFromBackend(params.backend)
                                                            sh """#!/bin/bash
                                                            set -xe
                                                            env

                                                            rm -fv ./latest

                                                            if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
                                                                export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
                                                            fi

                                                            export SCT_CLUSTER_BACKEND=${params.backend}

                                                            if [[ -n "${params.region ? params.region : ''}" ]] ; then
                                                                export SCT_REGION_NAME='${params.region}'
                                                            fi
                                                            if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
                                                                export SCT_GCE_DATACENTER=${params.gce_datacenter}
                                                            fi

                                                            export SCT_CONFIG_FILES=${test_config}
                                                            export SCT_SCYLLA_VERSION=${base_version}

                                                            if [[ ! -z "${params.new_scylla_repo}" ]]; then
                                                                export SCT_NEW_SCYLLA_REPO=${params.new_scylla_repo}
                                                            fi

                                                            if [[ ! -z "${params.azure_image_db}" ]]; then
                                                                export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"
                                                            fi
                                                            if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
                                                                export SCT_AZURE_REGION_NAME=${params.azure_region_name}
                                                            fi

                                                            if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
                                                                export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
                                                            fi

                                                            if [[ -n "${params.post_behavior_db_nodes ? params.post_behavior_db_nodes : ''}" ]] ; then
                                                                export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                            fi
                                                            if [[ -n "${params.post_behavior_loader_nodes ? params.post_behavior_loader_nodes : ''}" ]] ; then
                                                                export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                            fi
                                                            if [[ -n "${params.post_behavior_monitor_nodes ? params.post_behavior_monitor_nodes : ''}" ]] ; then
                                                                export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                                            fi
                                                            if [[ -n "${params.post_behavior_k8s_cluster ? params.post_behavior_k8s_cluster : ''}" ]] ; then
                                                                export SCT_POST_BEHAVIOR_K8S_CLUSTER="${params.post_behavior_k8s_cluster}"
                                                            fi
                                                            if [[ ${pipelineParams.use_preinstalled_scylla} != null ]] ; then
                                                                export SCT_USE_PREINSTALLED_SCYLLA="${pipelineParams.use_preinstalled_scylla}"
                                                            fi
                                                            if [[ -n "${params.provision_type ? params.provision_type : ''}" ]] ; then
                                                                export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                                            fi
                                                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )
                                                            if [[ ${pipelineParams.gce_image_db} != null ]] ; then
                                                                export SCT_GCE_IMAGE_DB=${pipelineParams.gce_image_db}
                                                            fi
                                                            if [[ ${pipelineParams.disable_raft} != null ]] ; then
                                                                export SCT_DISABLE_RAFT=${pipelineParams.disable_raft}
                                                            fi
                                                            export SCT_SCYLLA_LINUX_DISTRO=${pipelineParams.linux_distro}
                                                            export SCT_AMI_ID_DB_SCYLLA_DESC="\$SCT_AMI_ID_DB_SCYLLA_DESC-\$SCT_SCYLLA_LINUX_DISTRO"

                                                            if [[ ${pipelineParams.internode_compression} != null ]] ; then
                                                                export SCT_INTERNODE_COMPRESSION=${pipelineParams.internode_compression}
                                                            fi

                                                            echo "start test ......."
                                                            SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                                            if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
                                                                ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} run-test ${pipelineParams.test_name} --backend ${params.backend}
                                                            else
                                                                ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend ${params.backend}  --logdir "`pwd`"
                                                            fi
                                                            echo "end test ....."
                                                            """
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
                                                            runCollectLogs(params, builder.region)
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
                                                            runCleanupResource(params, builder.region)
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
                                        stage('Finish Argus Test Run') {
                                            catchError(stageResult: 'FAILURE') {
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
                        parallel tasks
                    }
                }
            }
        }
    }
}
