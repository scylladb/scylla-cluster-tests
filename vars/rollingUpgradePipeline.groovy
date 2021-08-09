#!groovy

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.aws_region, params.gce_datacenter)

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
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('aws_region', 'eu-west-1')}",
               description: 'Supported: us-east-1|eu-west-1|eu-west-2|eu-north-1|random (randomly select region)',
               name: 'aws_region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'new_scylla_repo')

            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration',
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
            booleanParam(defaultValue: "${pipelineParams.get('workaround_kernel_bug_for_iotune', false)}",
                 description: 'Workaround a known kernel bug which causes iotune to fail in scylla_io_setup, only effect GCE backend',
                 name: 'workaround_kernel_bug_for_iotune')
            string(defaultValue: '', description: 'scylla option: internode_compression', name: 'internode_compression')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout(pipelineParams.timeout)
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
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]

                        for (version in supportedUpgradeFromVersions(pipelineParams.base_versions, params.new_scylla_repo)) {
                            def base_version = version
                            tasks["${base_version}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {

                                        stage("Create SCT Runner for ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        checkout scm
                                                        timeout(time: 5, unit: 'MINUTES') {
                                                            createSctRunner(params, pipelineParams.timeout.time, builder.region)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        stage("Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                        def cloud_provider = getCloudProviderFromBackend(params.backend)
                                                        sh """
                                                        #!/bin/bash
                                                        set -xe
                                                        env

                                                        rm -fv ./latest

                                                        export SCT_CLUSTER_BACKEND=${params.backend}

                                                        if [[ -n "${params.aws_region ? params.aws_region : ''}" ]] ; then
                                                            export SCT_REGION_NAME=${params.aws_region}
                                                        fi
                                                        if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
                                                            export SCT_GCE_DATACENTER=${params.gce_datacenter}
                                                        fi

                                                        export SCT_CONFIG_FILES=${test_config}
                                                        export SCT_SCYLLA_VERSION=${base_version}
                                                        export SCT_NEW_SCYLLA_REPO=${params.new_scylla_repo}

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
                                                        export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                                        export SCT_GCE_IMAGE_DB=${pipelineParams.gce_image_db}
                                                        export SCT_SCYLLA_LINUX_DISTRO=${pipelineParams.linux_distro}
                                                        export SCT_AMI_ID_DB_SCYLLA_DESC="\$SCT_AMI_ID_DB_SCYLLA_DESC-\$SCT_SCYLLA_LINUX_DISTRO"

                                                        export SCT_WORKAROUND_KERNEL_BUG_FOR_IOTUNE=${pipelineParams.workaround_kernel_bug_for_iotune}
                                                        if [[ ${pipelineParams.internode_compression} != null ]] ; then
                                                            export SCT_INTERNODE_COMPRESSION=${pipelineParams.internode_compression}
                                                        fi

                                                        echo "start test ......."
                                                        if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" ]]; then
                                                            SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                                            if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
                                                                ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} run-test ${pipelineParams.test_name} --backend ${params.backend}
                                                            else
                                                                echo "SCT runner IP file is empty. Probably SCT Runner was not created."
                                                                exit 1
                                                            fi
                                                        else
                                                            ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend ${params.backend}  --logdir "`pwd`"
                                                        fi
                                                        echo "end test ....."
                                                        """
                                                    }
                                                }
                                            }
                                        }
                                        stage("Collect logs for Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        runCollectLogs(params, builder.region)
                                                    }
                                                }
                                            }
                                        }
                                        stage("Clean resources for Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        timeout(time: 10, unit: 'MINUTES') {
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
