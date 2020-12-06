#!groovy

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
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
               description: 'aws|gce',
               name: 'backend')

            string(defaultValue: "${pipelineParams.get('aws_region', 'eu-west-1')}",
               description: 'Supported: us-east-1|eu-west-1|eu-west-2|eu-north-1|random (randomly select region)',
               name: 'aws_region')
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
            booleanParam(defaultValue: "${pipelineParams.get('workaround_kernel_bug_for_iotune', false)}",
                 description: 'Workaround a known kernel bug which causes iotune to fail in scylla_io_setup, only effect GCE backend',
                 name: 'workaround_kernel_bug_for_iotune')
            string(defaultValue: '', description: 'scylla option: internode_compression', name: 'internode_compression')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout(pipelineParams.timeout)
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]

                        for (version in supportedUpgradeFromVersions(env.GIT_BRANCH, pipelineParams.base_versions)) {
                            def base_version = version
                            tasks["${base_version}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",]) {

                                        stage("Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        checkout scm
                                                        def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                        sh """
                                                        #!/bin/bash
                                                        set -xe
                                                        env

                                                        rm -fv ./latest

                                                        export SCT_CLUSTER_BACKEND=${params.backend}

                                                        export SCT_CONFIG_FILES=${test_config}
                                                        export SCT_SCYLLA_VERSION=${base_version}
                                                        export SCT_NEW_SCYLLA_REPO=${params.new_scylla_repo}

                                                        export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                        export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                        export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
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
                                                        ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend ${params.backend}  --logdir "`pwd`"
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
                                                        def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                        sh """
                                                        #!/bin/bash

                                                        set -xe
                                                        env

                                                        export SCT_CLUSTER_BACKEND=${params.backend}
                                                        export SCT_CONFIG_FILES=${test_config}

                                                        echo "start collect logs ..."
                                                        ./docker/env/hydra.sh collect-logs --logdir "`pwd`" --backend ${params.backend}
                                                        echo "end collect logs"
                                                        """
                                                    }
                                                }
                                            }
                                        }
                                        stage("Clean resources for Upgrade from ${base_version}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        sh """
                                                        #!/bin/bash

                                                        set -xe
                                                        env

                                                        export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                        export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                        export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"

                                                        echo "start clean resources ..."
                                                        ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
                                                        echo "end clean resources"
                                                        """
                                                    }
                                                }
                                            }
                                        }
                                        stage("Send email for Upgrade from ${base_version}") {
                                            def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        sh """
                                                        #!/bin/bash

                                                        set -xe
                                                        env

                                                        echo "Start send email ..."
                                                        ./docker/env/hydra.sh send-email --logdir "`pwd`" --email-recipients "${email_recipients}"
                                                        echo "Email sent"
                                                        """
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
