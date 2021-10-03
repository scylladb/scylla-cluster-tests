#!groovy
import groovy.json.JsonSlurper

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
               description: 'us-east-1|eu-west-1',
               name: 'aws_region')

            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('mgmt_agent_repo', 'http://downloads.scylladb.com/manager/deb/unstable/focal/branch-2.3/latest/scylladb-manager-2.3/scylla-manager.list')}",
                   description: 'default manager agent repo URL (on master it is on Ubuntu)',
                   name: 'mgmt_agent_repo')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration|spot',
                   name: 'provision_type')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'destroy')}",
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

            string(defaultValue: "${pipelineParams.get('email_recipients', 'scylla-perf-results@scylladb.com')}",
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
            stage('Run SCT Performance Tests') {
                steps {
                    script {
                        def tasks = [:]
                        def sub_tests
                        if (params.sub_tests) {
                            sub_tests = new JsonSlurper().parseText(params.sub_tests)
                        } else {
                            sub_tests = [pipelineParams.test_name]
                        }
                        for (t in sub_tests) {
                            def perf_test
                            def sub_test = t
                            if (sub_test == pipelineParams.test_name) {
                                perf_test = sub_test
                            } else {
                                perf_test = "${pipelineParams.test_name}.${sub_test}"
                            }

                            tasks["sub_test=${sub_test}"] = {
                                node(builder.label) {
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                                 "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",]) {
                                        stage("Run ${sub_test}"){
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)
                                                    dir('scylla-cluster-tests') {
                                                        checkout scm

                                                        sh """
                                                        #!/bin/bash
                                                        set -xe
                                                        env

                                                        rm -fv ./latest

                                                        export SCT_CLUSTER_BACKEND=${params.backend}
                                                        if [[ -n "${params.aws_region}" ]]; then
                                                            export SCT_REGION_NAME=${params.aws_region}
                                                        else
                                                            export SCT_REGION_NAME=${pipelineParams.aws_region}
                                                        fi
                                                        export SCT_CONFIG_FILES=${pipelineParams.test_config}
                                                        export SCT_SCYLLA_MGMT_AGENT_REPO=${pipelineParams.mgmt_agent_repo}
                                                        export SCT_EMAIL_RECIPIENTS="${email_recipients}"
                                                        if [[ ! -z "${params.scylla_ami_id}" ]] ; then
                                                            export SCT_AMI_ID_DB_SCYLLA=${params.scylla_ami_id}
                                                        elif [[ ! -z "${params.scylla_version}" ]] ; then
                                                            export SCT_SCYLLA_VERSION=${params.scylla_version}
                                                        elif [[ ! -z "${params.scylla_repo}" ]] ; then
                                                            export SCT_SCYLLA_REPO=${params.scylla_repo}
                                                        else
                                                            echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO"
                                                            exit 1
                                                        fi


                                                        export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                        export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                                        export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                                        export SCT_INSTANCE_PROVISION=${params.provision_type}
                                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                                        echo "start test ......."
                                                        ./docker/env/hydra.sh run-test ${perf_test} --backend ${params.backend}  --logdir "`pwd`"
                                                        echo "end test ....."
                                                        """
                                                    }
                                                }
                                            }
                                        }
                                        stage("Collect logs for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)
                                                        sh """
                                                        #!/bin/bash

                                                        set -xe
                                                        env

                                                        export SCT_CLUSTER_BACKEND="${params.backend}"
                                                        export SCT_REGION_NAME=${aws_region}
                                                        export SCT_CONFIG_FILES=${test_config}

                                                        echo "start collect logs ..."
                                                        ./docker/env/hydra.sh collect-logs --logdir "`pwd`"
                                                        echo "end collect logs"
                                                        """
                                                    }
                                                }
                                            }
                                        }
                                        stage("Clean resources for ${sub_test}") {
                                            catchError(stageResult: 'FAILURE') {
                                                script {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            def aws_region = groovy.json.JsonOutput.toJson(params.aws_region)
                                                            def test_config = groovy.json.JsonOutput.toJson(pipelineParams.test_config)

                                                            sh """
                                                            #!/bin/bash

                                                            set -xe
                                                            env

                                                            export SCT_CONFIG_FILES=${test_config}
                                                            export SCT_CLUSTER_BACKEND="${params.backend}"
                                                            export SCT_REGION_NAME=${aws_region}
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
