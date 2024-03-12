#!groovy

// TODO: fix it by adding creation of SCT Runner
def call(Map pipelineParams) {

    def builder = getJenkinsLabels('gce', null)

    pipeline {
        agent {
            label {
                label builder.label
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
            SCT_CLUSTER_BACKEND   = 'k8s-local-kind-gce'
		}
        parameters {
            string(defaultValue: 'https://storage.googleapis.com/scylla-operator-charts/latest',
                   description: '',
                   name: 'k8s_scylla_operator_helm_repo')
            string(defaultValue: 'latest',
                   description: '',
                   name: 'k8s_scylla_operator_chart_version')
            string(defaultValue: '',
                   description: '',
                   name: 'k8s_scylla_operator_docker_image')
            string(defaultValue: '4.0.0',
                   description: '',
                   name: 'scylla_version')
            string(defaultValue: '2.0.2',
                   description: '',
                   name: 'scylla_mgmt_agent_version')
            string(defaultValue: "${pipelineParams.get('k8s_enable_tls', '')}",
                   description: 'if true, enable operator tls feature',
                   name: 'k8s_enable_tls')
            string(defaultValue: "${pipelineParams.get('k8s_enable_sni', '')}",
                   description: 'if true, install haproxy ingress controller and use it',
                   name: 'k8s_enable_sni')
            string(defaultValue: "${pipelineParams.get('test_name', 'longevity_test.LongevityTest.test_custom_time')}",
                   description: '',
                   name: 'test_name')
            string(defaultValue: "${pipelineParams.get('test_config', 'test-cases/scylla-operator/longevity-scylla-operator-3h.yaml')}",
                   description: '',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_k8s_cluster', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_k8s_cluster')
            string(defaultValue: "qa@scylladb.com",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout(pipelineParams.timeout)
            buildDiscarder(logRotator(numToKeepStr: "${pipelineParams.get('builds_to_keep', '20')}",))
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
                    dir('scylla-cluster-tests') {
                        checkout scm
                    }
                }
            }
            stage('Run SCT Test') {
                steps {
                    script {
                        def test_config = groovy.json.JsonOutput.toJson(params.test_config)

                        sctScript """
                            rm -fv ./latest

                            export SCT_CONFIG_FILES=${test_config}

                            if [[ -n "${params.k8s_scylla_operator_helm_repo ? params.k8s_scylla_operator_helm_repo : ''}" ]] ; then
                                export SCT_K8S_SCYLLA_OPERATOR_HELM_REPO=${params.k8s_scylla_operator_helm_repo}
                            fi
                            if [[ -n "${params.k8s_scylla_operator_docker_image ? params.k8s_scylla_operator_docker_image : ''}" ]] ; then
                                export SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE=${params.k8s_scylla_operator_docker_image}
                            fi

                            if [[ -n "${params.k8s_scylla_operator_chart_version}" ]]; then
                                export SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION=${params.k8s_scylla_operator_chart_version}
                            fi
                            export SCT_SCYLLA_VERSION=${params.scylla_version}
                            export SCT_SCYLLA_MGMT_AGENT_VERSION=${params.scylla_mgmt_agent_version}

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

                            echo "start test ......."
                            ./docker/env/hydra.sh run-test ${params.test_name} --logdir "`pwd`"
                            echo "end test ....."
                        """
                    }
                }
            }
            stage('Collect log data') {
                steps {
                    script {
                        def test_config = groovy.json.JsonOutput.toJson(params.test_config)

                        sctScript """
                            export SCT_CONFIG_FILES=${test_config}

                            echo "start collect logs ..."
                            ./docker/env/hydra.sh collect-logs --logdir "`pwd`"
                            echo "end collect logs"
                        """
                    }
                }
            }
            stage('Clean resources') {
                steps {
                    script {
                        def test_config = groovy.json.JsonOutput.toJson(params.test_config)

                        sctScript """
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
                            export SCT_CLUSTER_BACKEND="${params.backend}"

                            echo "start clean resources ..."
                            ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
                            echo "end clean resources"
                        """
                    }
                }
            }
            stage('Send email with result') {
                steps {
                    script {
                        def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)

                        sctScript """
                            echo "Start send email ..."
                            ./docker/env/hydra.sh send-email --logdir "`pwd`" --email-recipients "${email_recipients}"
                            echo "Email sent"
                        """
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
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
