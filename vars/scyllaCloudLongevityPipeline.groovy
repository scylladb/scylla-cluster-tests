#!groovy

def call(Map pipelineParams) {
    pipeline {
        agent {
            label {
                label "aws-eu-west1-qa-builder1"
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
		}
        parameters {
            string(defaultValue: 'i3.large',
                   description: 'any type support by scylla cloud',
                   name: 'db_instance_type')

            string(defaultValue: "${pipelineParams.get('n_db_nodes', '3')}",
                   description: 'any type support by scylla cloud',
                   name: 'n_db_nodes')

            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'spot_low_price|on_demand|spot_fleet|spot_low_price|spot_duration',
                   name: 'provision_type')

            string(defaultValue: "${pipelineParams.get('post_behaviour', 'destroy')}",
                   description: 'keep|destroy',
                   name: 'post_behaviour')


        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout(pipelineParams.timeout)
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }

        stages {
            stage('Checkout') {
               steps {
                  dir("siren-tests") {
                    git(url: 'git@github.com:scylladb/siren-tests.git',
                      credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                      branch: 'master')
                  }
                  dir('scylla-cluster-tests') {
                      checkout scm
                  }
               }
            }
            stage('Create Cluster with siren-tests') {
                steps {
                        dir('siren-tests') {
                            sh """
                            #!/bin/bash
                            set -xe

                            export SIRENADA_BROWSER=chrome-headless

                            source /opt/rh/rh-python35/enable
                            # update the environment
                            ~/.local/bin/pipenv --bare install

                            export SIRENADA_REGION=eu-west-1
                            export SIRENADA_NUMBER_OF_NODES=${params.n_db_nodes}
                            export SIRENADA_CLUSTER_KEEP=true
                            export SIRENADA_INSTANCE_TYPE=${params.db_instance_type}

                            ~/.local/bin/pipenv run ./runtests.py --sct-conf
                            """
                        }
                }
            }
            stage('Run SCT Test') {
                steps {
                    wrap([$class: 'BuildUser']) {
                        dir('scylla-cluster-tests') {
                            sh """
                            #!/bin/bash
                            set -xe
                            env

                            export SCT_CLUSTER_BACKEND=aws-siren
                            export SCT_INTRA_NODE_COMM_PUBLIC=true
                            export SCT_REGION_NAME=eu-west-1
                            export SCT_CONFIG_FILES="['${pipelineParams.test_config}', '`realpath ../siren-tests/test_results/scylla_cloud.yaml`']"

                            export SCT_FAILURE_POST_BEHAVIOR=${pipelineParams.params.get('post_behaviour', '')}
                            export SCT_INSTANCE_PROVISION=${pipelineParams.params.get('provision_type', '')}
                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                            echo "start test ......."
                            ./docker/env/hydra.sh run-test ${pipelineParams.test_name} --backend aws-siren --logdir /sct
                            echo "end test ....."
                           """
                        }
                    }
                }
            }
        }
        post {
            always {
                script {
                    if (pipelineParams.params.post_behaviour == 'destroy') {
                        dir('siren-tests') {
                                sh '''
                                #!/bin/bash
                                set -xe
                                source /opt/rh/rh-python35/enable
                                ~/.local/bin/pipenv run ./runtests.py --untag-cluster=test_results/cluster_id.json
                                '''
                            }
                    }
                }
            }
        }
    }
}
