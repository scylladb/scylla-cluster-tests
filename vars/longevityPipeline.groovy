#!groovy

def call(Map pipelineParams) {

    pipeline {
        agent {
            label {
                label getJenkinsLabels(params.backend, params.aws_region)
            }
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
                  dir('scylla-cluster-tests') {
                      checkout scm
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
                            export SCT_NEW_CONFIG=yes
                            export SCT_CLUSTER_BACKEND=${params.backend}
                            export SCT_REGION_NAME=${params.aws_region}
                            export SCT_CONFIG_FILES=${pipelineParams.test_config}

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

                            export SCT_FAILURE_POST_BEHAVIOR=${pipelineParams.params.get('post_behaviour', '')}
                            export SCT_INSTANCE_PROVISION=${pipelineParams.params.get('provision_type', '')}
                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                            echo "start avocado ......."
                            ./docker/env/hydra.sh run ${pipelineParams.test_name} --xunit /sct/results.xml --job-results-dir /sct --show-job-log
                            echo "end avocado ....."
                            """
                        }
                    }
                }
            }
        }
    }

}
