#!groovy

def call(Map pipelineParams) {
    pipeline {
        agent {
            label {
                label getJenkinsLabels(params.backend, pipelineParams.aws_region)
            }
        }
         parameters {
            string(defaultValue: '', description: '', name: 'new_scylla_repo')

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
            stage('Run SCT Test') {
                steps {
                    script {
                        def tasks = [:]

                        for (version in pipelineParams.base_versions) {
                            def base_version = version;
                            tasks["${base_version}"] = {
                                node(getJenkinsLabels(params.backend, pipelineParams.aws_region)){

                                    wrap([$class: 'BuildUser']) {
                                        dir('scylla-cluster-tests') {
                                            git(url: "${pipelineParams.sct_repo}",
                                              credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                                              branch: "${pipelineParams.sct_branch}")

                                            sh """
                                            #!/bin/bash
                                            set -xe
                                            env
                                            export SCT_NEW_CONFIG=yes
                                            export SCT_CLUSTER_BACKEND=gce

                                            export SCT_CONFIG_FILES=${pipelineParams.test_config}
                                            export SCT_SCYLLA_VERSION=${base_version}
                                            export SCT_NEW_SCYLLA_REPO=${pipelineParams.params.new_scylla_repo}

                                            export SCT_FAILURE_POST_BEHAVIOR=${pipelineParams.params.get('post_behaviour', '')}
                                            export SCT_INSTANCE_PROVISION=${pipelineParams.params.get('provision_type', '')}
                                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                            export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                            export SCT_GCE_IMAGE_DB=${pipelineParams.gce_image_db}
                                            export SCT_SCYLLA_LINUX_DISTRO=${pipelineParams.linux_distro}

                                            echo "start avocado ......."
                                            ./docker/env/hydra.sh run ${pipelineParams.test_name} --xunit /sct/results.xml --job-results-dir /sct --show-job-log
                                            echo "end avocado ....."
                                            """
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
