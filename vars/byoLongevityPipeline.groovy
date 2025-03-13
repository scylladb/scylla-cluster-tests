#!groovy


def call() {

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter)

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
		}

         parameters {

            string(defaultValue: 'git@github.com:scylladb/scylla-cluster-tests.git',
                   description: 'sct git repo',
                   name: 'sct_repo')

            string(defaultValue: 'master',
                   description: 'sct git branch',
                   name: 'sct_branch')

            string(defaultValue: "longevity_test.LongevityTest.test_custom_time",
                   description: '',
                   name: 'test_name')
            string(defaultValue: "test-cases/longevity/longevity-100gb-4h.yaml",
                   description: '',
                   name: 'test_config')
            string(defaultValue: "aws",
               description: 'aws|gce',
               name: 'backend')
            string(defaultValue: "eu-west-1",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "us-east1",
                   description: 'GCE datacenter supported: us-east1',
                   name: 'gce_datacenter')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')

            string(defaultValue: '', description: '', name: 'loader_ami_id')
            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: '', description: '', name: 'gce_image_db')
            string(defaultValue: '', description: '', name: 'scylla_version')
            string(defaultValue: '', description: '', name: 'scylla_repo')

            string(defaultValue: '',
                   description: "Which version to use for oracle cluster during gemini test",
                   name: "oracle_scylla_version")

            string(defaultValue: '', description: 'run gemini with specific seed number',
                   name: "gemini_seed")

            string(defaultValue: '',
                   description: 'If empty - the default scylla manager agent repo will be taken',
                   name: 'scylla_mgmt_agent_address')

            string(defaultValue: '',
                   description: 'If empty - the default manager version will be taken',
                   name: 'scylla_mgmt_address')

            string(defaultValue: '',
                   description: 'master_latest|3.1|3.0',
                   name: 'manager_version')

            string(defaultValue: "spot",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            string(defaultValue: "false",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            string(defaultValue: "private",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            string(defaultValue: "destroy",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')

            string(defaultValue: "destroy",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')

            string(defaultValue: "destroy",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            string(defaultValue: '360',
                   description: 'timeout for jenkins job in minutes',
                   name: 'timeout')

            string(defaultValue: '',
                   description: 'cloud path for RPMs, s3:// or gs://',
                   name: 'update_db_packages')
            string(defaultValue: "qa@scylladb.com",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            text(defaultValue: "",
                 description: (
                     'Extra environment variables to be set in the test environment, uses the java Properties File Format.\n' +
                     'Example:\n' +
                     '\tSCT_STRESS_IMAGE.cassandra-stress=scylladb/cassandra-stress:3.13.0\n' +
                     '\tSCT_USE_MGMT=false'
                     ),
                 name: 'extra_environment_variables')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            timeout([time: params.timeout, unit: "MINUTES"])
            buildDiscarder(logRotator(numToKeepStr: '5'))
        }
        stages {
            stage('Checkout') {
               steps {
                  script {
                      loadEnvFromString(params.extra_environment_variables)
                  }
                  dir('scylla-cluster-tests') {
                      git(url: params.sct_repo,
                            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
                            branch: params.sct_branch)

                      checkoutQaInternal(params)
                  }
                  dockerLogin(params)
               }
            }
            stage('Create SCT Runner') {
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                println(params)
                                println(builder)
                                createSctRunner(params, params.timeout.toInteger(), builder.region)
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
                                    runSctTest(params, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage('Collect log data') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runCollectLogs(params, builder.region)
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
                                    runCleanupResource(params, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage('Send email with result') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runSendEmail(params, currentBuild)
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
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
