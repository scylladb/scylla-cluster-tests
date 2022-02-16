#! groovy

def call(Map pipelineParams) {
    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, params.azure_region_name)

    pipeline {
        agent {
            label {
                label 'built-in'
            }
        }
        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }
        parameters {
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
                   description: 'aws|gce|azure|docker',
                   name: 'backend')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against (for .rpm/.deb tests, should be blank otherwise)',
                   name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('unified_package', '')}",
                   description: 'Url to the unified package of scylla version to install scylla',
                   name: 'unified_package')
            booleanParam(defaultValue: "${pipelineParams.get('nonroot_offline_install', false)}",
                   description: 'Install Scylla without required root priviledge',
                   name: 'nonroot_offline_install')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_address', '')}",
                   description: 'a Scylla Manager repo to run against (for .rpm/.deb tests, should be blank otherwise)',
                   name: 'scylla_mgmt_address')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_address', '')}",
                   description: 'manager agent repo',
                   name: 'scylla_mgmt_agent_address')
            string(defaultValue: "${pipelineParams.get('manager_version', '')}",
                   description: 'master_latest|2.6|2.5|2.4|2.3',
                   name: 'manager_version')
            string(defaultValue: '',
                   description: 'a Scylla AMI to run against (for AMI test, should be blank otherwise)',
                   name: 'scylla_ami_id')
            string(defaultValue: '',
                   description: 'a GCE Image to run against',
                   name: 'gce_image_db')
            string(defaultValue: '',
                   description: 'a Azure Image to run against',
                   name: 'azure_image_db')
            string(defaultValue: "${pipelineParams.get('region', '')}",
                   description: 'AWS region with Scylla AMI (for AMI test, ignored otherwise)',
                   name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
           string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure location',
                   name: 'azure_region_name')
            string(defaultValue: '',
                   description: "a Scylla docker image to run against (for docker backend.) Should be `scylladb/scylla' for official images",
                   name: 'scylla_docker_image')
            string(defaultValue: '',
                   description: 'a Scylla version to run against (mostly for docker backend)',
                   name: 'scylla_version')
            string(defaultValue: "${pipelineParams.get('instance_type', '')}",
                   description: 'a cloud instance type (leave blank for test case defaults)',
                   name: 'instance_type')
            string(defaultValue: "${pipelineParams.get('test_config', 'test-cases/artifacts/centos7.yaml')}",
                   description: 'a config file for the artifacts test',
                   name: 'test_config')
            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot_low_price')}",
                   description: 'on_demand|spot_low_price|spot',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: "${pipelineParams.get('availability_zone', 'a')}",
               description: 'Availability zone',
               name: 'availability_zone')

        }
        options {
            timestamps()
            // Timeout for the whole test, add 30 MINUTES for waiting the builder
            timeout([time: pipelineParams.timeout.time + 30, unit: 'MINUTES'])
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
            stage('Run SCT stages') {
                steps {
                    script {
                        def tasks = [:]
                        params.instance_type.split(' ').each {
                            def instance_type = it
                            tasks["${instance_type}"] = {
                                node(builder.label) {
                                    // Timeout for the test itself
                                    timeout(pipelineParams.timeout) {
                                        withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                                 "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                                 "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {

                                            def test_config = groovy.json.JsonOutput.toJson(params.test_config)
                                            stage("Checkout (${instance_type})") {
                                                dir('scylla-cluster-tests') {
                                                    checkout scm
                                                }
                                            }
                                            stage("Run SCT Test (${instance_type})") {
                                                def cloud_provider = getCloudProviderFromBackend(params.backend)
                                                sctScript """
                                                    rm -fv ./latest

                                                    # clean the old sct_runner_ip file
                                                    rm -fv ./sct_runner_ip

                                                    export SCT_COLLECT_LOGS=false
                                                    export SCT_CONFIG_FILES=${test_config}

                                                    if [[ ! -z "${params.scylla_ami_id}" ]]; then
                                                        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
                                                        export SCT_REGION_NAME="${params.region}"
                                                    elif [[ ! -z "${params.gce_image_db}" ]]; then
                                                        export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
                                                        if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
                                                            export SCT_GCE_DATACENTER=${params.gce_datacenter}
                                                        fi
                                                    elif [[ ! -z "${params.azure_image_db}" ]]; then
                                                        export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"
                                                        if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
                                                            export SCT_AZURE_REGION_NAME=${params.azure_region_name}
                                                        fi
                                                    elif [[ ! -z "${params.scylla_version}" ]]; then
                                                        export SCT_SCYLLA_VERSION="${params.scylla_version}"
                                                    elif [[ ! -z "${params.scylla_repo}" ]]; then
                                                        export SCT_SCYLLA_REPO="${params.scylla_repo}"
                                                    elif [[ ! -z "${params.unified_package}" ]]; then
                                                        export SCT_UNIFIED_PACKAGE="${params.unified_package}"
                                                        export SCT_NONROOT_OFFLINE_INSTALL=${params.nonroot_offline_install}
                                                        export SCT_USE_MGMT=false
                                                    else
                                                        echo "need to choose one of SCT_AZURE_IMAGE_DB | SCT_GCE_IMAGE_DB | SCT_AMI_ID_DB_SCYLLA | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO | SCT_UNIFIED_PACKAGE"
                                                        exit 1
                                                    fi

                                                    if [[ ! -z "${params.scylla_mgmt_address}" && -z "${params.unified_package}" ]]; then
                                                        export SCT_USE_MGMT=true
                                                        export SCT_SCYLLA_REPO_M="${params.scylla_repo}"
                                                        export SCT_SCYLLA_MGMT_ADDRESS="${params.scylla_mgmt_address}"
                                                    fi

                                                    if [[ ! -z "${params.manager_version}" && -z "${params.unified_package}" ]]; then
                                                        export SCT_USE_MGMT=true
                                                        export SCT_SCYLLA_REPO_M="${params.scylla_repo}"
                                                        export SCT_MANAGER_VERSION="${params.manager_version}"
                                                    fi

                                                    if [[ ! -z "${params.scylla_mgmt_agent_address}" ]] ; then
                                                        export SCT_SCYLLA_MGMT_AGENT_ADDRESS="${params.scylla_mgmt_agent_address}"
                                                    fi

                                                    if [[ ! -z "${params.scylla_docker_image}" ]]; then
                                                        export SCT_DOCKER_IMAGE="${params.scylla_docker_image}"
                                                        if [[ -z "${params.scylla_version}" ]]; then
                                                            echo "need to provide SCT_SCYLLA_VERSION for Docker backend"
                                                            exit 1
                                                        fi
                                                    fi

                                                    if [[ ! -z "${instance_type}" ]]; then
                                                        case "${params.backend}" in
                                                            "aws")
                                                                export SCT_INSTANCE_TYPE_DB="${instance_type}"
                                                                ;;
                                                            "gce")
                                                                export SCT_GCE_INSTANCE_TYPE_DB="${instance_type}"
                                                                ;;
                                                            "azure")
                                                                export SCT_AZURE_INSTANCE_TYPE_DB="${instance_type}"
                                                                ;;
                                                        esac
                                                    fi

                                                    export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                                    export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"
                                                    export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                                    export SCT_AVAILABILITY_ZONE="${params.availability_zone}"

                                                    echo "start test ......."
                                                    ./docker/env/hydra.sh run-test artifacts_test --backend ${params.backend} --logdir "`pwd`"
                                                    echo "end test ....."
                                                """
                                            }
                                            stage("Collect log data (${instance_type})") {
                                                catchError(stageResult: 'FAILURE') {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            runCollectLogs(params, builder.region)
                                                        }
                                                    }
                                                }
                                            }
                                            stage("Clean resources (${instance_type})") {
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
                                            stage("Send email with result ${instance_type}") {
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
