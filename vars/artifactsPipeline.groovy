#! groovy

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
            string(defaultValue: "${pipelineParams.get('backend', 'gce')}",
                   description: 'aws|gce|azure|docker',
                   name: 'backend')
            string(defaultValue: "${pipelineParams.get('availability_zone', '')}",
               description: 'Availability zone',
               name: 'availability_zone')
            string(defaultValue: '',
                   description: 'a Scylla repo to run against (for .rpm/.deb tests, should be blank otherwise)',
                   name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('unified_package', '')}",
                   description: 'Url to the unified package of scylla version to install scylla',
                   name: 'unified_package')
            booleanParam(defaultValue: "${pipelineParams.get('nonroot_offline_install', false)}",
                   description: 'Install Scylla without required root priviledge',
                   name: 'nonroot_offline_install')
            separator(name: 'MANAGER_CONFIG', sectionHeader: 'Manager Configuration')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_address', '')}",
                   description: 'a Scylla Manager repo to run against (for .rpm/.deb tests, should be blank otherwise)',
                   name: 'scylla_mgmt_address')
            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_address', '')}",
                   description: 'manager agent repo',
                   name: 'scylla_mgmt_agent_address')
            string(defaultValue: "${pipelineParams.get('manager_version', '')}",
                   description: 'master_latest|3.2|3.1',
                   name: 'manager_version')
            separator(name: 'SCYLLA_DB', sectionHeader: 'ScyllaDB Configuration Selection')
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
            string(defaultValue: "${pipelineParams.get('test_config', 'test-cases/artifacts/centos7.yaml')}",
                   description: 'a config file for the artifacts test',
                   name: 'test_config')
            separator(name: 'PROVISIONING', sectionHeader: 'Provisioning Configuration')
            string(defaultValue: "${pipelineParams.get('instance_type', '')}",
                   description: 'a cloud instance type (leave blank for test case defaults)',
                   name: 'instance_type')
            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'on_demand|spot|spot_fleet',
                   name: 'provision_type')
            separator(name: 'MISC_CONFIG', sectionHeader: 'Miscellaneous Configuration')
            string(defaultValue: "${pipelineParams.get('gce_project', '')}",
               description: 'Gce project to use',
               name: 'gce_project')
            separator(name: 'EMAIL_REPORT', sectionHeader: 'Email Report Configuration')
            string(defaultValue: "${pipelineParams.get('email_recipients', 'qa@scylladb.com')}",
                   description: 'email recipients of email report',
                   name: 'email_recipients')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
            separator(name: 'EXTRA_ENVIRONMENTAL_VARIABLES', sectionHeader: 'Extra environment variables Configuration')
            text(defaultValue: "${pipelineParams.get('extra_environment_variables', '')}",
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
                                    withEnv(["AWS_ACCESS_KEY_ID=${env.AWS_ACCESS_KEY_ID}",
                                             "AWS_SECRET_ACCESS_KEY=${env.AWS_SECRET_ACCESS_KEY}",
                                             "SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {

                                        def test_config = groovy.json.JsonOutput.toJson(params.test_config)
                                        stage("Checkout (${instance_type})") {
                                            script {
                                                loadEnvFromString(params.extra_environment_variables)
                                            }
                                            dir('scylla-cluster-tests') {
                                                timeout(time: 10, unit: 'MINUTES') {
                                                    checkout scm
                                                }
                                            }
                                            dockerLogin(params)
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
                                        stage("Run SCT Test (${instance_type})") {
                                            // Timeout for the test itself
                                            timeout(time: pipelineParams.timeout.time, unit: 'MINUTES') {
                                                def cloud_provider = getCloudProviderFromBackend(params.backend)
                                                sctScript """
                                                    rm -fv ./latest

                                                    # clean the old sct_runner_ip file
                                                    rm -fv ./sct_runner_ip

                                                    if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
                                                        export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
                                                    fi
                                                    export SCT_COLLECT_LOGS=false
                                                    export SCT_CONFIG_FILES=${test_config}
                                                    if [[ -n "${params.region ? params.region : ''}" ]] ; then
                                                        export SCT_REGION_NAME='${params.region}'
                                                    fi
                                                    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
                                                        export SCT_GCE_DATACENTER=${params.gce_datacenter}
                                                    fi
                                                    if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
                                                        export SCT_AZURE_REGION_NAME=${params.azure_region_name}
                                                    fi
                                                    if [[ ! -z "${params.scylla_ami_id}" ]]; then
                                                        export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
                                                    elif [[ ! -z "${params.gce_image_db}" ]]; then
                                                        export SCT_GCE_IMAGE_DB="${params.gce_image_db}"
                                                    elif [[ ! -z "${params.azure_image_db}" ]]; then
                                                        export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"
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
                                                    if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
                                                        export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
                                                    fi

                                                    echo "start test ......."
                                                    ./docker/env/hydra.sh run-test artifacts_test --backend ${params.backend} --logdir "`pwd`"
                                                    echo "end test ....."
                                                """
                                            }
                                        }
                                        stage("Collect log data (${instance_type})") {
                                            catchError(stageResult: 'FAILURE') {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        timeout(time: 30, unit: 'MINUTES') {
                                                            runCollectLogs(params, builder.region)
                                                        }
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
