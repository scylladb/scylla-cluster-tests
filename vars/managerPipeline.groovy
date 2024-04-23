#!groovy

boolean jobEnabled (String jobName) {
	echo "Checking if Job $jobName exists / enabled"
	try {
		if (Jenkins.instance.getItemByFullName(jobName).isBuildable()) {
			echo "Job $jobName is enabled"
			return true
		} else {
			echo "Job $jobName is disabled, Skipping"
			return false
		}
	} catch (error) {
		echo "Error: General error |$error| while checking if job |$jobName| enabled (job does not exist)"
		return false
	}
}

def triggerJob(String jobToTrigger, def parameterList = [], boolean propagate = false, boolean wait = false) {
    if (jobEnabled(jobToTrigger)) {
        echo "Triggering '$jobToTrigger'"
        try {
            jobResults=build job: jobToTrigger,
                parameters: parameterList,
                propagate: propagate,  // if true, the triggering test will fail/pass based on the status of the triggered/downstream job/s
                wait: wait  // if true, the triggering job will not end until the triggered/downstream job/s will end
        } catch(Exception ex) {
            echo "Could not trigger jon $jobToTrigger due to"
            println(ex.toString())
        }
    }
}


def completed_stages = [:]
def (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = [0,0,0,0,0]

def call(Map pipelineParams) {

    def builder = getJenkinsLabels(params.backend, params.region, params.gce_datacenter, params.azure_region_name)

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
            string(defaultValue: "${pipelineParams.get('backup_bucket_backend', '')}",
               description: 's3|gcs|azure or empty',
               name: 'backup_bucket_backend')
            string(defaultValue: "${pipelineParams.get('backend', 'aws')}",
               description: 'aws|gce',
               name: 'backend')
            string(defaultValue: "${pipelineParams.get('region', 'eu-west-1')}",
               description: 'Supported: us-east-1 | eu-west-1 | eu-west-2 | eu-north-1 | eu-central-1 | us-west-2 | random (randomly select region)',
               name: 'region')
            string(defaultValue: "${pipelineParams.get('gce_datacenter', 'us-east1')}",
                   description: 'GCE datacenter',
                   name: 'gce_datacenter')
            string(defaultValue: "${pipelineParams.get('azure_region_name', 'eastus')}",
                   description: 'Azure location',
                   name: 'azure_region_name')
            string(defaultValue: "a",
               description: 'Availability zone',
               name: 'availability_zone')


            string(defaultValue: '', description: '', name: 'scylla_ami_id')
            string(defaultValue: "${pipelineParams.get('scylla_version', '2024.1')}", description: '', name: 'scylla_version')
            // When branching to manager version branch, set scylla_version to the latest release
            string(defaultValue: '', description: '', name: 'scylla_repo')
            string(defaultValue: "${pipelineParams.get('gce_image_db', '')}",
                   description: "gce image of scylla (since scylla_version doesn't work with gce)",
                   name: 'gce_image_db')  // TODO: remove setting once hydra is able to discover scylla images in gce from scylla_version
            string(defaultValue: "${pipelineParams.get('azure_image_db', '')}",
                   description: '',
                   name: 'azure_image_db')
            string(defaultValue: "${pipelineParams.get('provision_type', 'spot')}",
                   description: 'spot|on_demand|spot_fleet',
                   name: 'provision_type')
            string(defaultValue: "${pipelineParams.get('instance_provision_fallback_on_demand', 'false')}",
                   description: 'true|false',
                   name: 'instance_provision_fallback_on_demand')

            string(defaultValue: "${pipelineParams.get('post_behavior_db_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_db_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_loader_nodes', 'destroy')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_loader_nodes')
            string(defaultValue: "${pipelineParams.get('post_behavior_monitor_nodes', 'keep-on-failure')}",
                   description: 'keep|keep-on-failure|destroy',
                   name: 'post_behavior_monitor_nodes')

            string(defaultValue: "${pipelineParams.get('ip_ssh_connections', 'private')}",
                   description: 'private|public|ipv6',
                   name: 'ip_ssh_connections')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_address', '')}",
                   description: 'If empty - the default manager version will be taken',
                   name: 'scylla_mgmt_address')

            string(defaultValue: "${pipelineParams.get('manager_version', 'master_latest')}",
                   description: 'master_latest|3.1|3.0',
                   name: 'manager_version')

            string(defaultValue: "${pipelineParams.get('target_manager_version', '')}",
                   description: 'master_latest|3.1|3.0',
                   name: 'target_manager_version')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_agent_address', '')}",
                   description: 'manager agent repo',
                   name: 'scylla_mgmt_agent_address')

            string(defaultValue: "${pipelineParams.get('target_scylla_mgmt_server_address', '')}",
                   description: 'Link to the repository of the manager that will be used as a target of the manager server in the manager upgrade test',
                   name: 'target_scylla_mgmt_server_address')

            string(defaultValue: "${pipelineParams.get('target_scylla_mgmt_agent_address', '')}",
                   description: 'Link to the repository of the manager that will be used as a target of the manager agents in the manager upgrade test',
                   name: 'target_scylla_mgmt_agent_address')

            string(defaultValue: "'qa@scylladb.com','mgmt@scylladb.com'",
                   description: 'email recipients of email report',
                   name: 'email_recipients')

            string(defaultValue: "${pipelineParams.get('scylla_mgmt_pkg', '')}",
                   description: 'Url to the scylla manager packages',
                   name: 'scylla_mgmt_pkg')

            string(defaultValue: "${pipelineParams.get('test_config', '')}",
                   description: 'Test configuration file',
                   name: 'test_config')

            string(defaultValue: "${pipelineParams.get('test_name', '')}",
                   description: 'Name of the test to run',
                   name: 'test_name')

            string(defaultValue: "${pipelineParams.get('downstream_jobs_to_run', '')}",
                   description: 'Comma separated list of downstream jobs to run when the job passes',
                   name: 'downstream_jobs_to_run')
            string(defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
                   name: 'requested_by_user')
        }
        options {
            timestamps()
            disableConcurrentBuilds()
            buildDiscarder(logRotator(numToKeepStr: '20'))
        }
        stages {
            stage('Checkout') {
                options {
                    timeout(time: 5, unit: 'MINUTES')
                }
                steps {
                    script {
                        completed_stages = [:]
                    }
                    dir('scylla-cluster-tests') {
                        checkout scm
                        checkoutQaInternal(params)
                    }
               }
            }
            stage('Create Argus Test Run') {
                steps {
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
            }
            stage('Get test duration') {
                options {
                    timeout(time: 10, unit: 'MINUTES')
                }
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    (testDuration, testRunTimeout, runnerTimeout, collectLogsTimeout, resourceCleanupTimeout) = getJobTimeouts(params, builder.region)
                                }
                            }
                        }
                    }
                }
            }
            stage('Create SCT Runner') {
                options {
                    timeout(time: 5, unit: 'MINUTES')
                }
                steps {
                    script {
                        wrap([$class: 'BuildUser']) {
                            dir('scylla-cluster-tests') {
                                createSctRunner(params, runnerTimeout , builder.region)
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
                                timeout(time: testRunTimeout, unit: 'MINUTES') {
                                    dir('scylla-cluster-tests') {

                                        // handle params which can be a json list
                                        def region = initAwsRegionParam(params.region, builder.region)
                                        def datacenter = groovy.json.JsonOutput.toJson(params.gce_datacenter)
                                        def test_config = groovy.json.JsonOutput.toJson(params.test_config)
                                        def cloud_provider = params.backend.trim().toLowerCase()

                                        sh """#!/bin/bash
                                        set -xe
                                        env
                                        rm -fv ./latest

                                        export SCT_CLUSTER_BACKEND="${params.backend}"
                                        export SCT_REGION_NAME=${region}
                                        export SCT_GCE_DATACENTER=${datacenter}
                                        if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
                                            export SCT_AZURE_REGION_NAME=${params.azure_region_name}
                                        fi
                                        export SCT_CONFIG_FILES=${test_config}
                                        export SCT_COLLECT_LOGS=false

                                        if [[ -n "${params.backup_bucket_backend}" ]] ; then
                                            export SCT_BACKUP_BUCKET_BACKEND="${params.backup_bucket_backend}"
                                        fi

                                        if [[ ! -z "${params.scylla_ami_id}" ]] ; then
                                            export SCT_AMI_ID_DB_SCYLLA="${params.scylla_ami_id}"
                                        elif [[ ! -z "${params.scylla_version}" ]] ; then
                                            export SCT_SCYLLA_VERSION="${params.scylla_version}"
                                        elif [[ ! -z "${params.gce_image_db}" ]] ; then
                                            export SCT_GCE_IMAGE_DB="${params.gce_image_db}"  #TODO: remove it once scylla_version supports gce image detection
                                        elif [[ ! -z "${params.azure_image_db}" ]] ; then
                                            export SCT_AZURE_IMAGE_DB="${params.azure_image_db}"  #TODO: remove it once scylla_version supports azure image detection
                                        elif [[ ! -z "${params.scylla_repo}" ]] ; then
                                            export SCT_SCYLLA_REPO="${params.scylla_repo}"
                                        else
                                            echo "need to choose one of SCT_AMI_ID_DB_SCYLLA | SCT_GCE_IMAGE_DB | SCT_SCYLLA_VERSION | SCT_SCYLLA_REPO | SCT_AZURE_IMAGE_DB"
                                            exit 1
                                        fi
                                        if [[ -n "${params.availability_zone ? params.availability_zone : ''}" ]] ; then
                                            export SCT_AVAILABILITY_ZONE="${params.availability_zone}"
                                        fi
                                        export SCT_POST_BEHAVIOR_DB_NODES="${params.post_behavior_db_nodes}"
                                        export SCT_POST_BEHAVIOR_LOADER_NODES="${params.post_behavior_loader_nodes}"
                                        export SCT_POST_BEHAVIOR_MONITOR_NODES="${params.post_behavior_monitor_nodes}"
                                        export SCT_INSTANCE_PROVISION="${params.provision_type}"
                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$GIT_BRANCH | sed -E 's+(origin/|origin/branch-)++')
                                        export SCT_AMI_ID_DB_SCYLLA_DESC=\$(echo \$SCT_AMI_ID_DB_SCYLLA_DESC | tr ._ - | cut -c1-8 )

                                        export SCT_IP_SSH_CONNECTIONS="${params.ip_ssh_connections}"

                                        if [[ ! -z "${params.scylla_mgmt_address}" ]] ; then
                                            export SCT_SCYLLA_MGMT_ADDRESS="${params.scylla_mgmt_address}"
                                        fi

                                        if [[ ! -z "${params.manager_version}" ]] ; then
                                            export SCT_MANAGER_VERSION="${params.manager_version}"
                                        fi

                                        if [[ ! -z "${params.target_manager_version}" ]] ; then
                                            export SCT_TARGET_MANAGER_VERSION="${params.target_manager_version}"
                                        fi

                                        if [[ ! -z "${params.target_scylla_mgmt_server_address}" ]] ; then
                                            export SCT_TARGET_SCYLLA_MGMT_SERVER_ADDRESS="${params.target_scylla_mgmt_server_address}"
                                        fi

                                        if [[ ! -z "${params.target_scylla_mgmt_agent_address}" ]] ; then
                                            export SCT_TARGET_SCYLLA_MGMT_AGENT_ADDRESS="${params.target_scylla_mgmt_agent_address}"
                                        fi

                                        if [[ ! -z "${params.scylla_mgmt_agent_address}" ]] ; then
                                            export SCT_SCYLLA_MGMT_AGENT_ADDRESS="${params.scylla_mgmt_agent_address}"
                                        fi

                                        if [[ ! -z "${params.scylla_mgmt_pkg}" ]] ; then
                                            export SCT_SCYLLA_MGMT_PKG="${params.scylla_mgmt_pkg}"
                                        fi

                                        echo "start test ......."
                                        RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                        if [[ -n "\${RUNNER_IP}" ]] ; then
                                            ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} run-test ${params.test_name} --backend ${params.backend}
                                        else
                                            ./docker/env/hydra.sh run-test ${params.test_name} --backend ${params.backend}  --logdir "`pwd`"
                                        fi
                                        echo "end test ....."
                                        """
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage('Running Downstream Jobs') {  // Specifically placed after test stage, since downstream jobs should still be triggered when stages like collect logs fail.
                options {
                    timeout(time: 5, unit: 'MINUTES')
                }
                steps {
                    script {
                        if (currentBuild.currentResult == 'SUCCESS') {
                            jobNamesToTrigger = params.downstream_jobs_to_run.split(',')
                            currentJobDirectoryPath = JOB_NAME.substring(0, JOB_NAME.lastIndexOf('/'))
                            for (downstreamJobName in jobNamesToTrigger) {
                                fullJobPath = currentJobDirectoryPath + '/' + downstreamJobName.trim()
                                def repoParams = []
                                if (downstreamJobName.contains("upgrade")) {
                                    repoParams = [
                                        [$class: 'StringParameterValue', name: 'target_scylla_mgmt_server_address', value: params.scylla_mgmt_address],
                                        [$class: 'StringParameterValue', name: 'target_scylla_mgmt_agent_address', value: params.scylla_mgmt_agent_address],
                                        [$class: 'StringParameterValue', name: 'TARGET_MANAGER_VERSION', value: params.manager_version],
                                        [$class: 'StringParameterValue', name: 'provision_type', value: params.provision_type]
                                    ]
                                } else {
                                    repoParams = [
                                        [$class: 'StringParameterValue', name: 'scylla_mgmt_address', value: params.scylla_mgmt_address],
                                        [$class: 'StringParameterValue', name: 'scylla_mgmt_agent_address', value: params.scylla_mgmt_agent_address],
                                        [$class: 'StringParameterValue', name: 'manager_version', value: params.manager_version],
                                        [$class: 'StringParameterValue', name: 'provision_type', value: params.provision_type]
                                    ]
                                }
                                triggerJob(fullJobPath, repoParams)
                            }
                        } else {
                            echo "Job failed. Will not run downstream jobs."
                        }
                    }
                }
            }
            stage("Collect log data") {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: collectLogsTimeout, unit: 'MINUTES') {
                                        runCollectLogs(params, builder.region)
                                    }
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
                                    timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                        runCleanupResource(params, builder.region)
                                        completed_stages['clean_resources'] = true
                                    }
                                }
                            }
                        }
                    }
                }
            }
            stage("Send email with result") {
                options {
                    timeout(time: 10, unit: 'MINUTES')
                }
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    runSendEmail(params, currentBuild)
                                    completed_stages['send_email'] = true
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
            stage('Finish Argus Test Run') {
                steps {
                    catchError(stageResult: 'FAILURE') {
                        script {
                            wrap([$class: 'BuildUser']) {
                                dir('scylla-cluster-tests') {
                                    timeout(time: 5, unit: 'MINUTES') {
                                        finishArgusTestRun(params, currentBuild)
                                        completed_stages['report_to_argus'] = true
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        post {
            always {
                script {
                    def collect_logs = completed_stages['collect_logs']
                    def clean_resources = completed_stages['clean_resources']
                    def send_email = completed_stages['send_email']
                    sh """
                        echo "$collect_logs"
                        echo "$clean_resources"
                        echo "$send_email"
                    """
                    if (!completed_stages['clean_resources']) {
                        catchError {
                            script {
                                wrap([$class: 'BuildUser']) {
                                    dir('scylla-cluster-tests') {
                                        timeout(time: resourceCleanupTimeout, unit: 'MINUTES') {
                                            runCleanupResource(params, builder.region)
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (!completed_stages['send_email']) {
                        catchError {
                            script {
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
                    if (!completed_stages['report_to_argus']) {
                        catchError {
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
}
