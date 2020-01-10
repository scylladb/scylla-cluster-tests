#!groovy

// trick from https://github.com/jenkinsci/workflow-cps-global-lib-plugin/pull/43
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

def target_backends = ['aws', 'gce']

def runSctProvisionTest(String backend){
	if (currentBuild.result != null) {
		return
	}
	echo "Going to Provision test on ${backend} for Pull Request ID: ${env.CHANGE_ID}"
	checkout scm
	def distro_cmd = ""
	if (backend == 'gce') {
		distro_cmd = "export SCT_SCYLLA_LINUX_DISTRO='centos'"
	}
	sh """
	#!/bin/bash
	set -xe
	env
	export SCT_CLUSTER_BACKEND="${backend}"
	export SCT_COLLECT_LOGS=false
	export SCT_POST_BEHAVIOR_DB_NODES="destroy"
	export SCT_POST_BEHAVIOR_LOADER_NODES="destroy"
	export SCT_POST_BEHAVIOR_MONITOR_NODES="destroy"
	export SCT_INSTANCE_PROVISION="on_demand"
    export SCT_CONFIG_FILES="test-cases/PR-provision-test.yaml"
    export BUILD_USER_ID="\${CHANGE_AUTHOR}"
	$distro_cmd
	echo "start test ......."
	./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --backend $backend --logdir /sct
	echo "end test ....."
	"""
}

def runCollectLogs(String backend){
    sh """
    #!/bin/bash

    set -xe
    env

    export SCT_CLUSTER_BACKEND="${backend}"
    export SCT_REGION_NAME="eu-west-1"
    export SCT_CONFIG_FILES="test-cases/PR-provision-test.yaml"

    echo "start collect logs ..."
    ./docker/env/hydra.sh collect-logs --logdir /sct
    echo "end collect logs"
    """
}

def runSendEmail(){
    sh """
    #!/bin/bash

    set -xe
    env
    export LAST_COMMIT=`git rev-parse HEAD`
    export RECIPIENTS=`git show -s --format='%ae' \$LAST_COMMIT`,`git show -s --format='%ce' \$LAST_COMMIT`
    echo "Start send email ..."
    ./docker/env/hydra.sh send-email --logdir /sct --email-recipients "\${RECIPIENTS}"
    echo "Email sent"
    """
}

def runCleanupResource(String backend){
    sh """
    #!/bin/bash

    set -xe
    env

    export SCT_CONFIG_FILES="test-cases/PR-provision-test.yaml"
    export SCT_CLUSTER_BACKEND="${backend}"
    export SCT_REGION_NAME="aws-eu-west-1"
    export SCT_POST_BEHAVIOR_DB_NODES="destroy"
    export SCT_POST_BEHAVIOR_LOADER_NODES="destroy"
    export SCT_POST_BEHAVIOR_MONITOR_NODES="destroy"

    echo "start clean resources ..."
    ./docker/env/hydra.sh clean-resources --logdir /sct
    echo "end clean resources"
    """
}

pipeline {
	agent {
		label {
			label "sct-builders"
		}
	}
	environment {
		AWS_ACCESS_KEY_ID		 = credentials('qa-aws-secret-key-id')
		AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
	}
	options {
		timestamps()
		timeout(time: 1, unit: 'HOURS')
		buildDiscarder(logRotator(numToKeepStr: '10'))
	}
	stages {
		stage("precommit") {
			steps {
				script {
					try {
						sh './docker/env/hydra.sh bash -c "cd /sct; pre-commit run -a"'
						pullRequestSetResult('success', 'jenkins/precommit', 'Precommit passed')
					} catch(Exception ex) {
						pullRequestSetResult('failure', 'jenkins/precommit', 'Precommit failed')
					}
				}
			}
		}
		stage("unittest") {
			steps {
				script {
					try {
						sh './docker/env/hydra.sh unit-tests'
						pullRequestSetResult('success', 'jenkins/unittests', 'All unit tests are passed')
					} catch(Exception ex) {
						pullRequestSetResult('failure', 'jenkins/unittests', 'Some unit tests failed')
					}
				}
			}
		}
		stage("lint test-cases") {
			steps {
				script {
					try {
						sh ''' ./utils/lint_test_cases.sh '''
						pullRequestSetResult('success', 'jenkins/lint_test_cases', 'All test cases are passed')
					} catch(Exception ex) {
						pullRequestSetResult('failure', 'jenkins/lint_test_cases', 'Some test cases failed')
					}
				}
			}
		}
		stage("provision test") {
			when {
				expression {
					return pullRequestContainsLabels("test-provision,test-provision-aws,test-provision-gce") && currentBuild.result == null
				}
			}
			steps {
                script {
                    def sctParallelTests = [:]
                    target_backends.each {
                        def backend = it
                        def region = 'eu-west-1'
                        if (pullRequestContainsLabels("test-provision,test-provision-${backend}")) {
                            sctParallelTests["provision test on ${backend}"] = {
                                node(getJenkinsLabels("${backend}", "${region}")) {
                                    script {
                                        def result = null
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    runSctProvisionTest(backend)
                                                    result = 'SUCCESS'
                                                    pullRequestSetResult('success', "jenkins/provision_${backend}", 'All test cases are passed')
                                                }
                                            }
                                        } catch(Exception err) {
                                            result = 'FAILURE'
                                            pullRequestSetResult('failure', "jenkins/provision_${backend}", 'Some test cases are failed')
                                        }
                                        if(result == 'FAILURE') {
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        runCollectLogs(backend)
                                                    }
                                                }
                                            } catch(Exception err) {
                                            }
                                        }
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    runCleanupResource()
                                                }
                                            }
                                        } catch(Exception err) {
                                        }
                                        if (result == 'FAILURE'){
                                            currentBuild.result = 'FAILURE'
                                            sh "exit 1"
                                        }
                                    }
                                }
                            }
                        }
                    }
                    parallel sctParallelTests
                    post {
                        success {
                            pullRequestSetResult('success', 'jenkins/provision_test', 'Provision test succeeded')
                        }
                        unsuccessful {
                            pullRequestSetResult('failure', 'jenkins/provision_test', 'Provision test failed')
                        }
                    }
                }
            }
		}
	}
}
