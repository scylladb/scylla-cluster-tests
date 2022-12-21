#!groovy

// trick from https://github.com/jenkinsci/workflow-cps-global-lib-plugin/pull/43
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

def target_backends = ['aws', 'gce', 'docker']

def runSctProvisionTest(String backend){
    if (currentBuild.result != null) {
        return
    }
    echo "Going to Provision test on ${backend} for Pull Request ID: ${env.CHANGE_ID}"
    checkout scm
    def distro_cmd = ""
    def sct_config_files = "test-cases/PR-provision-test.yaml"
    if (backend == 'gce') {
        distro_cmd = "export SCT_SCYLLA_LINUX_DISTRO='centos'"
    } else if (backend == 'docker') {
        sct_config_files = "test-cases/PR-provision-test-docker.yaml"
    }
    sh """
    #!/bin/bash
    set -xe
    env
    export SCT_CLUSTER_BACKEND="${backend}"
    export SCT_COLLECT_LOGS=false
    export SCT_CONFIG_FILES="${sct_config_files}"
    export BUILD_USER_ID="\${CHANGE_AUTHOR}"
    $distro_cmd
    echo "start test ......."
    ./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --backend $backend --logdir "`pwd`"
    echo "end test ....."
    """
}

def runCollectLogs(String backend){
    def sct_config_files = "test-cases/PR-provision-test.yaml"
    if (backend == 'docker') {
        sct_config_files = "test-cases/PR-provision-test-docker.yaml"
    }
    sh """
    #!/bin/bash

    set -xe
    env

    export SCT_CLUSTER_BACKEND="${backend}"
    export SCT_REGION_NAME="eu-west-1"
    export SCT_CONFIG_FILES="${sct_config_files}"

    echo "start collect logs ..."
    ./docker/env/hydra.sh collect-logs --backend ${backend} --logdir "`pwd`"
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
    ./docker/env/hydra.sh send-email --logdir "`pwd`" --email-recipients "\${RECIPIENTS}"
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
    export SCT_REGION_NAME="eu-west-1"

    echo "start clean resources ..."
    ./docker/env/hydra.sh clean-resources --post-behavior --logdir "`pwd`"
    echo "end clean resources"
    """
}

def runRestoreMonitoringStack(){
    sh """
    #!/bin/bash

    set -xe
    env

    export MONITOR_STACK_TEST_ID=`cat latest/test_id`
    echo "Restoring Monitor stack for test-id \$MONITOR_STACK_TEST_ID"
    ./docker/env/hydra.sh investigate show-monitor \$MONITOR_STACK_TEST_ID --kill true
    """
}

pipeline {
    agent {
        label {
            label "aws-sct-builders-eu-west-1-v2-CI"
        }
    }
    environment {
        AWS_ACCESS_KEY_ID         = credentials('qa-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
    }
    options {
        timestamps()
        timeout(time: 90, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }
    stages {
        stage("precommit") {
            steps {
                script {
                    try {
                        sh './docker/env/hydra.sh pre-commit'
                        // also check the commit-messge for the rules we want
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
                        sh ''' ./docker/env/hydra.sh bash ./utils/lint_test_cases.sh '''
                        pullRequestSetResult('success', 'jenkins/lint_test_cases', 'All test cases are passed')
                    } catch(Exception ex) {
                        pullRequestSetResult('failure', 'jenkins/lint_test_cases', 'Some test cases failed')
                    }
                }
            }
        }
        stage("run mocked tests") {
            steps {
                script {
                    try {
                        sh ''' ./docker/env/hydra.sh run-aws-mock -r us-east-1 '''
                        sleep 10  // seconds
                        sh ''' ./docker/env/hydra.sh --aws-mock run-pytest functional_tests/mocked '''
                        pullRequestSetResult('success', 'jenkins/mocked_tests', 'All mocked tests are passed')
                    } catch(Exception ex) {
                        pullRequestSetResult('failure', 'jenkins/mocked_tests', 'Some mocked tests failed')
                    }
                    sh ''' ./docker/env/hydra.sh clean-aws-mocks '''
                }
            }
        }
        stage("provision test") {
            when {
                expression {
                    return pullRequestContainsLabels("test-provision,test-provision-aws,test-provision-gce,test-provision-docker") && currentBuild.result == null
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
                                def builder = getJenkinsLabels("${backend}", "${region}")
                                node(builder.label) {
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
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    runCollectLogs(backend)
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
                                        }
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    runCleanupResource(backend)
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
                                        }
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                dir('scylla-cluster-tests') {
                                                    runRestoreMonitoringStack()
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
                                            currentBuild.result = 'FAILURE'
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
                }
            }
        }
    }
}
