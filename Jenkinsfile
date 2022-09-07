#!groovy

// trick from https://github.com/jenkinsci/workflow-cps-global-lib-plugin/pull/43
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

def target_backends = ['aws', 'gce', 'docker', 'k8s-local-kind-aws', 'azure']
def sct_runner_backends = ['aws', 'gce', 'k8s-local-kind-aws', 'azure']

def createRunConfiguration(String backend) {

    def configuration = [
        backend: backend,
        test_name: 'longevity_test.LongevityTest.test_custom_time',
        test_config: 'test-cases/PR-provision-test.yaml',
        availability_zone: 'a',
        scylla_version: '4.6.3',
        region: 'eu-west-1',
    ]
    if (backend == 'gce') {
        configuration.gce_datacenter = "us-east1"
    } else if (backend == 'azure') {
        configuration.azure_region_name = 'eastus'
        configuration.availability_zone = ''
    } else if (backend == 'docker') {
        configuration.test_config = "test-cases/PR-provision-test-docker.yaml"
    } else if (backend == 'k8s-local-kind-aws') {
        configuration.test_config = "test-cases/scylla-operator/functional.yaml"
        configuration.test_name = "functional_tests/scylla_operator"
        configuration.functional_tests = true
        configuration.availability_zone = 'a,b'
    }
    return configuration
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

def runRestoreMonitoringStack(){
    sh """
    #!/bin/bash

    set -xe
    env

    echo "Restoring Monitor stack for test-id \$SCT_TEST_ID"
    ./docker/env/hydra.sh investigate show-monitor \$SCT_TEST_ID --kill true
    """
}

pipeline {
    agent {
        label {
            label "sct-builders"
        }
    }
    environment {
        AWS_ACCESS_KEY_ID         = credentials('qa-aws-secret-key-id')
        AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
    }
    options {
        timestamps()
        buildDiscarder(logRotator(numToKeepStr: '10'))
    }
    stages {
        stage("precommit") {
            options {
                timeout(time: 15, unit: 'MINUTES')
            }
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
            options {
                timeout(time: 20, unit: 'MINUTES')
            }
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
            options {
                timeout(time: 10, unit: 'MINUTES')
            }
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
            options {
                timeout(time: 10, unit: 'MINUTES')
            }
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
                    return pullRequestContainsLabels("test-provision,test-provision-aws,test-provision-gce,test-provision-docker,test-provision-k8s-local-kind-aws,test-provision-azure") && currentBuild.result == null
                }
            }
            options {
                timeout(time: 180, unit: 'MINUTES')
            }
            steps {
                script {
                    def sctParallelTests = [:]
                    target_backends.each {
                        def backend = it
                        if (pullRequestContainsLabels("test-provision,test-provision-${backend}")) {
                            sctParallelTests["provision test on ${backend}"] = {
                                def curr_params = createRunConfiguration(backend)
                                def builder = getJenkinsLabels(curr_params.backend, curr_params.region, curr_params.gce_datacenter, curr_params.azure_region_name)
                                node(builder.label) {
                                    withEnv(["SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                                        script {
                                            def result = null

                                            dir('scylla-cluster-tests') {
                                                checkout scm
                                            }
                                            if (sct_runner_backends.contains(backend)){
                                                try {
                                                    wrap([$class: 'BuildUser']) {
                                                        dir('scylla-cluster-tests') {
                                                            echo "calling createSctRunner"
                                                            createSctRunner(curr_params, 90 , builder.region)
                                                        }
                                                    }
                                                } catch(Exception err) {
                                                    echo "${err}"
                                                    result = 'FAILURE'
                                                    pullRequestSetResult('failure', "jenkins/provision_${backend}", 'Some test cases are failed')
                                                }
                                            }
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    env.BUILD_USER_ID=env.CHANGE_AUTHOR
                                                    dir('scylla-cluster-tests') {
                                                        runSctTest(curr_params, builder.region, curr_params.get('functional_tests', false))
                                                        result = 'SUCCESS'
                                                        pullRequestSetResult('success', "jenkins/provision_${backend}", 'All test cases are passed')
                                                    }
                                                }
                                            } catch(Exception err) {
                                                echo "${err}"
                                                result = 'FAILURE'
                                                pullRequestSetResult('failure', "jenkins/provision_${backend}", 'Some test cases are failed')
                                            }
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        runCollectLogs(curr_params, builder.region)
                                                    }
                                                }
                                            } catch(Exception err) {
                                                echo "${err}"
                                            }
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    dir('scylla-cluster-tests') {
                                                        runCleanupResource(curr_params, builder.region)
                                                    }
                                                }
                                            } catch(Exception err) {
                                                echo "${err}"
                                            }
                                            if (backend != 'k8s-local-kind-aws') {
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
                    }
                    parallel sctParallelTests
                }
            }
        }
    }
}
