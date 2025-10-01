#!groovy

// trick from https://github.com/jenkinsci/workflow-cps-global-lib-plugin/pull/43
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

def target_backends = ['aws', 'gce', 'docker', 'k8s-local-kind-aws', 'k8s-eks', 'azure', 'xcloud-aws', 'xcloud-gce', 'vs-docker', 'vs-aws']
def sct_runner_backends = ['aws', 'gce', 'docker', 'k8s-local-kind-aws', 'k8s-eks', 'azure', 'xcloud-aws', 'xcloud-gce', 'vs-docker', 'vs-aws']

def createRunConfiguration(String backend) {
	def scylla_version = params.scylla_version ?: getLatestScyllaRelease('scylla')

    def configuration = [
        backend: backend,
        test_name: 'longevity_test.LongevityTest.test_custom_time',
        test_config: 'test-cases/PR-provision-test.yaml',
        availability_zone: 'a',
        scylla_version: scylla_version,
        region: 'eu-west-1',
    ]
    if (backend == 'gce') {
        configuration.gce_datacenter = "us-east1"
    } else if (backend == 'azure') {
        configuration.azure_region_name = 'eastus'
        configuration.availability_zone = ''
    } else if (backend.contains('docker')) {
        // use latest version of nightly image for docker backend, until we get rid off rebuilding docker image for SCT
        configuration.scylla_version = 'latest'
        configuration.docker_image = 'scylladb/scylla-nightly'
        configuration.test_config = "test-cases/PR-provision-test-docker.yaml"
    } else if (backend in ['k8s-local-kind-aws', 'k8s-eks']) {
        if (scylla_version.endsWith('latest')) {
            configuration.scylla_version = 'latest'
            configuration.docker_image = 'scylladb/scylla-nightly'
            configuration.k8s_scylla_operator_helm_repo = 'https://storage.googleapis.com/scylla-operator-charts/latest'
            configuration.k8s_scylla_operator_chart_version = 'latest'
            configuration.k8s_enable_tls = 'false'
            configuration.k8s_enable_sni = 'false'
        }
        configuration.test_config = "test-cases/scylla-operator/functional.yaml"
        configuration.test_name = "functional_tests/scylla_operator"
        configuration.functional_tests = true
        configuration.availability_zone = 'a,b'
    } else if (backend in  ['xcloud-aws', 'xcloud-gce']) {
        configuration.backend = 'xcloud'
        configuration.xcloud_env = 'staging'

        if (backend == 'xcloud-gce') {
            configuration.xcloud_provider = 'gce'
            configuration.gce_datacenter = "us-east1"
        }
        if (backend == 'xcloud-aws') {
            configuration.xcloud_provider = 'aws'
            configuration.test_config = '["test-cases/PR-provision-test.yaml"]'
        }
    }

    if (backend in ['vs-docker', 'vs-aws']) {
        configuration.test_config = '["' + configuration.test_config + '", "configurations/vector_store_provision_test.yaml"]'
        if (backend == 'vs-docker') {
            configuration.backend = 'docker'
        } else if (backend == 'vs-aws') {
            configuration.backend = 'aws'
        }
    }

    return configuration
}

def runSendEmail(){
    sh """#!/bin/bash

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
    sh """#!/bin/bash

    set -xe
    env

    echo "Restoring Monitor stack for test-id \$SCT_TEST_ID"
    ./docker/env/hydra.sh investigate show-monitor \$SCT_TEST_ID --kill true
    """
}

pipeline {
    agent {
        label {
            label "aws-sct-builders-eu-west-1-v3-CI"
        }
    }
    parameters {
        string(defaultValue: '',
               description: 'the scylla version to use for the provision tests, if empty automatically selects latest release',
               name: 'scylla_version')
        string(defaultValue: '',
               description: 'Actual user requesting job start, for automated job builds (e.g. through Argus)',
               name: 'requested_by_user')
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
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        dockerLogin(params)
                        // also check the commit-message for the rules we want
                        sh 'touch ./.git/COMMIT_EDITMSG'
                        sh './docker/env/hydra.sh pre-commit'
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/precommit', 'Precommit passed')
                    }
                }
                failure {
                    script {
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
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        checkoutQaInternal(params)
                        sh './docker/env/hydra.sh unit-tests'
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/unittests', 'All unit tests are passed')
                    }
                }
                failure {
                    script {
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
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        checkoutQaInternal(params)
                        sh ''' ./docker/env/hydra.sh bash ./utils/lint_test_cases.sh '''
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/lint_test_cases', 'All test cases are passed')
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/lint_test_cases', 'Some test cases failed')
                    }
                }
            }
        }
        stage("integration tests") {
            when {
                expression {
                    return pullRequestContainsLabels("test-integration")
                }
            }
            options {
                timeout(time: 45, unit: 'MINUTES')
            }
            steps {
                catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {
                    script {
                        def curr_params = createRunConfiguration('docker')
                        def builder = getJenkinsLabels(curr_params.backend, curr_params.region, curr_params.gce_datacenter, curr_params.azure_region_name)
                        dockerLogin(params)
                        withEnv(["SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                            dir('scylla-cluster-tests') {
                                checkout scm

                                dockerLogin(params)

                                wrap([$class: 'BuildUser']) {
                                    echo "calling createSctRunner"
                                    timeout(time: 5, unit: 'MINUTES') {
                                        createSctRunner(curr_params, 50 , builder.region)
                                    }
                                }
                                sh """#!/bin/bash
                                    set -xe
                                    echo "start integration-tests ..."
                                    RUNNER_IP=\$(cat sct_runner_ip||echo "")
                                    ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} integration-tests
                                    echo "end  integration-tests ..."
                                """
                            }
                        }
                    }
                }
            }
            post {
                success {
                    script {
                        pullRequestSetResult('success', 'jenkins/integration-tests', 'All integration tests are passed')
                    }
                }
                failure {
                    script {
                        pullRequestSetResult('failure', 'jenkins/integration-tests', 'Some integration tests failed')
                    }
                }
            }
        }
        stage("provision test") {
            when {
                expression {
                    def labels = [
                        "test-provision",
                        "test-provision-docker",
                        "test-provision-aws", "test-provision-aws-reuse",
                        "test-provision-gce", "test-provision-gce-reuse",
                        "test-provision-azure", "test-provision-azure-reuse",
                        "test-provision-k8s-local-kind-aws", "test-provision-k8s-local-kind-aws-reuse",
                        "test-provision-k8s-eks", "test-provision-k8s-eks-reuse",
                        "test-provision-xcloud-aws", "test-provision-xcloud-aws-reuse",
                        "test-provision-xcloud-gce", "test-provision-xcloud-gce-reuse",
                        "test-provision-vs-docker",
                        "test-provision-vs-aws", "test-provision-vs-aws-reuse",
                    ].join(",")
                    return pullRequestContainsLabels(labels)
                }
            }
            steps {
                script {
                    def sctParallelTests = [:]
                    target_backends.each {
                        def backend = it
                        if (pullRequestContainsLabels("test-provision,test-provision-${backend},test-provision-${backend}-reuse")) {
                            sctParallelTests["provision test on ${backend}"] = {
                                def curr_params = createRunConfiguration(backend)
                                def working_dir = "${backend}/scylla-cluster-tests"
                                def builder = getJenkinsLabels(curr_params.backend, curr_params.region, curr_params.gce_datacenter, curr_params.azure_region_name, curr_params)
                                withEnv(["SCT_TEST_ID=${UUID.randomUUID().toString()}",]) {
                                    script {
                                        def result = null
                                        dir(working_dir) {
                                            checkout scm
                                        }
                                        dockerLogin(params)
                                        if (sct_runner_backends.contains(backend)){
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    dir(working_dir) {
                                                        timeout(time: 5, unit: 'MINUTES') {
                                                            echo "calling createSctRunner"
                                                            createSctRunner(curr_params, 90 , builder.region)
                                                        }
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
                                                timeout(time: 300, unit: 'MINUTES') {
                                                    dir(working_dir) {
                                                        echo "Run provision test"
                                                        runSctTest(curr_params, builder.region, curr_params.get('functional_tests', false))
                                                        result = 'SUCCESS'
                                                        pullRequestSetResult('success', "jenkins/provision_${backend}", 'All test cases are passed')
                                                    }
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
                                            result = 'FAILURE'
                                            pullRequestSetResult('failure', "jenkins/provision_${backend}", 'Some test cases are failed')
                                        }
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                timeout(time: 90, unit: 'MINUTES') {
                                                    dir(working_dir) {
                                                        runCollectLogs(curr_params, builder.region)
                                                    }
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
                                        }
                                        if (!(backend in ['k8s-local-kind-aws', 'k8s-eks'])) {
                                            try {
                                                wrap([$class: 'BuildUser']) {
                                                    timeout(time: 25, unit: 'MINUTES') {
                                                        dir(working_dir) {
                                                            runRestoreMonitoringStack()
                                                        }
                                                    }
                                                }
                                            } catch(Exception err) {
                                                echo "${err}"
                                                currentBuild.result = 'FAILURE'
                                            }
                                        }
                                        if (pullRequestContainsLabels("test-provision-${backend}-reuse")) {
                                            try {
                                                withEnv(["SCT_REUSE_CLUSTER=${env.SCT_TEST_ID}"]) {
                                                    wrap([$class: 'BuildUser']) {
                                                        env.BUILD_USER_ID=env.CHANGE_AUTHOR
                                                        timeout(time: 300, unit: 'MINUTES') {
                                                            dir(working_dir) {
                                                                echo "Reuse the cluster and re-run provision test"
                                                                runSctTest(curr_params, builder.region, curr_params.get('functional_tests', false))
                                                                result = 'SUCCESS'
                                                                pullRequestSetResult('success', "jenkins/provision_${backend}", 'All test cases are passed')
                                                            }
                                                        }
                                                    }
                                                }
                                            } catch(Exception err) {
                                                echo "${err}"
                                                result = 'FAILURE'
                                                pullRequestSetResult('failure', "jenkins/provision_${backend}", 'Some test cases are failed during cluster reuse test')
                                            }
                                        }
                                        try {
                                            wrap([$class: 'BuildUser']) {
                                                timeout(time: 30, unit: 'MINUTES') {
                                                    dir(working_dir) {
                                                        runCleanupResource(curr_params, builder.region)
                                                    }
                                                }
                                            }
                                        } catch(Exception err) {
                                            echo "${err}"
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
