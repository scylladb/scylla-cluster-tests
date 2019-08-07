pipeline {
  agent {
    label {
      label "sct-builders"
    }
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
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'success',
                                         context: 'jenkins/precommit',
                                         description: 'Precommit passed',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                } catch(Exception ex) {
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'failure',
                                         context: 'jenkins/precommit',
                                         description: 'Precommit failed',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
    stage("unittest") {
        steps {
            script {
                try {
                    sh './docker/env/hydra.sh unit-tests'
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'success',
                                         context: 'jenkins/unittests',
                                         description: 'All unit tests passed',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                } catch(Exception ex) {
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'failure',
                                     context: 'jenkins/unittests',
                                     description: 'unit tests failed',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
    stage("lint test-cases") {
        steps {
            script {
                try {
                    sh ''' ./utils/lint_test_cases.sh '''

                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'success',
                                         context: 'jenkins/lint_test_cases',
                                         description: 'all test cases are checked',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                } catch(Exception ex) {
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'failure',
                                         context: 'jenkins/lint_test_cases',
                                         description: 'some test cases failed to check',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
    stage("provision test") {
        when { changeRequest() }
        steps {
            node('aws-sct-builders-eu-west-1') {
                script {
                    def labels = []
                    pullRequest.labels.each { labels.add("${it}") }
                    def should_test = labels.find { it == "test-provision" }

                    if (should_test) {
                        echo "Going to Provision test Pull Request ID: ${env.CHANGE_ID}"

                        checkout scm

                        try {
                            sh """
                            #!/bin/bash
                            set -xe
                            env


                            echo "start test ......."
                            ./docker/env/hydra.sh run-test longevity_test.LongevityTest.test_custom_time --config test-cases/PR-provision-test.yaml --backend aws --logdir /sct
                            echo "end test ....."
                            """

                             pullRequest.createStatus(status: 'success',
                                         context: 'jenkins/provision_test',
                                         description: 'provision test succeeded',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                        }
                        catch(Exception ex) {

                            pullRequest.createStatus(status: 'failure',
                                             context: 'jenkins/provision_test',
                                             description: 'provision test failed',
                                             targetUrl: "${env.JOB_URL}/workflow-stage")

                            currentBuild.result = 'UNSTABLE'
                        }
                    }

                }
            }
        }
    }

  }
}
