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
                    pullRequest.createStatus(status: 'success',
                                     context: 'jenkins/precommit',
                                     description: 'Precommit passed',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
                } catch(Exception ex) {
                    pullRequest.createStatus(status: 'failure',
                                     context: 'jenkins/precommit',
                                     description: 'Precommit failed',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
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
                    pullRequest.createStatus(status: 'success',
                                     context: 'jenkins/unittests',
                                     description: 'All unit tests passed',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
                } catch(Exception ex) {
                    pullRequest.createStatus(status: 'failure',
                                 context: 'jenkins/unittests',
                                 description: 'unit tests failed',
                                 targetUrl: "${env.JOB_URL}/workflow-stage")
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
    stage("test microbenchmarking.py") {
        steps {
            script {
                try {
                    sh './docker/env/hydra.sh python sdcm/microbenchmarking.py --help'
                    pullRequest.createStatus(status: 'success',
                                     context: 'jenkins/microbenchmarking',
                                     description: 'microbenchmarking.py is runnable',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
                } catch(Exception ex) {
                    pullRequest.createStatus(status: 'failure',
                                     context: 'jenkins/microbenchmarking',
                                     description: 'microbenchmarking.py failed to run',
                                     targetUrl: "${env.JOB_URL}/workflow-stage")
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
  }
}
