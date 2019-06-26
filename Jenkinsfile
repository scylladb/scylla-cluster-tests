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
    stage("test microbenchmarking.py") {
        steps {
            script {
                try {
                    sh './docker/env/hydra.sh python sdcm/microbenchmarking.py --help'
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'success',
                                         context: 'jenkins/microbenchmarking',
                                         description: 'microbenchmarking.py is runnable',
                                         targetUrl: "${env.JOB_URL}/workflow-stage")
                    }
                } catch(Exception ex) {
                    if (env.CHANGE_ID) {
                        pullRequest.createStatus(status: 'failure',
                                         context: 'jenkins/microbenchmarking',
                                         description: 'microbenchmarking.py failed to run',
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
                    sh '''

                    for f in `find ./test-cases/ \\( -iname "*.yaml" ! -iname "*multi-dc.yaml" ! -iname *multiDC*.yaml ! -iname *multiple-dc*.yaml ! -iname *rolling* \\)` ; do
                        echo "---- testing: $f -----"
                        RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_AMI_ID_DB_SCYLLA=abc ./docker/env/hydra.sh conf $f" )
                        if [[ "$?" == "1" ]]; then
                            cat /tmp/test-case.txt
                            exit 1;
                        fi
                    done

                    for f in `find ./test-cases/ \\( -iname *multi-dc.yaml -or -iname *multiDC*.yaml -or -iname *multiple-dc*.yaml -or -iname *rolling* \\)`; do
                        echo "---- testing: $f -----"
                        RES=$( script --flush --quiet --return /tmp/test-case.txt --command "SCT_SCYLLA_REPO=abc ./docker/env/hydra.sh conf --backend gce $f" )
                        if [[ "$?" == "1" ]]; then
                            cat /tmp/test-case.txt
                            exit 1;
                        fi
                    done

                    '''

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
            script {
                sh "env"
                echo "Current Pull Request ID: ${env.CHANGE_ID}"
                pullRequest.labels.each {
                    echo "${it}"
                }
            }
        }
    }

  }
}
