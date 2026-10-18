#! groovy

// catchFailure: false lets the caller see the failure (runWithSpotRetry needs it per attempt)
// instead of marking the build failed here.
def call(String script, boolean catchFailure = true) {
    def body = {
        wrap([$class: 'BuildUser']) {
            dir('scylla-cluster-tests') {
                sh """#!/bin/bash
                set -xe
                env

                ${script}
                """
            }
        }
    }
    if (catchFailure) {
        catchError(stageResult: 'FAILURE') {
            body()
        }
    } else {
        body()
    }
}
