#! groovy

def call(String script) {
    catchError(stageResult: 'FAILURE') {
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
}
