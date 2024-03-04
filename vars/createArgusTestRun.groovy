#!groovy

def call(Map params) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    sh """#!/bin/bash
        set -xe

        echo "Creating Argus test run ..."
        if [[ -n "${params.requested_by_user}" ]] ; then
            export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
        fi
        export SCT_CLUSTER_BACKEND="${params.backend}"
        export SCT_CONFIG_FILES=${test_config}

        ./docker/env/hydra.sh create-argus-test-run

        echo " Argus test run created."
    """
}
