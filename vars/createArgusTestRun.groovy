#!groovy

def call(Map params) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    retry(3) {
		sh """#!/bin/bash
			set -xe

			echo "Creating Argus test run ..."
			if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
				export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
			fi
			export SCT_CLUSTER_BACKEND="${params.backend}"
			export SCT_CONFIG_FILES=${test_config}

            if [[ "${params.backend}" == "xcloud" ]] ; then
                export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
                export SCT_XCLOUD_ENV="${params.xcloud_env}"
            fi

			./docker/env/hydra.sh create-argus-test-run

			echo " Argus test run created."
		"""
    }
    if (!currentBuild.description) {
        currentBuild.description = ''
    }
    String runButton = """
        <div style="margin: 12px 4px;">
            <a
                href='https://argus.scylladb.com/tests/scylla-cluster-tests/${SCT_TEST_ID}'
            >
                Argus: <span style='font-weight: 500'>${SCT_TEST_ID}</span>
            </a>
        </div>
    """
    currentBuild.description += "${runButton}"
}
