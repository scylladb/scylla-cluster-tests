#!groovy

def call(Map params) {
    withEnv(pipelineEnv(params: params)) {
    retry(3) {
		sh """#!/bin/bash
			set -xe

			echo "Creating Argus test run ..."

			./docker/env/hydra.sh create-argus-test-run

			echo " Argus test run created."
		"""
    }
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
