#!groovy

def call(Map params){
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    // Bound the journal to this build. On an ephemeral builder the journal is minutes old so the
    // bound changes nothing, but on a long-lived agent it is weeks of other jobs' history, and
    // unbounded this used to tar and upload the entire host journal every build.
    def sinceEpoch = (long) (currentBuild.startTimeInMillis / 1000)
    sh """#!/bin/bash

	set -xe

	echo "${params.test_config}"
	export SCT_CONFIG_FILES=${test_config}
	SHORT_SCT_TEST_ID=\$(echo \$SCT_TEST_ID | cut -c1-8)
    # sudo -n so an agent without passwordless sudo fails fast instead of hanging on a password
    # prompt, and the if so that a journal we cannot read degrades to "no builder log" instead of
    # aborting the stage under set -e before anything got uploaded.
    if sudo -n journalctl --since "@${sinceEpoch}" --no-tail --no-pager -o short-precise > builder-\$SHORT_SCT_TEST_ID.log ; then
        tar -zcvf builder-\$SHORT_SCT_TEST_ID.log.tar.gz builder-\$SHORT_SCT_TEST_ID.log
        ./docker/env/hydra.sh upload --test-id \$SCT_TEST_ID builder-\$SHORT_SCT_TEST_ID.log.tar.gz
    else
        echo "WARNING: sudo -n journalctl failed on \$(hostname) - skipping builder journal upload"
    fi
    """
}
