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
    # prompt. Then fall back to an unprivileged read before giving up: on a static agent that is
    # still the journal this build can see (its own units, and everything else when the agent user
    # is in systemd-journal), which beats no builder log at all. The if/else keeps a journal we
    # cannot read from aborting the stage under set -e before anything got uploaded.
    if sudo -n journalctl --since "@${sinceEpoch}" --no-tail --no-pager -o short-precise > builder-\$SHORT_SCT_TEST_ID.log ; then
        journal_source="sudo journalctl"
    elif journalctl --since "@${sinceEpoch}" --no-tail --no-pager -o short-precise > builder-\$SHORT_SCT_TEST_ID.log ; then
        journal_source="unprivileged journalctl"
    else
        journal_source=""
        echo "WARNING: neither sudo -n journalctl nor an unprivileged journalctl could read the journal on \$(hostname) - skipping builder journal upload"
    fi

    if [[ -n "\${journal_source}" ]] ; then
        echo "collected builder journal via \${journal_source}"
        tar -zcvf builder-\$SHORT_SCT_TEST_ID.log.tar.gz builder-\$SHORT_SCT_TEST_ID.log
        ./docker/env/hydra.sh upload --test-id \$SCT_TEST_ID builder-\$SHORT_SCT_TEST_ID.log.tar.gz
    fi
    """
}
