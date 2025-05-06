#!groovy

import org.jenkinsci.plugins.workflow.support.steps.build.RunWrapper

def call(Map params, RunWrapper currentBuild){
    def start_time = currentBuild.startTimeInMillis.intdiv(1000)
    def test_status = currentBuild.currentResult
    if (test_status) {
        test_status = "--test-status " + test_status
    }
    if (start_time) {
        start_time = "--start-time " + start_time
    }

    def email_recipients = groovy.json.JsonOutput.toJson(params.email_recipients)
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    sh """#!/bin/bash
    set -xe
    env
    echo "Start send email ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")

    if [[ -z "${email_recipients}" ]]; then
        echo "Email was not sent because no recipient addresses were provided"
    else
        if [[ -n "\${RUNNER_IP}" ]] ; then
            ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} send-email ${test_status} ${start_time} \
            --runner-ip \${RUNNER_IP} --email-recipients "${email_recipients}"
        else
            ./docker/env/hydra.sh send-email ${test_status} ${start_time} --logdir "`pwd`" --email-recipients "${email_recipients}"
        fi
        echo "Email sent."
    fi
    """
}
