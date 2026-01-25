#!groovy

def call(Map params) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    sh """#!/bin/bash

    set -xe

    echo "${params.test_config}"
    export SCT_CONFIG_FILES=${test_config}
    SHORT_SCT_TEST_ID=\$(echo \$SCT_TEST_ID | cut -c1-8)
    sudo journalctl --no-tail --no-pager -o short-precise > builder-\$SHORT_SCT_TEST_ID.log
    tar -zcvf builder-\$SHORT_SCT_TEST_ID.log.tar.gz builder-\$SHORT_SCT_TEST_ID.log

    ./docker/env/hydra.sh upload --test-id \$SCT_TEST_ID builder-\$SHORT_SCT_TEST_ID.log.tar.gz
    """
}
