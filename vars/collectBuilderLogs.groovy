#!groovy

def call(){
    sh """#!/bin/bash

	set -xe

	SHORT_SCT_TEST_ID=\$(echo \$SCT_TEST_ID | cut -c1-8)
    sudo journalctl --no-tail --no-pager -o short-precise > builder-\$SHORT_SCT_TEST_ID.log
    tar -zcvf builder-\$SHORT_SCT_TEST_ID.log.tar.gz builder-\$SHORT_SCT_TEST_ID.log

    ./docker/env/hydra.sh upload --test-id \$SCT_TEST_ID builder-\$SHORT_SCT_TEST_ID.log.tar.gz
    """
}
