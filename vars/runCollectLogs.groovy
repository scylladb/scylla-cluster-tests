#! groovy


def call(Map params, String region){
    def aws_region = initAwsRegionParam(params.region_name, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    sh """
    #!/bin/bash

    set -xe
    env

    echo "${params.test_config}"
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_REGION_NAME=${aws_region}
    export SCT_CONFIG_FILES=${test_config}

    echo "start collect logs ..."
    SCT_RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${SCT_RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${SCT_RUNNER_IP} collect-logs
    else
        ./docker/env/hydra.sh collect-logs --logdir "`pwd`"
    fi
    echo "end collect logs"
    """
}
