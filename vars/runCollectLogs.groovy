#! groovy


def call(Map params, String region){
    def region = initAwsRegionParam(params.region, region)
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    sh """
    #!/bin/bash

    set -xe
    env

    echo "${params.test_config}"
    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_REGION_NAME=${region}
    if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
        export SCT_GCE_DATACENTER=${params.gce_datacenter}
    fi
    export SCT_CONFIG_FILES=${test_config}

    echo "start collect logs ..."
    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} collect-logs
    else
        ./docker/env/hydra.sh collect-logs --logdir "`pwd`"
    fi
    echo "end collect logs"
    """
}
