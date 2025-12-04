#!groovy

def call(Map params, Integer test_duration, String region) {
    def cloud_provider = getCloudProviderFromBackend(params.backend)

    // for xcloud backend, use the underlying cloud provider
    if ( params.backend.equals("xcloud") ) {
        cloud_provider = params.xcloud_provider
    }

    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def test_name = groovy.json.JsonOutput.toJson(params.test_name)

    // NOTE: EKS jobs have 'availability_zone' be defined as 'a,b'
    //       So, just pick up the first one for the SCT runner in such a case.
    def availability_zone = ""
    def availability_zone_arg = ""
    if ( params.availability_zone.contains(',') ) {
        availability_zone = params.availability_zone[0]
    } else {
        availability_zone = params.availability_zone
    }

    if ( availability_zone ) {
        availability_zone_arg = "--availability-zone " + availability_zone
    }

    if ( params.backend.equals("azure") ) {
        region_zone_arg = "--region " + params.azure_region_name
    } else {
        region_zone_arg = "--region " + region
    }

    println(params)
    sh """#!/bin/bash
    set -xe
    env

    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" || "$cloud_provider" == "azure" ]]; then
        rm -fv sct_runner_ip

        export SCT_CLUSTER_BACKEND="${params.backend}"
        export SCT_CONFIG_FILES=${test_config}

        if [[ "${params.backend}" == "xcloud" ]] ; then
            export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
            export SCT_XCLOUD_ENV="${params.xcloud_env}"
        fi

        if [[ -n "${params.requested_by_user ? params.requested_by_user : ''}" ]] ; then
            export BUILD_USER_REQUESTED_BY=${params.requested_by_user}
        fi

        ./docker/env/hydra.sh create-runner-instance \
            --cloud-provider ${cloud_provider} \
            $region_zone_arg \
            $availability_zone_arg \
            --test-id \${SCT_TEST_ID} \
            --duration ${test_duration} \
            --test-name ${test_name}

    else
        echo "Currently, <$cloud_provider> not supported to. Will run on regular builder."
    fi
    """
}
