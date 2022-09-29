#!groovy

def call(Map params, Integer test_duration, String region) {
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    def instance_type_arg = ""
    def root_disk_size_gb_arg = ""
    if ( params.backend == "k8s-local-kind-aws" ) {
        instance_type_arg = "--instance-type c5.2xlarge"
        root_disk_size_gb_arg = "--root-disk-size-gb 120"
    } else if ( params.backend == "k8s-local-kind-gce" ) {
        instance_type_arg = "--instance-type e2-standard-8"
        root_disk_size_gb_arg = "--root-disk-size-gb 120"
    }

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
    println(params)
    sh """
    #!/bin/bash
    set -xe
    env

    if [[ "$cloud_provider" == "aws" || "$cloud_provider" == "gce" || "$cloud_provider" == "azure" ]]; then
        rm -fv sct_runner_ip
        ./docker/env/hydra.sh create-runner-instance \
            --cloud-provider ${cloud_provider} \
            --region ${region} \
            $availability_zone_arg \
            $instance_type_arg \
            $root_disk_size_gb_arg \
            --test-id \${SCT_TEST_ID} \
            --duration ${test_duration}
    else
        echo "Currently, <$cloud_provider> not supported to. Will run on regular builder."
    fi
    """
}
