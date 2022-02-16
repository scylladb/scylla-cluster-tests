#!groovy

def call(Map params, Integer test_duration, String region) {
    def cloud_provider = getCloudProviderFromBackend(params.backend)
    def instance_type_arg = ""
    if ( params.backend == "k8s-local-kind-aws" ) {
        instance_type_arg = "--instance-type c5.xlarge"
    } else if ( params.backend == "k8s-local-kind-gce" ) {
        instance_type_arg = "--instance-type e2-standard-4"
    }

    // NOTE: EKS jobs have 'availability_zone' be defined as 'a,b'
    //       So, just pick up the first one for the SCT runner in such a case.
    def availability_zone = ""
    if ( params.availability_zone.contains(',') ) {
        availability_zone = params.availability_zone[0]
    } else {
        availability_zone = params.availability_zone
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
            --availability-zone ${availability_zone} \
            $instance_type_arg \
            --test-id \${SCT_TEST_ID} \
            --duration ${test_duration}
    else
        echo "Currently, <$cloud_provider> not supported to. Will run on regular builder."
    fi
    """
}
