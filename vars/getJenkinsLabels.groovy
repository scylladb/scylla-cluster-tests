#!groovy
import groovy.json.JsonSlurper

def call(String backend, String region=null, String datacenter=null, String location=null, Map overrides=null) {
    if (!(params instanceof Map)) {
        params = params.collectEntries()
    }
    if (overrides == null){
        overrides = [:]
    }
    params += overrides // merge, overrides take precedence

    if (!backend) {
        backend = 'aws'
        println("Backend is null or empty, defaulting to 'aws'")
    }

    try {
        regionList = new JsonSlurper().parseText(region)
        region = regionList[0]
    } catch(Exception) {

    }
    try {
        datacenterList = new JsonSlurper().parseText(datacenter)
        datacenter = datacenterList[0]
    } catch(Exception) {

    }

    def gcp_project = params.gce_project?.trim() ?: 'gcp-sct-project-1'
    gcp_project = gcp_project == 'gcp' ? 'gcp-skilled-adapter-452' : gcp_project

    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1-v3-asg',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2-v3-asg',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1-v3-asg',
                          'aws-eu-central-1': 'aws-sct-builders-eu-central-1-v3-asg',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1-v3-asg',
                          'aws-us-west-2' : 'aws-sct-builders-us-west-2-v3-asg',
                          'aws-eu-west-3' : 'aws-sct-builders-eu-west-3-v3-asg',
                          'aws-ca-central-1' : 'aws-sct-builders-ca-central-1-v3-asg',
                          'gce-us-east1': "${gcp_project}-builders-us-east1-template-v6",
                          'gce-us-west1': "${gcp_project}-builders-us-west1-template-v6",
                          'gce-us-central1': "${gcp_project}-builders-us-central1-template-v6",
                          'gce': "${gcp_project}-builders-us-east1-template-v6",
                          'aws': 'aws-sct-builders-eu-west-1-v3-asg',
                          'azure-eastus': 'aws-sct-builders-us-east-1-v3-asg',
                          'aws-fips': 'aws-sct-builders-us-east-1-v4-fibs-CI-FIPS',
                          ]

    def cloud_provider = getCloudProviderFromBackend(backend)

    // for xcloud backend, use the underlying cloud provider
    if (backend == 'xcloud') {
        cloud_provider = params.xcloud_provider?.trim()?.toLowerCase()
    }

    if ((cloud_provider == 'aws' && region) || (cloud_provider == 'gce' && datacenter) || (cloud_provider == 'azure' && location) || (cloud_provider == 'aws-fibs' && region)) {
        def supported_regions = []

        if (cloud_provider == 'aws') {
            supported_regions = ["eu-west-2", "eu-north-1", "eu-central-1", "us-west-2", "eu-west-3", "ca-central-1"]
        } else if (cloud_provider == 'gce') {
            supported_regions = ["us-east1", "us-west1", "us-central1"]
            region = datacenter
        } else {
            supported_regions = ["eastus"]
            region = location
        }

        println("Finding builder for region: " + region)
        if (region == "random" || datacenter == "random" || location == "random") {
            Collections.shuffle(supported_regions)
            region = supported_regions[0]
        }

        def cp_region = cloud_provider + "-" + region
        println("Checking if we have a label for " + cp_region)

        def label = jenkins_labels.get(cp_region, null)
        if (label != null) {
            println("Found builder with label: " + label)
            return [ "label": label, "region": region ]
        } else {
            throw new Exception("=================== ${cloud_provider} region ${region} not supported ! ===================")
        }
    } else if (region == 'fips') {
        return [ "label": jenkins_labels['aws-fips'], "region": '' ]
    } else {
        def label = jenkins_labels.get(cloud_provider, null)
        if (label == null) {
            throw new Exception("=================== No Jenkins builder label mapping found for backend '${backend}' (resolved " +
                                "to cloud_provider '${cloud_provider}'). Available mappings: ${jenkins_labels.keySet().sort()} ===================")
        }
        return [ "label": label, "region": region ]
    }
}
