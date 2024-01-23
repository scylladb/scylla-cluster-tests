#!groovy
import groovy.json.JsonSlurper

def call(String backend, String region=null, String datacenter=null, String location=null) {
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
    gcp_project = gcp_project == 'gcp' ? 'gce-sct' : gcp_project

    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1-new',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1',
                          'aws-eu-central-1': 'aws-sct-builders-eu-central-1',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1-new',
                          'gce-us-east1': "${gcp_project}-builders-us-east1",
                          'gce-us-west1': "${gcp_project}-builders-us-west1",
                          'gce': "${gcp_project}-builders",
                          'docker': 'aws-sct-builders-eu-west-1-v2',
                          'azure-eastus': 'azure-sct-builders']

    def cloud_provider = getCloudProviderFromBackend(backend)

    if ((cloud_provider == 'aws' && region) || (cloud_provider == 'gce' && datacenter) || (cloud_provider == 'azure' && location)) {
        def supported_regions = []

        if (cloud_provider == 'aws') {
            supported_regions = ["eu-west-2", "eu-north-1", "eu-central-1"]
        } else if (cloud_provider == 'gce') {
            supported_regions = ["us-east1", "us-west1"]
            region = datacenter
        } else {
            supported_regions = ["eastus"]
            region = location
        }

        println("Finding builder for region: " + region)
        if (region == "random" || datacenter == "random") {
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
    } else {
        return [ "label": jenkins_labels[cloud_provider], "region": region ]
    }
}
