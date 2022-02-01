#!groovy
import groovy.json.JsonSlurper

def call(String backend, String region=null, String datacenter=null, String location=null) {
    try {
        regionList = new JsonSlurper().parseText(region)
        region = regionList[0]
    } catch(Exception) {

    }

    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1-new',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1',
                          'aws-eu-central-1': 'aws-sct-builders-eu-central-1',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1-new',
                          'gce-us-east1': 'gce-sct-builders-us-east1',
                          'gce-us-west1': 'gce-sct-builders-us-west1',
                          'gce': 'gce-sct-builders',
                          'docker': 'sct-builders',
                          'azure-eastus': 'aws-sct-builders-us-east-1-new']

    def cloud_provider = getCloudProviderFromBackend(backend)

    if ((cloud_provider == 'aws' && region) || (cloud_provider == 'gce' && datacenter) || (cloud_provider == 'azure' && location))
    {
        if (cloud_provider == 'aws')
        {
            def supported_regions = ["eu-west-2", "eu-north-1", "eu-central-1"]
        } else if (cloud_provider == 'gce') {
            def supported_regions = ["us-east1", "us-west1"]
            region = datacenter
        } else {
            def supported_regions = ["eastus"]
            region = location
        }

        println("Finding builder for region: " + region)
        if (region == "random" || datacenter == "random"){
            Collections.shuffle(supported_regions)
            region = supported_regions[0]
        }

        def cp_region = cloud_provider + "-" + region
        println("Checking if we have a label for " + cp_region)

        def label = jenkins_labels.get(cp_region, null)
        if (label != null){
            println("Found builder with label: " + label)
            return [ "label": label, "region": region ]
        } else {
            throw new Exception("=================== ${cloud_provider} region ${region} not supported ! ===================")
        }
    }
    else
    {
        return [ "label": jenkins_labels[cloud_provider], "region": region ]
    }
}
