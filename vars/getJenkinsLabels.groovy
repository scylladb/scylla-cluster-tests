#!groovy
import groovy.json.JsonSlurper

def call(String backend, String aws_region=null) {
    try {
        regionList = new JsonSlurper().parseText(aws_region)
        aws_region = regionList[0]
    } catch(Exception) {

    }

    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1-v2-asg',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2-v2-asg',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1-v2-asg',
                          'aws-eu-central-1': 'aws-sct-builders-eu-central-1-v2-asg',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1-v2-asg',
                          'aws-us-west-2' : 'aws-sct-builders-us-west-2-v2-asg',
                          'gce-us-east1': "${gcp_project}-builders-us-east1",
                          'gce-us-west1': "${gcp_project}-builders-us-west1",
                          'gce': "${gcp_project}-builders",
                          'docker': 'sct-builders',
                          'azure-eastus': 'azure-sct-builders']

    if (backend == 'aws' && aws_region)
    {
        println("Finding builder for AWS region: " + aws_region)
        if (aws_region == "random"){
            def aws_supported_regions = ["eu-west-2", "eu-north-1", "eu-central-1"]
            Collections.shuffle(aws_supported_regions)
            aws_region = aws_supported_regions[0]
        }
        def cp_region = backend + "-" + aws_region
        println("Checking if we have a label for " + cp_region)
        def label = jenkins_labels.get(cp_region, null)
        if (label != null){
            println("Found AWS builder with label: " + label)
            return [ "label": label, "region": aws_region ]
        }
        else{
            throw new Exception("=================== AWS region ${aws_region} not supported ! ===================")
        }

    }
    else
    {
        return [ "label": jenkins_labels[backend] ]
    }
}
