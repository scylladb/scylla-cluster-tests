#!groovy
import groovy.json.JsonSlurper

def call(String backend, String aws_region=null) {
    try {
        regionList = new JsonSlurper().parseText(aws_region)
        aws_region = regionList[0]
    } catch(Exception) {

    }

    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1',
                          'aws-eu-west-2': 'aws-sct-builders-eu-west-2',
                          'aws-eu-north-1': 'aws-sct-builders-eu-north-1',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1',
                          'gce': 'gce-sct-builders',
                          'docker': 'sct-builders']
    println("Finding builder for region: " + aws_region)
    if (backend == 'aws' && aws_region)
    {
        if (aws_region == "random"){
            def aws_supported_regions = ["eu-west-2", "eu-north-1"]
            Collections.shuffle(aws_supported_regions)
            aws_region = aws_supported_regions[0]
        }

        return [ "label": jenkins_labels["${backend}-${aws_region}"], "region": aws_region ]

    }
    else
    {
        return [ "label": jenkins_labels[backend] ]
    }
}
