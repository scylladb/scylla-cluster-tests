#!groovy
import groovy.json.JsonSlurper

def call(String backend, String aws_region=null) {
    try {
        regionList = new JsonSlurper().parseText(aws_region)
        aws_region = regionList[0]
    } catch(Exception) {

    }


    def jenkins_labels = ['aws-eu-west-1': 'aws-sct-builders-eu-west-1',
                          'aws-us-east-1' : 'aws-sct-builders-us-east-1',
                          'gce': 'gce-sct-builders']

    if (backend == 'aws' && aws_region)
    {
        return jenkins_labels["${backend}-${aws_region}"]
    }
    else
    {
        return jenkins_labels[backend]
    }
}
