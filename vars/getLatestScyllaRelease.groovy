#!groovy

import groovy.json.JsonSlurper

String call(String product) {
    product = product.replaceFirst('^scylla-', '')
    def url = "https://repositories.scylladb.com/scylla/check_version?system=${product}"

    def response = sh(script: "curl -s ${url}", returnStdout: true).trim()

    def json = new JsonSlurper().parseText(response)
    return json.version
}
