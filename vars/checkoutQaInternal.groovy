#!groovy

def call(Map params = [:]) {
    dir("scylla-qa-internal") {
        git(url: 'git@github.com:dimakr/scylla-qa-internal.git',
            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
            branch: 'add_limits_for_strongdm_container')  // TEMPORARY: testing StrongDM limits
    }
}
