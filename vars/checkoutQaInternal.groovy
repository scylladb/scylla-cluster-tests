#!groovy

def call(Map params = [:]) {
    dir("scylla-qa-internal") {
        git(url: 'git@github.com:scylladb/scylla-qa-internal.git',
            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
            branch: 'master')
    }
}
