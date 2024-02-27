#!groovy

def call(Map params = [:]) {
    dir("scylla-qa-internal") {
        // TODO: Use 'scylladb/scylla-qa-internal' repo when following PR gets merged:
        //       https://github.com/scylladb/scylla-qa-internal/pull/38
        git(url: 'git@github.com:vponomaryov/scylla-qa-internal.git',
            credentialsId:'b8a774da-0e46-4c91-9f74-09caebaea261',
            branch: 'custom-d1-update')
    }
}
