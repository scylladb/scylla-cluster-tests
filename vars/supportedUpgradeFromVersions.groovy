#!groovy

// Returns list of base versions either provided explicitly or discovered via hydra script.
def call(List base_versions_list, String linux_distro, String new_scylla_repo, String backend, Boolean base_version_all_sts_versions) {
    if (base_versions_list.isEmpty()) { // auto mode: discover supported base versions
        def result = sh(
            returnStdout: true,
            script: """./docker/env/hydra.sh get-scylla-base-versions --only-print-versions true \
                --linux-distro ${linux_distro} --scylla-repo ${new_scylla_repo} \
                --backend ${backend} \
                ${base_version_all_sts_versions ? '--base_version_all_sts_versions' : ''}
            """
        ).trim()
        printf('Docker get-scylla-base-versions output:\n%s', result)
        def last_line = result.split('\n')[-1]
        if (last_line.matches("Base\\sVersions:\\s*[\\d\\w\\W]*")) {
            return last_line.replaceAll('Base Versions: ', '').split(' ')
        } else {
            println("Did not find a valid base versions string!")
            throw new Exception("Didn't get valid base versions automatically!")
        }
    }
    return base_versions_list
}
