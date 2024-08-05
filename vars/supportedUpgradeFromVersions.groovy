#!groovy

def call(List base_versions_list, String linux_distro, String new_scylla_repo, String backend) {
    if (base_versions_list.size() == 0) {  // auto mode, get the supported base versions list by a hydra command
        def result = sh (returnStdout: true,
                         script: """ ./docker/env/hydra.sh get-scylla-base-versions --only-print-versions true \
                                     --linux-distro ${linux_distro} --scylla-repo ${new_scylla_repo} \
                                     --backend ${backend} """)
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
