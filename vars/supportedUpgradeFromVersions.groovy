#!groovy

def call(List base_versions_list, String linux_distro, String new_scylla_repo) {
    if (base_versions_list.size() == 0) {  // auto mode, get the supported base versions list by a hydra command
        def result = sh (returnStdout: true,
                         script: """ ./docker/env/hydra.sh get-scylla-base-versions --only-print-versions true \
                                     --linux-distro ${linux_distro} --scylla-repo ${new_scylla_repo} """)
        println(result)
        def last_line = result.split('\n')[-1]
        if (last_line.contains('Base Versions: ')) {
            return last_line.replaceAll('Base Versions: ', '').split(' ')
        } else {
            throw new Exception("Didn't get valid base versions automatically!")
        }

    }
    if (new_scylla_repo.contains('enterprise')) {
        return base_versions_list
    } else {
        return base_versions_list.findAll{ ! is_enterprise_version(it) }
    }
}

def is_enterprise_version(String version) {
    def first_version = 0
    try {
        first_version = version.tokenize('.')[0] as Integer
    } catch (java.lang.NumberFormatException ex) {
        println "WARN: non formal/versioned branch '$version', assuming opensource version"
    }
    return first_version > 2000
}
