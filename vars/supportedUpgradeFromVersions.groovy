#!groovy

def call(String git_branch, List base_versions_list) {
    def clean_branch_name = git_branch.tokenize('-')[-1]
    if (is_enterprise_version(clean_branch_name)) {
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
