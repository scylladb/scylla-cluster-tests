#!groovy

def call(Map params, boolean build_image){
    if (! (params.byo_scylla_repo && params.byo_scylla_branch) ) {
        println("BYO scylladb is not provided. Skipping this step.")
        return ''
    }
    config_conflict_error_msg_suffix = ' and "byo_scylla_repo"+"byo_scylla_branch" are mutually exclusive params.'
    if (params.backend == "aws" && params.scylla_ami_id && build_image) {
        error('CONFLICT: "scylla_ami_id"' + config_conflict_error_msg_suffix)
    } else if (params.backend == "gce" && params.gce_image_db && build_image) {
        error('CONFLICT: "gce_image_db"' + config_conflict_error_msg_suffix)
    } else if (params.backend == "azure" && params.azure_image_db && build_image) {
        error('CONFLICT: "azure_image_db"' + config_conflict_error_msg_suffix)
    } else if (params.new_scylla_repo && !build_image) {
        // NOTE: rolling upgrade case
        error('CONFLICT: "new_scylla_repo"' + config_conflict_error_msg_suffix)
    } else if (params.backend == 'docker' || params.backend.startsWith("k8s")) {
        // TODO: add docker image building support
        error('BYO Scylladb is not supported yet for building docker image in SCT.')
    }

    if (params.byo_job_path) {
        jobToTrigger = params.byo_job_path
    } else {
        jobToTrigger = "/enterprise-2024.2/byo/byo_build_tests_dtest"
    }
    if (jobToTrigger.startsWith("./")) {
        currentJobDirectoryPath = JOB_NAME.substring(0, JOB_NAME.lastIndexOf('/'))
        jobToTrigger = currentJobDirectoryPath + '/' + jobToTrigger[2..-1].trim()
    }

    byoParameterList = [
        string(name: 'DEFAULT_PRODUCT', value: params.byo_default_product),
        string(name: 'DEFAULT_BRANCH', value: params.byo_default_branch),
        string(name: 'SCYLLA_REPO', value: "git@github.com:scylladb/${params.byo_default_product}.git"),
        string(name: 'SCYLLA_BRANCH', value: params.byo_default_branch),
        string(name: 'SCYLLA_FORK_REPO', value: params.byo_scylla_repo),
        string(name: 'SCYLLA_FORK_BRANCH', value: params.byo_scylla_branch),
        //
        string(name: 'MACHINE_IMAGE_REPO',
               value: "git@github.com:scylladb/${params.byo_default_product}-machine-image.git"),
        string(name: 'MACHINE_IMAGE_BRANCH', value: params.byo_default_branch),
        // NOTE: SCT repo is used only for benchmarking which we disable here.
        //       So, hardcode 'master' SCT version for all cases.
        string(name: 'SCT_REPO', value: 'git@github.com:scylladb/scylla-cluster-tests.git'),
        string(name: 'SCT_BRANCH', value: 'master'),
        string(name: 'RELENG_REPO', value: "git@github.com:scylladb/${params.byo_default_product}-pkg.git"),
        string(name: 'RELENG_BRANCH', value: params.byo_default_branch),
        //
        booleanParam(name: 'BUILD_WITH_CMAKE', value: false),
        // NOTE: 'BUILD_MODE' must be empty string for '5.4'/'2024.1' and older branches
        string(name: 'BUILD_MODE', value: 'ALL'),
        booleanParam(name: 'CODE_COVERAGE', value: false),
        booleanParam(name: 'TEST_DEBUG_INFO', value: false),
        // SPECIAL_CONFIGURE_PY_PARAMS = '' // TODO: add it's support?
        //
        booleanParam(name: 'ENABLE_MICRO_BENCHMARKS', value: false),
        booleanParam(name: 'ENABLE_TESTS', value: false),
        string(name: 'X86_NUM_OF_UNITTEST_REPEATS', value: '1'),
        string(name: 'INCLUDE_TESTS', value: ''),
        //
        booleanParam(name: 'ENABLE_DTEST', value: false),
        //
        booleanParam(name: 'CREATE_CENTOS_RPM', value: false),
        booleanParam(name: 'CREATE_UNIFIED_DEB', value:  true),
        booleanParam(name: 'CREATE_DOCKER', value: false),
        booleanParam(name: 'CREATE_AMI', value: (params.backend == 'aws' && build_image)),
        string(name: 'COPY_AMI_TO_REGIONS', value: params.region),
        booleanParam(name: 'CREATE_GCE', value: (params.backend == 'gce' && build_image)),
        booleanParam(name: 'CREATE_AZURE', value: (params.backend == 'azure' && build_image)),
        //
        string(name: 'ARCH', value: 'x86_64'),
        string(name: 'TIMEOUT_PARAM', value: '4'), // reduced because dtests don't get run
        booleanParam(name: 'DEBUG_MAIL', value: true),
        booleanParam(name: 'DRY_RUN', value: false),
    ]
    try {
        jobResults=build job: jobToTrigger,
            parameters: byoParameterList,
            propagate: true,
            wait: true
    } catch(Exception ex) {
        echo "Could not trigger jon $jobToTrigger due to"
        println(ex.toString())
    }
    def byoBuildInfo = jobResults.getBuildVariables();
    try {
        println('byoBuildInfo=' + byoBuildInfo)
    } catch(Exception ex) {
        println('Failed to print BYO ScyllaDB build info')
    }
    scyllaBuildFailed = !(jobResults.result == "SUCCESS")
    if (scyllaBuildFailed) {
        currentBuild.description = ('BYO ScyllaDB failed')
        currentBuild.result = 'FAILED'
        error('BYO ScyllaDB failed')
    }

    // NOTE: export appropriate env vars to be reused further by the SCT
    if (build_image) {
        // NOTE: longevity case
        if (params.backend == "aws") {
            env.SCT_AMI_ID_DB_SCYLLA = byoBuildInfo.BYO_AMI_ID
        } else if (params.backend == "gce") {
            env.SCT_GCE_IMAGE_DB = byoBuildInfo.BYO_GCE_IMAGE_DB_URL
        } else if (params.backend == "azure") {
            env.SCT_AZURE_IMAGE_DB = byoBuildInfo.BYO_AZURE_IMAGE_NAME
        }
    } else {
        // NOTE: rolling upgrade case
        if (byoBuildInfo.BYO_SCYLLA_DEB_LIST_FILE_URL.startsWith("http")) {
            env.SCT_NEW_SCYLLA_REPO = byoBuildInfo.BYO_SCYLLA_DEB_LIST_FILE_URL
        } else {
            env.SCT_NEW_SCYLLA_REPO = 'https://' + byoBuildInfo.BYO_SCYLLA_DEB_LIST_FILE_URL
        }
    }
}
