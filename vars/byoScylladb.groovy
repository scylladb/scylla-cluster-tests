#!groovy
import groovy.json.JsonSlurperClassic

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
    } else if (params.backend == "oci" && params.oci_image_db && build_image) {
        error('CONFLICT: "oci_image_db"' + config_conflict_error_msg_suffix)
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
        jobToTrigger = "/scylla-master/byo/byo_build_tests_dtest"
    }
    if (jobToTrigger.startsWith("./")) {
        currentJobDirectoryPath = JOB_NAME.substring(0, JOB_NAME.lastIndexOf('/'))
        jobToTrigger = currentJobDirectoryPath + '/' + jobToTrigger[2..-1].trim()
    }

    // Auto-detect architecture from the resolved test config using SCT/hydra
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def needArm = false
    def needX86 = false
    try {
        def dbArch = sh(
            returnStdout: true,
            script: """#!/bin/bash
                set -o pipefail
                ./docker/env/hydra.sh get-db-arch ${test_config} -b ${params.backend}
            """
        ).trim()
        // Extract the architecture value from output (hydra prints lots of noise before it)
        def archLine = dbArch.split('\n').findAll { it.trim() in ['arm64', 'x86_64'] }.last()
        dbArch = archLine.trim()
        println("Auto-detected DB architecture: ${dbArch}")
        needArm = (dbArch == 'arm64')
        needX86 = !needArm
    } catch (Exception ex) {
        println("WARNING: Could not auto-detect architecture from config: ${ex.getMessage()}")
        println("Falling back to default (x86)")
        needX86 = true
    }
    // If both-arch build is requested, build for both platforms (for reuse across test runs)
    // Preserve the auto-detected arch for image selection in this test run
    def testNeedArm = needArm
    if (params.byo_build_both_arch) {
        needArm = true
        needX86 = true
    }
    println("BYO build targets: ARM=${needArm}, x86=${needX86} (test uses: ${testNeedArm ? 'arm64' : 'x86_64'})")

    try {
        copyAmiToRegions = new JsonSlurperClassic().parseText(params.region)
        copyAmiToRegions = copyAmiToRegions.join(",").replace(" ", "")
    } catch(Exception) {
        copyAmiToRegions = params.region
    }
    byoParameterList = [
        string(name: 'DEFAULT_BRANCH', value: params.byo_default_branch),
        string(name: 'SCYLLA_REPO', value: "git@github.com:scylladb/${params.byo_default_product}.git"),
        string(name: 'SCYLLA_BRANCH', value: params.byo_default_branch),
        string(name: 'SCYLLA_FORK_REPO', value: params.byo_scylla_repo),
        string(name: 'SCYLLA_FORK_BRANCH', value: params.byo_scylla_branch),
        //
        string(name: 'MACHINE_IMAGE_REPO',
               value: "git@github.com:scylladb/${params.byo_default_product}-machine-image.git"),
        string(name: 'MACHINE_IMAGE_BRANCH', value: params.byo_default_branch),
        string(name: 'RELENG_REPO', value: "git@github.com:scylladb/${params.byo_default_product}-pkg.git"),
        string(name: 'RELENG_BRANCH', value: params.byo_default_branch),
        //
        string(name: 'BUILD_MODE', value: 'release'),
        booleanParam(name: 'TEST_DEBUG_INFO', value: false),
        //
        string(name: 'X86_NUM_OF_UNITTEST_REPEATS', value: '1'),
        string(name: 'INCLUDE_TESTS', value: ''),
        //
        booleanParam(name: 'RUN_UNIT_TESTS', value: false),
        booleanParam(name: 'RUN_DTEST', value: false),
        //
        booleanParam(name: 'CREATE_CENTOS_RPM', value: false),
        booleanParam(name: 'CREATE_UNIFIED_DEB', value:  true),
        booleanParam(name: 'CREATE_DOCKER', value: false),
        booleanParam(name: 'CREATE_AMI', value: (params.backend == 'aws' && build_image)),
        string(name: 'COPY_AMI_TO_REGIONS', value: copyAmiToRegions),
        booleanParam(name: 'CREATE_GCE', value: (params.backend == 'gce' && build_image)),
        booleanParam(name: 'CREATE_AZURE', value: (params.backend == 'azure' && build_image)),
        booleanParam(name: 'CREATE_OCI', value: (params.backend == 'oci' && build_image)),
        //
        booleanParam(name: 'BUILD_ARM', value: needArm),
        booleanParam(name: 'BUILD_X86', value: needX86),
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
        // NOTE: non-upgrade cases
        if (params.backend == "aws") {
            env.SCT_AMI_ID_DB_SCYLLA = testNeedArm ? byoBuildInfo.BYO_AMI_ID_ARM : byoBuildInfo.BYO_AMI_ID
        } else if (params.backend == "gce") {
            env.SCT_GCE_IMAGE_DB = testNeedArm ? byoBuildInfo.BYO_GCE_IMAGE_DB_URL_ARM : byoBuildInfo.BYO_GCE_IMAGE_DB_URL
        } else if (params.backend == "azure") {
            env.SCT_AZURE_IMAGE_DB = testNeedArm ? byoBuildInfo.BYO_AZURE_IMAGE_NAME_ARM : byoBuildInfo.BYO_AZURE_IMAGE_NAME
        } else if (params.backend == "oci") {
            env.SCT_OCI_IMAGE_DB = testNeedArm ? byoBuildInfo.BYO_OCI_IMAGE_ID_ARM : byoBuildInfo.BYO_OCI_IMAGE_ID
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
