#!groovy

/**
 * Print what this run is expected to cost, before anything is provisioned.
 *
 * Reads configuration only. Everything but AWS spot is answered from the checked-in
 * instance catalog; an AWS spot run costs one pricing call per region, whatever the node
 * count. Never throws: a cost estimate is not worth failing a run over, so an unusable
 * result is reported and the pipeline carries on.
 *
 * Must see the same sizing the test will run with, so it exports every override the
 * pipelines apply on top of the config files. `instanceTypeDb` is the db instance type of
 * one parallel branch (artifacts runs one branch per type), exported the way the Run SCT
 * Test stage exports it.
 *
 * Returns the parsed estimate as a Map, or null when it could not be produced.
 */
Map call(Map params, String region, String instanceTypeDb = '') {
    instanceTypeDb = instanceTypeDb ?: ''  // a branch with no type passes null, which bash would see as "null"
    def current_region = initAwsRegionParam(params.region, region)
    def current_oci_region = ""
    if (params.oci_region_name) {
        current_oci_region = initAwsRegionParam(params.oci_region_name, region)
    }
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    // One file per parallel branch, so branches estimating different instance types cannot
    // overwrite each other's result.
    def branch_suffix = instanceTypeDb ? "_" + instanceTypeDb.replaceAll(/[^A-Za-z0-9._-]/, '_') : ''
    def estimate_file = "cost_estimate_${env.BUILD_NUMBER ?: 'local'}${branch_suffix}.json"

    try {
        // Loading a config prints heavily to stdout, so the estimate is written to a file
        // rather than parsed back out of the log.
        def cmd = """#!/bin/bash
        export SCT_CLUSTER_BACKEND="${params.backend}"
        export SCT_CONFIG_FILES=${test_config}
        if [[ -n "${params.region ? params.region : ''}" ]] ; then
            export SCT_REGION_NAME=${current_region}
        fi

        if [[ -n "${params.gce_datacenter ? params.gce_datacenter : ''}" ]] ; then
            export SCT_GCE_DATACENTER='${params.gce_datacenter}'
        fi

        if [[ -n "${params.azure_region_name ? params.azure_region_name : ''}" ]] ; then
            export SCT_AZURE_REGION_NAME=${groovy.json.JsonOutput.toJson(params.azure_region_name)}
        fi

        if [[ -n "${params.oci_region_name ? params.oci_region_name : ''}" ]] ; then
            export SCT_OCI_REGION_NAME=${current_oci_region}
        fi

        # When set, these - not test_duration - decide how long the test runs.
        if [[ -n "${params.stress_duration ? params.stress_duration : ''}" ]] ; then
            export SCT_STRESS_DURATION=${params.stress_duration}
        fi
        if [[ -n "${params.prepare_stress_duration ? params.prepare_stress_duration : ''}" ]] ; then
            export SCT_PREPARE_STRESS_DURATION=${params.prepare_stress_duration}
        fi

        if [[ -n "${params.provision_type ? params.provision_type : ''}" ]] ; then
            export SCT_INSTANCE_PROVISION="${params.provision_type}"
        fi

        if [[ -n "${params.instance_provision_fallback_on_demand ? params.instance_provision_fallback_on_demand : ''}" ]] ; then
            export SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND="${params.instance_provision_fallback_on_demand}"
        fi

        # xcloud has no cloud of its own; without its provider nothing can be priced.
        if [[ "${params.backend}" == "xcloud" ]] ; then
            export SCT_XCLOUD_PROVIDER="${params.xcloud_provider}"
            export SCT_XCLOUD_ENV="${params.xcloud_env}"
        fi

        # Sizing overrides some pipelines take as job parameters (perfSearchBestConfig, OCI jobs).
        if [[ -n "${params.instance_type_db ?: ''}" ]] ; then
            export SCT_INSTANCE_TYPE_DB="${params.instance_type_db}"
        fi
        if [[ -n "${params.instance_type_loader ?: ''}" ]] ; then
            export SCT_INSTANCE_TYPE_LOADER="${params.instance_type_loader}"
        fi
        if [[ -n "${params.n_loaders ?: ''}" ]] ; then
            export SCT_N_LOADERS="${params.n_loaders}"
        fi
        if [[ -n "${params.oci_instance_type_db ?: ''}" ]] ; then
            export SCT_OCI_INSTANCE_TYPE_DB="${params.oci_instance_type_db}"
        fi

        if [[ -n "${instanceTypeDb}" ]] ; then
            case "${params.backend}" in
                "aws")
                    export SCT_INSTANCE_TYPE_DB="${instanceTypeDb}"
                    ;;
                "gce")
                    export SCT_GCE_INSTANCE_TYPE_DB="${instanceTypeDb}"
                    ;;
                "azure")
                    export SCT_AZURE_INSTANCE_TYPE_DB="${instanceTypeDb}"
                    ;;
                "oci")
                    export SCT_OCI_INSTANCE_TYPE_DB="${instanceTypeDb}"
                    ;;
            esac
        fi

        ./docker/env/hydra.sh estimate-cost -b "${params.backend}" --output "${estimate_file}" --report-to-argus
        """
        sh(script: cmd)

        // The command prints the human-readable table itself; re-printing it here only
        // doubled it in the log. Read the JSON purely for the build description and for
        // whatever later decides to act on the number.
        def estimate = readJSON(file: estimate_file)
        def basis = estimate.is_spot ? "spot" : "on-demand"
        def summary = estimate.total == null
            ? "Estimated cost: unknown"
            : String.format("Estimated cost: \$%.2f (%s)", estimate.total as Double, basis)
        // Only name the ceiling when the run can actually reach it: with fallback disabled a
        // spot run cannot become an on-demand one, and quoting the higher number would mislead.
        if (estimate.is_spot && estimate.fallback_to_on_demand && estimate.on_demand_total != null) {
            summary += String.format(" [up to \$%.2f on fallback]", estimate.on_demand_total as Double)
        }
        if (estimate.partial) {
            summary += " [partial]"
        }
        if (instanceTypeDb) {
            summary += " [${instanceTypeDb}]"
        }
        currentBuild.description = ((currentBuild.description ?: '') + "\n" + summary).trim()
        return estimate
    } catch (Exception ex) {
        echo "Could not estimate test cost: ${ex.getMessage()}"
        return null
    }
}
