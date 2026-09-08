#!groovy

/**
 * Print what this run is expected to cost, before anything is provisioned.
 *
 * Reads configuration only - the estimate is answered from the checked-in instance
 * catalog and calls no cloud pricing API - so this cannot hang or fail the build on
 * someone else's availability. Never throws: a cost estimate is not worth failing a run
 * over, so an unusable result is reported and the pipeline carries on.
 *
 * Returns the parsed estimate as a Map, or null when it could not be produced.
 */
Map call(Map params, String region) {
    def current_region = initAwsRegionParam(params.region, region)
    def current_oci_region = ""
    if (params.oci_region_name) {
        current_oci_region = initAwsRegionParam(params.oci_region_name, region)
    }
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)
    def estimate_file = "cost_estimate_${env.BUILD_NUMBER ?: 'local'}.json"

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

        if [[ -n "${params.instance_provision ? params.instance_provision : ''}" ]] ; then
            export SCT_INSTANCE_PROVISION="${params.instance_provision}"
        fi

        ./docker/env/hydra.sh estimate-cost -b "${params.backend}" --output "${estimate_file}"
        """
        sh(script: cmd)

        def estimate = readJSON(file: estimate_file)
        def total = estimate.total
        if (total == null) {
            echo "Estimated cost: unknown - no price for: ${estimate.unpriced_roles.join(', ')}"
            return estimate
        }

        def summary = String.format("Estimated cost: \$%.2f over %.2fh", total as Double,
                                    estimate.duration_hours as Double)
        if (estimate.is_spot) {
            summary += " (priced at on-demand; spot run, so this is an upper bound)"
        }
        if (estimate.partial) {
            summary += " [PARTIAL - no price for: ${estimate.unpriced_roles.join(', ')}]"
        }
        echo summary
        estimate.roles.each { role ->
            echo String.format("  %-10s %3d x %-24s %s", role.role, role.node_count as Integer,
                               role.instance_type,
                               role.cost == null ? "unknown" : String.format("\$%.2f", role.cost as Double))
        }
        currentBuild.description = ((currentBuild.description ?: '') + "\n" + summary).trim()
        return estimate
    } catch (Exception ex) {
        echo "Could not estimate test cost: ${ex.getMessage()}"
        return null
    }
}
