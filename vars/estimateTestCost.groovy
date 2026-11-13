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
    // One file per parallel branch, so branches estimating different instance types cannot
    // overwrite each other's result.
    def branch_suffix = instanceTypeDb ? "_" + instanceTypeDb.replaceAll(/[^A-Za-z0-9._-]/, '_') : ''
    def estimate_file = "cost_estimate_${env.BUILD_NUMBER ?: 'local'}${branch_suffix}.json"

    try {
        // Loading a config prints heavily to stdout, so the estimate is written to a file
        // rather than parsed back out of the log.
        def cmd = """#!/bin/bash
        ./docker/env/hydra.sh estimate-cost -b "${params.backend}" --output "${estimate_file}" --report-to-argus
        """
        // the branch's db instance type, for whichever backend runs (each backend reads only its own option)
        def inputs = instanceTypeDb ? [instance_type_db: instanceTypeDb, gce_instance_type_db: instanceTypeDb,
                                       azure_instance_type_db: instanceTypeDb, oci_instance_type_db: instanceTypeDb] : [:]
        withEnv(pipelineEnv(params: params, region: region, inputs: inputs)) {
            sh(script: cmd)
        }

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
