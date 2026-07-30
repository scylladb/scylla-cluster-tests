#!groovy

// Everything a Jenkins pipeline needs in order to run its test against minicloud instead of a
// real cloud. This used to be a whole pipeline of its own (minicloudPipeline.groovy, a fork of
// longevityPipeline) - a fork drifts, so the minicloud-specific parts live here and the regular
// pipelines opt in with `minicloud: true` in their jenkinsfile.
//
// A pipeline needs three touch points:
//
//   1. a MINICLOUD_CONFIG parameters section (declarative `parameters {}` cannot be shared, so
//      each pipeline declares it - see longevityPipeline for the canonical block)
//   2. startMinicloud.exportEnv(params, pipelineParams)  in 'Checkout', before anything talks
//      to a cloud API: env writes there are global for the rest of the build
//   3. a 'Start Minicloud' stage calling startMinicloud(params) - after 'Create SCT Runner',
//      before 'Provision Resources'
//
// Deliberately NOT here, because it belongs to the test-case configuration rather than to the
// pipeline: KMS (`enterprise_disable_kms`, `enable_kms_key_rotation`). minicloud implements no KMS
// endpoint yet, so the yamls switch it off; once minicloud does implement it, that becomes a
// per-test choice and no pipeline has to be touched.
//
// Two topologies reach this helper, and it works the same in both:
//
//   * a local KVM Jenkins agent, with no sct-runner at all - what artifacts jobs always use, and
//     what longevity/rolling-upgrade get with `local_agent: true`
//   * a nested-virtualization cloud sct-runner (c8i/m8i/r8i on AWS, n1/n2/c2/c3 on GCE, sized by
//     `instance_type_runner` in the test-case yaml) - still available to longevity and
//     rolling-upgrade for anyone without access to a lab machine

// Whether this build runs against minicloud. Single definition of the default so the three
// pipelines and their `when {}` guards cannot disagree.
def active(Map pipelineParams) {
    return pipelineParams.get('minicloud', false)
}

// Point SCT at minicloud for the rest of the build.
//
// SCT_MINICLOUD_ENDPOINT_URL is the switch: is_minicloud_active() in sdcm/utils/minicloud.py
// keys off it, and tester.py then brings a MinicloudManager up itself. Everything else here is
// detail on top of that.
def exportEnv(Map params, Map pipelineParams) {
    if (!active(pipelineParams)) {
        return false
    }

    // extra_environment_variables is loaded before this runs and wins: a per-run override has
    // to beat the jenkinsfile's default.
    env.SCT_MINICLOUD_ENDPOINT_URL = env.SCT_MINICLOUD_ENDPOINT_URL ?: 'http://localhost:5000'

    // Which minicloud image to run. The job parameter wins over the jenkinsfile default; empty
    // in both leaves minicloud_docker_image at its defaults/test_default.yaml value. Set the
    // SCT_ variant rather than a bare env var: SCT_* is what hydra forwards into the runner
    // container, and it lands in the run's config dump, so the image in use is verifiable.
    def minicloudImage = params.minicloud_docker ?: pipelineParams.get('minicloud_docker', '')
    if (minicloudImage && !env.SCT_MINICLOUD_DOCKER_IMAGE) {
        env.SCT_MINICLOUD_DOCKER_IMAGE = minicloudImage
    }

    return true
}

// Bring the minicloud container up and prepare its regions.
//
// Must run before any stage that calls the EC2/GCE API - in minicloud mode that is
// localhost:5000, so nothing can be provisioned until the container answers.
//
// `hydra start-minicloud` is MinicloudManager.start() with keep_alive=True, which is the whole
// point of doing it here rather than leaving it to the test process: 'Provision Resources',
// 'Run SCT Test', 'Collect logs' and 'Clean resources' are separate hydra invocations, and they
// all have to reach the same live endpoint. A container that died with the first of them would
// take cleanup down with it.
def call(Map params) {
    def test_config = groovy.json.JsonOutput.toJson(params.test_config)

    sh """#!/bin/bash
    set -xe

    export SCT_CLUSTER_BACKEND="${params.backend}"
    export SCT_CONFIG_FILES=${test_config}

    # Narrows region preparation to the one the test uses; unset prepares every SCT-supported
    # region, about two seconds each. GCE backends never prepare regions at all.
    if [[ -n "${params.region ?: ''}" ]] ; then
        export MINICLOUD_AWS_REGION="${params.region}"
    fi

    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} start-minicloud -b "${params.backend}"
    else
        ./docker/env/hydra.sh start-minicloud -b "${params.backend}"
    fi
    """
}
