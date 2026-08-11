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
// Every job that reaches this helper runs on a nested-virtualization cloud sct-runner
// (c8i/m8i/r8i on AWS, n1/n2/c2/c3 on GCE, sized by `instance_type_runner` in the test-case yaml
// or by configurations/minicloud/{aws,gce}.yaml). Running the emulator on a long-lived KVM
// Jenkins agent instead is a follow-up; this helper is written to work either way, selected as
// everywhere else by the presence of ./sct_runner_ip.

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

    // 'random' must be rejected for every topology, not only when getJenkinsLabels resolves the
    // KVM label (local agent): in runner topology the raw region parameter flows into
    // SCT_MINICLOUD_REGIONS below and prepare_regions() would construct AwsRegion("random").
    // minicloud cannot do random regions anyway - it validates uncached AMIs against its own
    // --aws-region.
    if ([params.region, params.gce_datacenter, params.azure_region_name].contains('random')) {
        error("minicloud needs a fixed region, not 'random': it validates uncached AMIs against its own --aws-region")
    }

    // extra_environment_variables is loaded before this runs and wins: a per-run override has
    // to beat the jenkinsfile's default.
    env.SCT_MINICLOUD_ENDPOINT_URL = env.SCT_MINICLOUD_ENDPOINT_URL ?: 'http://localhost:5000'

    // Which minicloud image to run: the `minicloud_docker` parameter feeds the
    // minicloud_docker_image SCT option, whose env form hydra forwards like any other SCT_*.
    //
    // Exported here, for the WHOLE build, rather than in the 'Start Minicloud' sh step alone:
    // MinicloudManager.start() restarts a running container whose image differs from the one
    // its own config resolved, so a 'Run SCT Test' that cannot see the override tears the
    // pipeline's container down and replaces it with the renovate-managed default. The run then
    // tests the wrong emulator (minicloud-artifact-ami-test #18,
    // minicloud-artifact-gce-image-test #4 both did), and in longevity topology that restart
    // lands after 'Provision Resources' and takes the guests with it.
    //
    // Safe as a Groovy env write only because the names differ by more than case: Jenkins'
    // EnvVars map is case-insensitive (a CASE_INSENSITIVE_ORDER TreeMap) and auto-injects every
    // job parameter, so exporting `MINICLOUD_DOCKER` next to a `minicloud_docker` parameter
    // silently updated the parameter's own entry instead and never reached sh - which is what
    // #16-17 of the same job chased. Keep any future parameter name distinct from the env var
    // it feeds. Empty stays unset: an unset override leaves the default image in charge.
    //
    // Whichever source wins is validated against the minicloud repositories, and deliberately
    // before it is exported: the resolved reference reaches `docker run` on a host where the QA
    // AWS credentials are passed in and the GCP service-account key is mounted, so permission to
    // trigger a build must not become permission to run an arbitrary container with those. The
    // check covers the extra_environment_variables path too, which sets the env var directly.
    def minicloudImage = env.SCT_MINICLOUD_DOCKER_IMAGE ?: params.minicloud_docker ?: pipelineParams.get('minicloud_docker', '')
    if (minicloudImage) {
        if (!(minicloudImage ==~ /^(ghcr\.io\/scylladb|scylladb)\/minicloud:[\w][\w.\-]*$/)) {
            error("minicloud image must be a scylladb/minicloud or ghcr.io/scylladb/minicloud " +
                  "reference with an explicit tag, got '${minicloudImage}'")
        }
        env.SCT_MINICLOUD_DOCKER_IMAGE = minicloudImage
    }
    println("minicloud image: " + (env.SCT_MINICLOUD_DOCKER_IMAGE ?: 'renovate-managed default'))

    exportSizing(params, pipelineParams)

    return true
}

// Resolve one sizing knob: job parameter first, then the jenkinsfile's pipelineParams.
//
// Empty at every level stays empty, so the yaml default keeps charge - a Jenkins string parameter
// nobody filled in arrives as '', and exporting that would override the yaml with nothing.
def pickSizing(Map params, Map pipelineParams, String name) {
    def value = params.get(name) ?: pipelineParams.get(name, '')
    return value?.toString() ?: ''
}

// Export the guest and container sizing knobs build-wide.
//
// Every one is an ordinary SCT config option with a default in defaults/test_default.yaml, so a
// test-case or overlay yaml already covers the common case. These exist for the axis a yaml cannot
// express: the same test wants a different guest budget on a lab machine than on a CI agent.
//
// Build-wide rather than inside the 'Start Minicloud' sh step, for the same reason the image is:
// start-minicloud, provision-resources, run-test, collect-logs and clean-resources are separate
// hydra invocations and MinicloudConfig is rebuilt in each one. A later invocation that cannot see
// minicloud_container_name or minicloud_state_dir looks for the container under the default name,
// or in the wrong state directory - so teardown leaves the real container running and collection
// finds no logs.
//
// `env.X = env.X ?: value` keeps extra_environment_variables winning: it is loaded before this
// runs, and a per-run override has to beat both the parameter and the jenkinsfile. Written out one
// name at a time rather than looped over a map, because a dynamic env write needs
// env.setProperty() and nothing else in vars/ does that - a groovy mistake here surfaces as a
// pipeline that will not start, not as a test failure.
//
// Parameter names stay distinct from the env vars they feed (the SCT_ prefix does it) - see the
// EnvVars case-insensitivity note above, which cost two builds of the same job to diagnose.
def exportSizing(Map params, Map pipelineParams) {
    def value

    if ((value = pickSizing(params, pipelineParams, 'minicloud_lightweight'))) {
        env.SCT_MINICLOUD_LIGHTWEIGHT = env.SCT_MINICLOUD_LIGHTWEIGHT ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_lightweight_memory'))) {
        env.SCT_MINICLOUD_LIGHTWEIGHT_MEMORY = env.SCT_MINICLOUD_LIGHTWEIGHT_MEMORY ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_lightweight_vcpus'))) {
        env.SCT_MINICLOUD_LIGHTWEIGHT_VCPUS = env.SCT_MINICLOUD_LIGHTWEIGHT_VCPUS ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_container_memory'))) {
        env.SCT_MINICLOUD_CONTAINER_MEMORY = env.SCT_MINICLOUD_CONTAINER_MEMORY ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_container_cpus'))) {
        env.SCT_MINICLOUD_CONTAINER_CPUS = env.SCT_MINICLOUD_CONTAINER_CPUS ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_state_dir'))) {
        env.SCT_MINICLOUD_STATE_DIR = env.SCT_MINICLOUD_STATE_DIR ?: value
    }
    if ((value = pickSizing(params, pipelineParams, 'minicloud_container_name'))) {
        env.SCT_MINICLOUD_CONTAINER_NAME = env.SCT_MINICLOUD_CONTAINER_NAME ?: value
    }

    println("minicloud sizing: lightweight=${env.SCT_MINICLOUD_LIGHTWEIGHT ?: 'yaml default'} " +
            "memory=${env.SCT_MINICLOUD_LIGHTWEIGHT_MEMORY ?: 'yaml default'} " +
            "vcpus=${env.SCT_MINICLOUD_LIGHTWEIGHT_VCPUS ?: 'yaml default'} " +
            "container_memory=${env.SCT_MINICLOUD_CONTAINER_MEMORY ?: 'no limit'} " +
            "container_cpus=${env.SCT_MINICLOUD_CONTAINER_CPUS ?: 'no limit'} " +
            "state_dir=${env.SCT_MINICLOUD_STATE_DIR ?: 'yaml default'} " +
            "container=${env.SCT_MINICLOUD_CONTAINER_NAME ?: 'minicloud'}")
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

    # SCT_MINICLOUD_DOCKER_IMAGE is exported build-wide by exportEnv - see the comment there
    # for why it cannot live in this sh step alone.

    # Narrows region preparation to the one the test uses; unset prepares every SCT-supported
    # region, about two seconds each. GCE backends never prepare regions at all.
    if [[ -n "${params.region ?: ''}" ]] ; then
        export SCT_MINICLOUD_REGIONS="${params.region}"
    fi

    RUNNER_IP=\$(cat sct_runner_ip||echo "")
    if [[ -n "\${RUNNER_IP}" ]] ; then
        ./docker/env/hydra.sh --execute-on-runner \${RUNNER_IP} start-minicloud -b "${params.backend}"
    else
        ./docker/env/hydra.sh start-minicloud -b "${params.backend}"
    fi
    """
}
