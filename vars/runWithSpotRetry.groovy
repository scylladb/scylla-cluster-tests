#!groovy

import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

// Re-run a test inside this build when an attempt is lost to a spot reclaim (SCT-707).
//
// `attemptBody(provisionType)` runs one full attempt - Argus run, test, logs, cleanup, Argus finish - and
// returns `[error: <the test's error, or null when it passed>, retryable: <false after a real abort, in the
// test or in a cleanup step>]`. It must
// not mark the build failed itself: a build result can only get worse, so a reclaimed attempt that is retried
// would leave the build red. Only the error of the last attempt is recorded, which gives the same build
// result as before (FAILURE, or ABORTED for a timeout or an abort).
//
// The attempt counter is a local variable of this loop and is bounded here, whatever the config says.
// Each attempt gets a new SCT_TEST_ID (the first keeps the branch's own). Returns the test id of the
// last attempt, so steps after the loop (the result email) report on it.
String call(Map params, String label, Closure attemptBody) {
    final int maxAttempts = 3
    // A job with no instance_type list has an empty label.
    def prefix = label ? "${label}: " : ''
    def provisionType = params.provision_type
    def testId = env.SCT_TEST_ID
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        if (attempt > 1) {
            testId = UUID.randomUUID().toString()
        }
        def error = null
        def decision = 'stop'
        withEnv(["SCT_TEST_ID=${testId}"]) {
            def result = attemptBody(provisionType)
            error = result.error
            if (error != null && result.retryable && attempt < maxAttempts) {
                dir('scylla-cluster-tests') {
                    decision = spotRetryDecision(params, attempt)
                }
            }
        }
        if (error == null || decision == 'stop') {
            if (attempt > 1) {
                def outcome = error == null ? 'passed' : 'failed'
                currentBuild.description = (currentBuild.description ? currentBuild.description + '\n' : '') +
                    "${prefix}spot-retried ${attempt - 1}x, ${outcome}"
            }
            if (error != null) {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    throw error
                }
            }
            return testId
        }
        echo "${prefix}attempt ${attempt} was lost to a spot reclaim, retrying with instance_provision=${decision}"
        // A timeout kills the hydra client, not necessarily its container, which would keep running the old
        // attempt next to the new one. hydra names it "<SCT_TEST_ID>_<timestamp>". A no-op once it is gone.
        sh "docker ps -aq --filter name=^${testId}_ | xargs -r docker rm -f || true"
        provisionType = decision
    }
    return testId
}

// True for an interruption: an abort, or a timeout step that fired. The caller tells the two apart by
// elapsed time, because the interruption's causes cannot be read from the sandbox.
boolean isAbort(Throwable error) {
    return error instanceof FlowInterruptedException
}

// Run one cleanup step of an attempt (collect logs, clean resources, finish Argus) under its own timeout.
// A failure marks the stage, and sets the build to `buildResult`. A fired timeout counts as a failure too:
// left alone, its interruption sets the build ABORTED whatever `buildResult` says, so a retry that passes
// could not make the build green again. Log collection from a reclaimed node can easily use up its timeout.
// A real abort still aborts the build, and returns true: catchError swallows the interruption so the
// remaining cleanup steps run, so the caller must not retry the attempt after it.
boolean cleanupStep(String buildResult, int minutes, Closure body) {
    def aborted = false
    catchError(buildResult: buildResult, stageResult: 'FAILURE') {
        def startedMs = currentBuild.duration
        try {
            timeout(time: minutes, unit: 'MINUTES') {
                body()
            }
        } catch (FlowInterruptedException interruption) {
            if (currentBuild.duration - startedMs < minutes * 60000) {
                aborted = true
                throw interruption
            }
            error("Timed out after ${minutes} minutes")
        }
    }
    return aborted
}
