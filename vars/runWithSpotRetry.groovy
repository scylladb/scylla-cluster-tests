#!groovy

import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

// Re-run a test inside this build when an attempt is lost to a spot reclaim (SCT-707).
//
// `attemptBody(provisionType)` runs one full attempt - Argus run, test, logs, cleanup, Argus finish - and
// returns the test's error, or null when the test passed. It must not mark the build failed itself: a
// build result can only get worse, so a reclaimed attempt that is retried would leave the build red.
// Only the error of the last attempt is recorded, which gives the same build result as before.
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
            error = attemptBody(provisionType)
            // A real abort is never retried. A stage timeout is: a reclaimed node can leave the test
            // retrying SSH until the timeout fires (artifacts-ubuntu2404-test #696, #697).
            if (error != null && !isAbort(error) && attempt < maxAttempts) {
                dir('scylla-cluster-tests') {
                    decision = spotRetryDecision(params, attempt)
                }
            }
        }
        if (error == null || decision == 'stop') {
            if (attempt > 1) {
                def outcome = error == null ? 'passed' : 'failed'
                currentBuild.description = (currentBuild.description ? currentBuild.description + '\n' : '') +
                    "${prefix}spot-retried ${attempt - 1}/${maxAttempts - 1}, ${outcome}"
            }
            if (error != null) {
                catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
                    throw error
                }
            }
            return testId
        }
        echo "${prefix}attempt ${attempt} was lost to a spot reclaim, retrying with instance_provision=${decision}"
        provisionType = decision
    }
    return testId
}

// True for an abort by a user or by Jenkins itself. The caller turns a timeout step that fired into a plain
// failure first, because the interruption's causes cannot be read from the sandbox.
boolean isAbort(Throwable error) {
    return error instanceof FlowInterruptedException
}
