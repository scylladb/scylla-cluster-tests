#!groovy

// Mark the step that failed (and its enclosing stage or parallel branch) as failed in the Jenkins UI
// and let the pipeline continue. Call it from a catch block that has already handled the error; without
// it the step whose exception was caught is shown green.
// failBuild=false is for best-effort steps: they are shown UNSTABLE and the build result is not changed.
def call(String message, boolean failBuild = true) {
    catchError(buildResult: failBuild ? 'FAILURE' : 'SUCCESS', stageResult: failBuild ? 'FAILURE' : 'UNSTABLE') {
        error(message)
    }
}
