#!groovy

def call(String status, String context, String description){
	if (env.CHANGE_ID) {
		pullRequest.createStatus(status: status,
			context: context,
			description: description,
			targetUrl: "${env.BUILD_URL}pipeline-overview")
	}
	if (status == 'failure') {
		currentBuild.result = 'FAILURE'
	}
}
