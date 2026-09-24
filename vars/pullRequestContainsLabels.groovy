#!groovy

// pullRequest.labels is backed by a non-serializable lambda. Iterating it in CPS code leaves that
// object in the program state, and a checkpoint taken by a parallel branch then fails with
// NotSerializableException. Reading the labels in a @NonCPS method keeps it out of the CPS state.
@NonCPS
private List<String> getPullRequestLabelNames() {
	return pullRequest.labels.collect { it.toString() }
}

def call(String labels){
	if (!changeRequest() || !env.CHANGE_ID){
		return false
	}
	def labels_to_look_for = labels.split(',')
	for (String label : getPullRequestLabelNames()) {
		if (labels_to_look_for.contains(label)){
			return true
		}
	}
	return false
}
