#!groovy

def call(String labels){
	if (!changeRequest() || !env.CHANGE_ID){
		return false
	}
	def labels_to_look_for = labels.split(',')
	def result = false
	pullRequest.labels.each {
		if (labels_to_look_for.contains(it)){
			result = true
		}
	}
	return result
}
