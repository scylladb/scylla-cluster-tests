#!groovy

def call(String labels){
	def tmp = labels.split(',')
	labels_from_request = pullRequest.labels.join(',')
	if (!changeRequest()){
		return false
	}
	def result = false
	pullRequest.labels.each {
		if (tmp.contains(it)){
			result = true
		}
	}
	return result
}
