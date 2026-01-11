#!groovy

def call(Map params = [:]) {
    withCredentials([
            string(credentialsId: 'docker-hub-jenkins-user', variable: 'DOCKER_USERNAME'),
            string(credentialsId: 'docker-hub-api-key', variable: 'DOCKER_PASSWORD')
        ]) {
        sh 'docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD'
        }
}
