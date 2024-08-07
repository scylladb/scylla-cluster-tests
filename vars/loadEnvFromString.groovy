#!groovy

def call(String parameter) {
    if (parameter == null || parameter.isEmpty()) {
        return
    }
    def props = readProperties text: parameter
    props.each { key, value ->
        env[key] = value
    }
}
