#!groovy

def call(String parameter) {
    if (!parameter) return

    readProperties(text: parameter).each { key, value ->
        // Remove surrounding quotes from value if present
        def cleanValue = (value instanceof String) ? value.replaceAll(/^(['"])(.*)\1$/, '$2') : value
        env[key] = cleanValue
    }
}
