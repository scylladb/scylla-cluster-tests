#!groovy
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

def call(Map params) {
    // check if custom_config exists in the pipeline params
    if (params.containsKey('custom_config') && params.custom_config) {
        // Write custom config to file
        def customConfigPath = 'custom_config.yaml'
        writeFile file: customConfigPath, text: params.custom_config.toString()

        // Parse test_config into a list
        def testConfigRaw = params.test_config.toString()
        def testConfigList
        if (testConfigRaw.startsWith('[')) {
                // JSON list
                testConfigList = new JsonSlurper().parseText(testConfigRaw)
            } else {
                // Single string => convert to list
                testConfigList = [testConfigRaw]
            }

        // Add custom config path to list
        testConfigList << customConfigPath

        // Convert back to JSON string
        env.TEST_CONFIG = "'${JsonOutput.toJson(testConfigList)}'"
    } else {
        // If custom_config is not provided, use the original test_config
        env.TEST_CONFIG = "'${params.test_config}'"
    }
}
