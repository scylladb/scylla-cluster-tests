#!groovy
import groovy.json.JsonOutput
import groovy.json.JsonSlurperClassic

// The SCT_* environment of the job, from `sct.py pipeline env` (SCT-1492), as "NAME=value" strings for
// withEnv(). Generated from the SCT configuration: each option is set by the job parameter of the same name
// (or the one its SctField `jenkins_param` names). No shell quoting is involved.
//
//   withEnv(pipelineEnv(params: params, region: builder.region, pipelineParams: pipelineParams)) { sh ... }
//
// `inputs` adds values that are not job parameters, keyed by SCT option name.
def call(Map args) {
    def input = [
        params: (args.params ?: [:]) + (args.inputs ?: [:]),
        pipeline_params: args.pipelineParams ?: [:],
        builder_region: args.region ?: '',
    ]
    def name = "pipeline_env_${UUID.randomUUID()}"
    writeFile(file: "${name}_params.json", text: JsonOutput.toJson(input))
    try {
        sh "./docker/env/hydra.sh pipeline env --params-file ${name}_params.json --json --output ${name}.json"
        return toEnvList(readFile("${name}.json"))
    } finally {
        sh "rm -f ${name}_params.json ${name}.json"
    }
}

@NonCPS
def toEnvList(String text) {
    return new JsonSlurperClassic().parseText(text).collect { name, value -> "${name}=${value}".toString() }
}
