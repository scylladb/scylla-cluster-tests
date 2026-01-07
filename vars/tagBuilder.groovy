#!groovy

import hudson.util.Secret
import com.cloudbees.plugins.credentials.CredentialsProvider
import com.google.jenkins.plugins.credentials.oauth.GoogleRobotPrivateKeyCredentials
import com.google.jenkins.plugins.credentials.oauth.GoogleOAuth2ScopeRequirement

def call() {
    def cloudInfo = identifyCloud()
    def tags = [RunByUser: "${getRunningUserId()}",
                JenkinsJobTag: "${BUILD_TAG}",
                NodeType: "builder",
                keep_action: "terminate",
                billing_project:"${GetBillingProjectTag()}"]
    applyTagsWithCreds(cloudInfo, tags)
}

private String GetBillingProjectTag() {
    // First, try to derive from the JOB_NAME
    def jobName = env.JOB_NAME
    if (jobName) {
        def releaseFolder = jobName.split('/')[0]
        if (releaseFolder == ~/^(scylla-|scylladb-)/) {
            echo "Project tag '${part}' derived from JOB_NAME."
            def prefix = ~/^(scylladb-|scylla-)/
            return releaseFolder - prefix
        }
    }

    // If not found in JOB_NAME, try the GIT_BRANCH
    def gitBranch = env.GIT_BRANCH
    if (gitBranch) {
        // The GIT_BRANCH might be prefixed with 'origin/', remove it
        def branchName = gitBranch.split('/').last()
        if (branchName == ~/"branch-\d*\.\d*/) {
            echo "Project tag '${branchName}' derived from GIT_BRANCH."
            return branchName - ~/^branch-/
        }
    }

    echo "No project tag could be derived."
    return 'no_billing_project'
}

def getRunningUserId () {
    def buildCause = currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause')
    String runningUserID = "jenkins"
    echo "Build cause: |${buildCause.toString()}|"

    if (buildCause != null && !buildCause.isEmpty()) {
        runningUserID = buildCause[0].userId
    }
    if (runningUserID?.contains('@')) {
        runningUserID = runningUserID.split('@')[0]
    }
    return runningUserID
}


/**
 * Identify the cloud environment for the current Jenkins agent.
 *
 * Detection strategy and priority:
 *  - First, try to infer the provider from local DMI / vendor files under
 *    /sys/class/dmi/id (e.g. sys_vendor, product_version, product_name,
 *    chassis_asset_tag). Based on these values, it may detect AWS, OCI,
 *    or GCE and then query the provider-specific instance metadata service
 *    to populate {@code instanceId} and {@code region}.
 *  - Additional heuristics and metadata lookups may be implemented later
 *    in this function (see implementation) to support other providers or
 *    to act as fallbacks when the DMI-based detection is inconclusive.
 *
 * Return value:
 *  - A {@link java.util.Map} with the following keys:
 *      provider   : String cloud provider identifier such as {@code 'AWS'},
 *                   {@code 'GCE'}, {@code 'OCI'}, or {@code 'UNKNOWN'}.
 *      instanceId : String instance identifier for the current VM
 *                   (empty string if unknown).
 *      region     : String region or zone for the instance, in the format
 *                   used by the underlying provider (empty string if unknown).
 *      method     : String describing the detection method that set the
 *                   values (for example {@code 'DMI_Check'} or another
 *                   heuristic), or empty string if no specific method
 *                   could be determined.
 *
 * Failure / unknown environment:
 *  - If no detection method positively identifies a supported cloud
 *    provider, the function returns the default map
 *    {@code [provider: 'UNKNOWN', instanceId: '', region: '', method: '']}.
 *  - Exceptions thrown by individual detection steps are caught and logged
 *    so they do not cause the Jenkins pipeline to fail; detection simply
 *    proceeds to the next heuristic or returns the default.
 */
def identifyCloud() {
    def info = [provider: 'UNKNOWN', instanceId: '', region: '', method: '']

    // --- METHOD 1: Local DMI/Vendor Files ---
    try {
        def sysVendor  = sh(script: "cat /sys/class/dmi/id/sys_vendor 2>/dev/null || true", returnStdout: true).trim()
        def prodVersion = sh(script: "cat /sys/class/dmi/id/product_version 2>/dev/null || true", returnStdout: true).trim()
        def chassisAsset = sh(script: "cat /sys/class/dmi/id/chassis_asset_tag 2>/dev/null || true", returnStdout: true).trim()
        def prodName   = sh(script: "cat /sys/class/dmi/id/product_name 2>/dev/null || true", returnStdout: true).trim()

        // 1. AWS Detection
        if (sysVendor == "Amazon EC2" || prodVersion.toLowerCase().contains("amazon")) {
            info.provider = 'AWS'
            info.method = 'DMI_Check'

            // --- AWS IMDSv2 FIX ---
            // 1. Try to get a Session Token (TTL 6 hours)
            // We do not use -f here so we can see if it works, but suppress errors with 2>/dev/null
            def token = sh(script: "curl -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' -s 2>/dev/null", returnStdout: true).trim()

            // 2. Prepare Header (Use token if we got one)
            def tokenHeader = token ? "-H 'X-aws-ec2-metadata-token: ${token}'" : ""

            // 3. Fetch Data (Using Token)
            info.instanceId = sh(script: "curl -fs ${tokenHeader} http://169.254.169.254/latest/meta-data/instance-id", returnStdout: true).trim()
            def az = sh(script: "curl -fs ${tokenHeader} http://169.254.169.254/latest/meta-data/placement/availability-zone", returnStdout: true).trim()

            if (az) {
                info.region = az.substring(0, az.length() - 1)
            }
            return info
        }

        // 2. OCI Detection
        if (chassisAsset == "OracleCloud.com" || sysVendor == "OracleCloud") {
            info.provider = 'OCI'
            info.method = 'DMI_Check'
            info.instanceId = sh(script: "curl -fs -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/id", returnStdout: true).trim()
            info.region = sh(script: "curl -fs -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/canonicalRegionName", returnStdout: true).trim()
            return info
        }

        // 3. GCE Detection
        if (prodName.contains("Google")) {
            info.provider = 'GCE'
            info.method = 'DMI_Check'
            info.instanceId = sh(script: "curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name", returnStdout: true).trim()
            def zonePath = sh(script: "curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone", returnStdout: true).trim()
            info.region = zonePath.split('/').last()
            return info
        }

    } catch (Exception e) {
        echo "DMI check warning: ${e.message}"
    }

    // --- METHOD 2: Network Fallback ---

    // AWS Network Check (IMDSv2 Aware)
    // Try to fetch token first
    def awsToken = sh(script: "curl -X PUT 'http://169.254.169.254/latest/api/token' -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600' --connect-timeout 1 -s 2>/dev/null", returnStdout: true).trim()
    def awsHeader = awsToken ? "-H 'X-aws-ec2-metadata-token: ${awsToken}'" : ""

    if (sh(script: "curl -fs --connect-timeout 2 ${awsHeader} http://169.254.169.254/latest/meta-data/instance-id", returnStatus: true) == 0) {
        info.provider = 'AWS'
        info.instanceId = sh(script: "curl -fs ${awsHeader} http://169.254.169.254/latest/meta-data/instance-id", returnStdout: true).trim()
        def az = sh(script: "curl -fs ${awsHeader} http://169.254.169.254/latest/meta-data/placement/availability-zone", returnStdout: true).trim()
        if (az && az.length() > 1) {
            info.region = az.substring(0, az.length() - 1)
        } else {
            echo "Warning: Unexpected AWS availability zone value '${az}', cannot derive region"
            info.region = az ?: ''
        }
        return info
    }

    // OCI Network Check
    if (sh(script: "curl -fs --connect-timeout 2 -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/", returnStatus: true) == 0) {
        info.provider = 'OCI'
        info.instanceId = sh(script: "curl -fs -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/id", returnStdout: true).trim()
        info.region = sh(script: "curl -fs -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/canonicalRegionName", returnStdout: true).trim()
        return info
    }

    // GCE Network Check
    if (sh(script: "curl -fs --connect-timeout 2 -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/id", returnStatus: true) == 0) {
        info.provider = 'GCE'
        info.instanceId = sh(script: "curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name", returnStdout: true).trim()
        def zonePath = sh(script: "curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone", returnStdout: true).trim()
        info.region = zonePath.split('/').last()
        return info
    }

    return info
}

// --- GitHub Issue #86 Fix Helper Methods ---

/**
 * Manually extracts the Google Service Account Key from Jenkins credentials
 * and writes it to a temporary file for the duration of the body closure.
 */
def withGceCredentials(String credentialsId, Closure body) {
    def serviceAccount = getCredentials(credentialsId).getServiceAccountConfig()
    def keyFile = writeKeyFile(serviceAccount.getSecretJsonKey())

    // We set the CLOUDSDK env var just in case, but pass the file path to the body for explicit use
    withEnv(["CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=${keyFile}"]) {
        try {
            body(keyFile)
        } finally {
            // Cleanup the sensitive key file
            sh "rm -f '${keyFile}'"
        }
    }
}

@NonCPS
private def getCredentials(String credentialsId) {
    def build = currentBuild.rawBuild
    return CredentialsProvider.findCredentialById(
        credentialsId,
        GoogleRobotPrivateKeyCredentials.class,
        build,
        new GoogleOAuth2ScopeRequirement() {
            @Override
            public Collection<String> getScopes() { return null; }
        }
    )
}

private def writeKeyFile(jsonKey) {
    def json
    try {
        // Try to decrypt if it's a Secret object
        json = Secret.decrypt(new String(jsonKey.getPlainData())).getPlainText()
    } catch(Exception e) {
        // Fallback if plain text
        json = new String(jsonKey.getPlainData())
    }

    def tempDir = pwd() + "/.auth"
    sh "mkdir -p '${tempDir}'"
    def tempFile = "${tempDir}/gcloud-${UUID.randomUUID().toString()}.json"

    writeFile encoding: 'UTF-8', file: tempFile, text: json
    return tempFile
}

def cleanTags(Map tags) {
    return tags.collectEntries { k, v ->
        def cleanKey = k.toString().toLowerCase().replaceAll("[^a-z0-9_-]", "-")
        def cleanValue = v.toString().toLowerCase().replaceAll("[^a-z0-9_-]", "-")
        [(cleanKey): cleanValue]
    }
}

/**
 * Apply instance tags for the current build using cloud-provider-specific CLIs
 * and Jenkins-managed credentials.
 *
 * This helper normalizes the tag map when required (for example for OCI),
 * injects the appropriate credentials for each provider, and issues the
 * corresponding CLI command to update tags/labels on the target instance.
 *
 * @param cloudInfo
 *     Cloud metadata describing the target instance. Expected keys:
 *     - provider: String identifying the cloud provider (e.g. 'AWS', 'GCE', 'OCI')
 *     - instanceId: String identifier of the instance to tag
 *     - region: String region or location used by the provider CLI
 *
 * @param tags
 *     Map of tag keys to values. Keys and values are converted to strings.
 *     For AWS, tags are passed as "Key=<key>,Value=<value>" pairs to
 *     `aws ec2 create-tags`. For GCE, equivalent labels are applied using
 *     GCP credentials and tooling (see implementation below). For OCI, tags
 *     are first normalized via {@link #cleanTags(Map)} to match OCI's
 *     restricted character set, then sent as a JSON object to
 *     `oci compute instance update --freeform-tags`.
 *
 * Behavior:
 *  - If {@code tags} is null or empty, the function logs a message and returns
 *    without performing any operation.
 *  - For each supported provider, the required Jenkins credentials are bound
 *    within the scope of the CLI invocation; no credentials are expected to
 *    be present in the environment by callers.
 */
def applyTagsWithCreds(Map cloudInfo, Map tags) {
    if (!tags || tags.isEmpty()) {
        echo "No tags provided; skipping applyTagsWithCreds"
        return
    }

    if (cloudInfo.provider == 'AWS') {
        withCredentials([[
            $class: 'AmazonWebServicesCredentialsBinding',
            credentialsId: 'jenkins2 aws account',
            accessKeyVariable: 'AWS_ACCESS_KEY_ID',
            secretKeyVariable: 'AWS_SECRET_ACCESS_KEY'
        ]]) {
            def maxTagLength = 256
            def tagsArg = tags.collect { k, v -> "Key=${k},Value=${normalizeValueLength(v, maxTagLength)}" }.join(' ')
            sh "aws ec2 create-tags --resources ${cloudInfo.instanceId} --tags ${tagsArg} --region ${cloudInfo.region}"
        }
    }
    else if (cloudInfo.provider == 'GCE') {
        def maxLabelLength = 63
        tags = cleanTags(tags)
        def gce_project = "${params.gce_project ?: 'gcp-sct-project-1'}" - "gcp-"
        withGceCredentials(gce_project) { keyFilePath ->
            sh "gcloud auth activate-service-account --key-file='${keyFilePath}'"
            def labelsArg = tags.collect { k, v -> "${k}=${normalizeValueLength(v, maxLabelLength)}" }.join(',')
            sh "gcloud compute instances add-labels ${cloudInfo.instanceId} --labels=${labelsArg} --zone=${cloudInfo.region} --quiet"
        }
    }
    else if (cloudInfo.provider == 'OCI') {
        def maxTagLength = 256
        tags = cleanTags(tags)
        withCredentials([ociCredentials(credentialsId: 'oci-sct-user')]) {
            withEnv(["OCI_CLI_REGION=${cloudInfo.region}", "OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True"]) {
                def jsonTag = tags.collect { k, v -> "\"${k}\": \"${normalizeValueLength(v, maxTagLength)}\"" }.join(',')
                def script = """
                    oci compute instance update --instance-id ${cloudInfo.instanceId} --freeform-tags '{${jsonTag}}' --force
                """
                sh script
            }
        }
    }
}

/**
 * Trim tags to specified max length as cloud providers enforce different rules.
**/
def normalizeValueLength(String label, int maxLabelLength) {
    def result = label

    if (label != null && label.length() > maxLabelLength) {
        def cuttingIndex = label.length() - maxLabelLength
        // trimming at the start is preferable to the end as it is much less interesting, like "jenkins-..."
        result = result.substring(cuttingIndex)
    }

    return result
}
