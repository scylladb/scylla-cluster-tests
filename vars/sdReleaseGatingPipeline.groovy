#!groovy

import groovy.json.JsonSlurperClassic
import groovy.json.JsonOutput

/**
 * Scylla Doctor Release Gating Pipeline
 *
 * Triggers all artifact tests in parallel for each supported Scylla version × OS variant
 * combination, passing SD-specific parameters (version or tarball URL) and the SD-only mode flag.
 *
 * After all jobs complete, aggregates results into a summary table showing pass/fail per
 * OS variant × Scylla version, with links to individual job logs for failed runs.
 */
def call(Map pipelineParams = [:]) {

    // Artifact test job definitions: each entry maps a human-readable OS label to the
    // Jenkins job path (relative to the current folder) and backend-specific settings.
    // The job paths use '..' prefix to reference sibling folders in the Jenkins job tree.
    def ARTIFACT_TEST_JOBS = [
        [
            os_label: 'centos9',
            job_path: 'artifacts/artifacts-centos9',
            backend: 'gce',
        ],
        [
            os_label: 'rocky9',
            job_path: 'artifacts/artifacts-rocky9',
            backend: 'gce',
        ],
        [
            os_label: 'ubuntu2204',
            job_path: 'artifacts/artifacts-ubuntu2204',
            backend: 'gce',
        ],
        [
            os_label: 'ubuntu2404',
            job_path: 'artifacts/artifacts-ubuntu2404',
            backend: 'gce',
        ],
        [
            os_label: 'debian12',
            job_path: 'artifacts/artifacts-debian12',
            backend: 'gce',
        ],
        [
            os_label: 'ami',
            job_path: 'artifacts/artifacts-ami',
            backend: 'aws',
        ],
        [
            os_label: 'docker',
            job_path: 'artifacts/artifacts-docker',
            backend: 'docker',
        ],
        [
            os_label: 'rhel10',
            job_path: 'artifacts/artifacts-rhel10',
            backend: 'aws',
        ],
        [
            os_label: 'oel9',
            job_path: 'artifacts/artifacts-oel9',
            backend: 'aws',
        ],
        [
            os_label: 'amazon2023',
            job_path: 'artifacts/artifacts-amazon2023',
            backend: 'aws',
        ],
        [
            os_label: 'centos9-arm',
            job_path: 'artifacts/artifacts-centos9-arm',
            backend: 'aws',
        ],
        [
            os_label: 'ubuntu2204-arm',
            job_path: 'artifacts/artifacts-ubuntu2204-arm',
            backend: 'aws',
        ],
        [
            os_label: 'ubuntu2404-arm',
            job_path: 'artifacts/artifacts-ubuntu2404-arm',
            backend: 'aws',
        ],
        [
            os_label: 'ami-arm',
            job_path: 'artifacts/artifacts-ami-arm',
            backend: 'aws',
        ],
    ]

    def builder = getJenkinsLabels('aws', 'eu-west-1')

    pipeline {
        agent {
            label {
                label builder.label
            }
        }

        environment {
            AWS_ACCESS_KEY_ID     = credentials('qa-aws-secret-key-id')
            AWS_SECRET_ACCESS_KEY = credentials('qa-aws-secret-access-key')
        }

        parameters {
            separator(name: 'SD_CONFIG', sectionHeader: 'Scylla Doctor Configuration')
            string(name: 'sd_tarball_url', defaultValue: '',
                   description: '(Required) Direct URL to a Scylla Doctor tarball in S3. Downloads SD directly from this URL, bypassing the standard version-based S3 lookup.')

            separator(name: 'SCYLLA_CONFIG', sectionHeader: 'Scylla Version Configuration')
            string(name: 'scylla_versions', defaultValue: '',
                   description: 'Space-separated list of Scylla versions to test (e.g., "2025.1.12 2026.1.1"). Leave empty to auto-discover the latest patch of each supported enterprise release branch.')

            separator(name: 'JOB_CONFIG', sectionHeader: 'Job Configuration')
            string(name: 'os_filter', defaultValue: '',
                   description: 'Comma-separated list of OS labels to test (e.g., "centos9,ubuntu2204,docker"). Leave empty to test all OS variants.')
            string(name: 'post_behavior_db_nodes', defaultValue: 'destroy',
                   description: 'keep|keep-on-failure|destroy')
            string(name: 'provision_type', defaultValue: 'spot',
                   description: 'on_demand|spot|spot_fleet')

            separator(name: 'NOTIFICATION_CONFIG', sectionHeader: 'Notification Configuration')
            string(name: 'email_recipients', defaultValue: 'qa@scylladb.com',
                   description: 'Email recipients for the gating summary report')
            string(name: 'requested_by_user', defaultValue: '',
                   description: 'Actual user requesting job start, for automated job builds')
        }

        options {
            timestamps()
            buildDiscarder(logRotator(numToKeepStr: '30'))
            timeout(time: 180, unit: 'MINUTES')
        }

        stages {
            stage('Preparation') {
                when { expression { env.BUILD_NUMBER == '1' } }
                steps {
                    script {
                        if (currentBuild.getBuildCauses('hudson.model.Cause$UserIdCause') != null) {
                            currentBuild.description = ('Aborted build#1 not having parameters loaded. \n'
                              + 'Build#2 is ready to run')
                            currentBuild.result = 'ABORTED'
                            error('Abort build#1 which only loads params')
                        }
                    }
                }
            }

            stage('Validate Parameters') {
                steps {
                    script {
                        if (!params.sd_tarball_url?.trim()) {
                            error("'sd_tarball_url' is required. Provide a direct URL to a Scylla Doctor tarball in S3.")
                        }
                    }
                }
            }

            stage('Checkout') {
                steps {
                    dir('scylla-cluster-tests') {
                        timeout(time: 5, unit: 'MINUTES') {
                            checkout scm
                        }
                    }
                }
            }

            stage('Discover Scylla Versions') {
                steps {
                    script {
                        env.DISCOVERED_VERSIONS = ''

                        if (params.scylla_versions?.trim()) {
                            // Use explicitly provided versions
                            env.DISCOVERED_VERSIONS = params.scylla_versions.trim()
                            println("Using explicitly provided Scylla versions: ${env.DISCOVERED_VERSIONS}")
                        } else {
                            // Auto-discover supported enterprise release versions using hydra
                            dir('scylla-cluster-tests') {
                                def output = sh(
                                    returnStdout: true,
                                    script: '''#!/bin/bash
                                        set -e
                                        cat > discover_versions.py << 'PYTHON_EOF'
import json
from sdcm.utils.version_utils import get_s3_scylla_repos_mapping, get_all_versions, is_enterprise

repo_map = get_s3_scylla_repos_mapping('centos')
enterprise_branches = sorted(
    [v for v in repo_map.keys() if is_enterprise(v)],
    key=lambda x: [int(p) for p in x.split('.')]
)

# Keep only the last 2 enterprise release branches (LTS + latest)
branches_to_test = enterprise_branches[-2:] if len(enterprise_branches) >= 2 else enterprise_branches

result = {}
for branch in branches_to_test:
    versions = get_all_versions(repo_map[branch])
    # Filter out rc versions and get the latest patch
    release_versions = sorted(
        [v for v in versions if 'rc' not in v],
        key=lambda x: [int(p) for p in x.split('.')]
    )
    if release_versions:
        result[branch] = release_versions[-1]

print('SD_VERSIONS_JSON=' + json.dumps(result))
PYTHON_EOF
                                        ./docker/env/hydra.sh python -u discover_versions.py
                                    '''
                                ).trim()

                                println("Hydra version discovery output:\n${output}")
                                def versionLine = output.split('\n').find { it.startsWith('SD_VERSIONS_JSON=') }
                                if (versionLine) {
                                    def versionJson = versionLine.replace('SD_VERSIONS_JSON=', '')
                                    def versionMap = new JsonSlurperClassic().parseText(versionJson)
                                    env.DISCOVERED_VERSIONS = versionMap.values().join(' ')
                                    env.DISCOVERED_VERSIONS_JSON = versionJson
                                    println("Auto-discovered Scylla versions: ${env.DISCOVERED_VERSIONS}")
                                    println("Version map (branch → latest patch): ${versionJson}")
                                } else {
                                    error("Failed to auto-discover Scylla versions. Output: ${output}")
                                }
                            }
                        }

                        if (!env.DISCOVERED_VERSIONS?.trim()) {
                            error("No Scylla versions discovered or provided. Cannot proceed.")
                        }
                    }
                }
            }

            stage('Run Artifact Tests') {
                steps {
                    script {
                        def scyllaVersions = env.DISCOVERED_VERSIONS.trim().split('\\s+')
                        def osFilter = params.os_filter?.trim() ? params.os_filter.trim().split(',').collect { it.trim() } : []

                        // Filter OS variants if os_filter is specified
                        def jobsToRun = ARTIFACT_TEST_JOBS
                        if (osFilter) {
                            jobsToRun = ARTIFACT_TEST_JOBS.findAll { osFilter.contains(it.os_label) }
                            println("Filtered OS variants: ${jobsToRun*.os_label}")
                        }

                        // Track results: key = "os_label/scylla_version", value = result map
                        def results = Collections.synchronizedMap([:])
                        def parallelJobs = [:]

                        for (def jobDef in jobsToRun) {
                            for (def version in scyllaVersions) {
                                def osLabel = jobDef.os_label
                                def jobPath = jobDef.job_path
                                def stageKey = "${osLabel}/${version}"
                                def scyllaVersion = version

                                parallelJobs[stageKey] = {
                                    def jobResult = [
                                        os: osLabel,
                                        version: scyllaVersion,
                                        status: 'NOT_RUN',
                                        url: '',
                                    ]
                                    try {
                                        // Resolve job path relative to current folder
                                        def currentJobDir = JOB_NAME.substring(0, JOB_NAME.lastIndexOf('/'))
                                        def fullJobPath = "${currentJobDir}/../${jobPath}"

                                        println("Triggering ${stageKey}: job=${fullJobPath}, version=${scyllaVersion}")

                                        def triggered = build(
                                            job: fullJobPath,
                                            wait: true,
                                            propagate: false,
                                            parameters: [
                                                string(name: 'scylla_version', value: scyllaVersion),
                                                string(name: 'scylla_doctor_tarball_url', value: params.sd_tarball_url.trim()),
                                                string(name: 'scylla_doctor_edition', value: 'full'),
                                                booleanParam(name: 'run_scylla_doctor_only', value: true),
                                                string(name: 'post_behavior_db_nodes', value: params.post_behavior_db_nodes),
                                                string(name: 'provision_type', value: params.provision_type),
                                                string(name: 'email_recipients', value: ''),  // Suppress per-job emails
                                                string(name: 'requested_by_user', value: params.requested_by_user),
                                            ]
                                        )
                                        jobResult.status = triggered.result ?: 'UNKNOWN'
                                        jobResult.url = triggered.absoluteUrl ?: ''
                                    } catch (Exception e) {
                                        jobResult.status = 'FAILURE'
                                        jobResult.url = ''
                                        println("Error triggering ${stageKey}: ${e.message}")
                                    }
                                    results[stageKey] = jobResult
                                }
                            }
                        }

                        // Execute all jobs in parallel
                        parallel parallelJobs

                        // Store results for the report stage
                        env.GATING_RESULTS_JSON = JsonOutput.toJson(results)
                    }
                }
            }

            stage('Generate Report') {
                steps {
                    script {
                        def results = new JsonSlurperClassic().parseText(env.GATING_RESULTS_JSON)
                        def scyllaVersions = env.DISCOVERED_VERSIONS.trim().split('\\s+')

                        // Collect unique OS labels preserving order
                        def osLabels = results.values().collect { it.os }.unique()

                        def sdVersionDisplay = "tarball: ${params.sd_tarball_url}"

                        def hasFailures = results.values().any { it.status != 'SUCCESS' }
                        def overallStatus = hasFailures ? 'FAILED' : 'PASSED'

                        // Build summary table
                        def report = new StringBuilder()
                        report.append("=".multiply(80) + "\n")
                        report.append("SCYLLA DOCTOR RELEASE GATING REPORT\n")
                        report.append("=".multiply(80) + "\n\n")
                        report.append("SD Version: ${sdVersionDisplay}\n")
                        report.append("Overall Status: ${overallStatus}\n")
                        report.append("Scylla Versions Tested: ${scyllaVersions.join(', ')}\n\n")

                        // Table header
                        def colWidth = 15
                        def osColWidth = 20
                        report.append(String.format("%-${osColWidth}s", 'OS'))
                        for (def ver in scyllaVersions) {
                            report.append(String.format("%-${colWidth}s", ver))
                        }
                        report.append('\n')
                        report.append('-'.multiply(osColWidth + colWidth * scyllaVersions.length) + '\n')

                        // Table rows
                        for (def os in osLabels) {
                            report.append(String.format("%-${osColWidth}s", os))
                            for (def ver in scyllaVersions) {
                                def key = "${os}/${ver}"
                                def result = results[key]
                                def statusIcon = 'N/A'
                                if (result) {
                                    switch (result.status) {
                                        case 'SUCCESS': statusIcon = '✅ PASS'; break
                                        case 'FAILURE': statusIcon = '❌ FAIL'; break
                                        case 'UNSTABLE': statusIcon = '⚠️  UNSTABLE'; break
                                        case 'ABORTED': statusIcon = '⏹️  ABORTED'; break
                                        default: statusIcon = "? ${result.status}"; break
                                    }
                                }
                                report.append(String.format("%-${colWidth}s", statusIcon))
                            }
                            report.append('\n')
                        }

                        report.append('\n')

                        // Failed jobs details
                        def failedJobs = results.findAll { k, v -> v.status != 'SUCCESS' }
                        if (failedJobs) {
                            report.append("FAILURES:\n")
                            report.append('-'.multiply(80) + '\n')
                            failedJobs.each { key, result ->
                                report.append("  ${result.os} × ${result.version} — ${result.status}\n")
                                if (result.url) {
                                    report.append("    → ${result.url}\n")
                                }
                            }
                        } else {
                            report.append("All tests PASSED. SD release is approved.\n")
                        }

                        report.append('\n' + '='.multiply(80) + '\n')

                        def reportStr = report.toString()
                        println(reportStr)

                        // Write report to file for archiving
                        writeFile file: 'sd-gating-report.txt', text: reportStr

                        // Set build description with summary
                        def passCount = results.count { k, v -> v.status == 'SUCCESS' }
                        def totalCount = results.size()
                        currentBuild.description = "SD ${sdVersionDisplay} | ${overallStatus} | ${passCount}/${totalCount} passed"

                        // Fail the build if any test failed
                        if (hasFailures) {
                            currentBuild.result = 'FAILURE'
                        }
                    }
                }
            }

            stage('Send Notification') {
                when {
                    expression { params.email_recipients?.trim() }
                }
                steps {
                    script {
                        def sdVersionDisplay = "tarball: ${params.sd_tarball_url}"

                        def results = new JsonSlurperClassic().parseText(env.GATING_RESULTS_JSON)
                        def hasFailures = results.values().any { it.status != 'SUCCESS' }
                        def status = hasFailures ? 'FAILED' : 'PASSED'

                        emailext(
                            subject: "SD Release Gating ${status}: ${sdVersionDisplay}",
                            body: readFile('sd-gating-report.txt'),
                            to: params.email_recipients,
                            mimeType: 'text/plain'
                        )
                    }
                }
            }
        }

        post {
            always {
                archiveArtifacts artifacts: 'sd-gating-report.txt', allowEmptyArchive: true
            }
        }
    }
}
