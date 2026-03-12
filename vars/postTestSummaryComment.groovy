#!groovy

/**
 * Post a structured test/precommit summary as a PR comment.
 *
 * Parses JUnit XML files and pre-commit output, then posts (or updates) a
 * Markdown comment on the PR with summary tables and per-failure details.
 * The comment is designed to be parsed by LLM agents for automated triage.
 *
 * Uses an HTML marker so that repeated runs update the same comment instead of
 * creating new ones.
 *
 * @param args  Map with keys:
 *   - junitXmlPaths:  List of paths to JUnit XML files (optional)
 *   - precommitLog:   Path to pre-commit output log file (optional)
 *   - stageName:      Human-readable name for the comment heading
 */
def call(Map args) {
    if (!env.CHANGE_ID) {
        echo "postTestSummaryComment: not a PR build, skipping"
        return
    }

    def junitXmlPaths = args.get('junitXmlPaths', [])
    def precommitLog = args.get('precommitLog', '')
    def stageName = args.get('stageName', 'Test Summary')

    def existingXmls = junitXmlPaths.findAll { fileExists(it) }
    def hasPrecommitLog = precommitLog && fileExists(precommitLog)

    if (!existingXmls && !hasPrecommitLog) {
        echo "postTestSummaryComment: no result files found, skipping"
        return
    }

    def cmdParts = ["./docker/env/hydra.sh python sdcm/utils/junit_summary.py"]

    if (existingXmls) {
        def xmlArgs = existingXmls.collect { "'${it}'" }.join(" ")
        cmdParts.add("--junit-xml ${xmlArgs}")
    }

    if (hasPrecommitLog) {
        cmdParts.add("--precommit-log '${precommitLog}'")
    }

    cmdParts.add("--stage-name '${stageName}'")
    cmdParts.add("--build-url '${env.BUILD_URL}'")

    def summary = sh(
        script: "#!/bin/bash\n" + cmdParts.join(" \\\n    "),
        returnStdout: true,
    ).trim()

    if (!summary) {
        echo "postTestSummaryComment: empty summary output, skipping"
        return
    }

    def marker = "<!-- sct-test-summary -->"

    // Update existing comment if one exists, otherwise create a new one
    def existingComment = pullRequest.comments.find { it.body.contains(marker) }
    if (existingComment) {
        existingComment.body = summary
    } else {
        pullRequest.comment(summary)
    }
}
