# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""The Groovy script the trigger matrix runs on Jenkins' Script Console.

Builds are scheduled through /scriptText rather than the REST build API, which omits the
'Location' header and makes python-jenkins retry -- triggering the same job several times.

The script text is an f-string with literal Groovy braces doubled. Nothing here is covered by
a unit test, so treat it as it is: change it only deliberately."""

import logging
import os

logger = logging.getLogger(__name__)


def _escape_groovy_string(value: str) -> str:
    """Escape a value for embedding in a Groovy single-quoted string literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_trigger_groovy(job_name: str, parameters: dict, return_queue_url: bool = False) -> str:
    """Build Groovy script that triggers a Jenkins job with UpstreamCause.

    Uses Jenkins Script Console to schedule a build, bypassing the REST API
    'Location' header issue (https://github.com/bndr/gojenkins/issues/248).
    Injects UpstreamCause so Jenkins UI shows 'Started by upstream project X build Y'.

    Args:
        job_name: Full Jenkins job path (slash-separated).
        parameters: Parameters to pass to the job.
        return_queue_url: If True, return queue item URL (for wait-mode polling).
            If False, return the job URL (fire-and-forget).

    Returns:
        Groovy script string.
    """
    escaped_job_name = _escape_groovy_string(job_name)
    params_groovy = ", ".join(
        f"new StringParameterValue('{_escape_groovy_string(k)}', '{_escape_groovy_string(str(v))}')"
        for k, v in parameters.items()
    )

    upstream_job = os.environ.get("JOB_NAME", "")
    upstream_build = os.environ.get("BUILD_NUMBER", "")

    if upstream_job and upstream_build:
        escaped_upstream = _escape_groovy_string(upstream_job)
        cause_section = f"""
def upstreamJob = Jenkins.instance.getItemByFullName('{escaped_upstream}')
def upstreamBuild = upstreamJob?.getBuildByNumber({upstream_build})
def cause
if (upstreamBuild) {{
    cause = new hudson.model.Cause.UpstreamCause((hudson.model.Run) upstreamBuild)
}} else {{
    cause = new hudson.model.Cause.RemoteCause('{escaped_upstream}', 'Build #{upstream_build} (upstream build not found)')
}}
"""
    else:
        cause_section = """
def cause = new hudson.model.Cause.RemoteCause('trigger-matrix', 'Triggered via SCT trigger-matrix CLI')
"""

    if return_queue_url:
        # Return the queue item URL for wait-mode polling via _wait_for_build_start()
        output_section = """
def rootUrl = Jenkins.instance.rootUrl ?: ''
def queueItem = Jenkins.instance.queue.getItems().find { it.task == targetJob }
if (queueItem) {
    println "TRIGGERED:${rootUrl}queue/item/${queueItem.id}/"
} else {
    println "TRIGGERED:${targetJob.getAbsoluteUrl()}"
}
"""
    else:
        output_section = """
println "TRIGGERED:${targetJob.getAbsoluteUrl()}"
"""

    return f"""
import jenkins.model.Jenkins
import hudson.model.*

def targetJob = Jenkins.instance.getItemByFullName('{escaped_job_name}')
if (!targetJob) {{
    println "ERROR: Job not found: {escaped_job_name}"
    return
}}
{cause_section}
def params = [{params_groovy}]
def actions = []
if (params) {{
    actions.add(new ParametersAction(params))
}}
actions.add(new CauseAction(cause))

def future = targetJob.scheduleBuild2(0, actions.toArray(new Action[0]))
if (future != null) {{
{output_section}
}} else {{
    println "ERROR: scheduleBuild2 returned null for {escaped_job_name}"
}}
"""
