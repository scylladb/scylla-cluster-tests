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

"""Email report for wait-mode trigger runs."""

import logging

from sdcm.utils.trigger_matrix.constants import DEFAULT_EMAIL_RECIPIENTS
from sdcm.utils.trigger_matrix.models import BuildResult

logger = logging.getLogger(__name__)


def send_trigger_matrix_email(
    build_results: list[BuildResult],
    scylla_version: str,
    matrix_file: str,
    email_recipients: list[str] | None = None,
    trigger_job_url: str | None = None,
) -> None:
    """Send an email report summarizing wait-mode build results.

    Args:
        build_results: List of BuildResult from JenkinsClient.wait_for_builds.
        scylla_version: Version that was tested.
        matrix_file: Path to the matrix YAML that was used.
        email_recipients: List of email addresses. Falls back to DEFAULT_EMAIL_RECIPIENTS.
    """
    recipients = email_recipients or DEFAULT_EMAIL_RECIPIENTS
    if not recipients:
        logger.warning("No email recipients configured — skipping email report")
        return

    all_passed = all(r.success for r in build_results)
    status = "PASSED" if all_passed else "FAILED"

    subject = f"[Trigger Matrix] {status} — scylla-doctor gating ({scylla_version})"

    trigger_line = (
        f'<b>Trigger job:</b> <a href="{trigger_job_url}">View trigger run</a><br/>' if trigger_job_url else ""
    )

    rows = []
    for result in build_results:
        emoji = "✅" if result.success else "❌"
        artifacts_str = ", ".join(result.artifacts) if result.artifacts else "—"
        job_cell = (
            f'<a href="{result.build_url}">{emoji} {result.job_name}</a>'
            if result.build_url
            else f"{emoji} {result.job_name}"
        )
        build_cell = (
            f'<a href="{result.build_url}">#{result.build_number}</a>'
            if result.build_url
            else f"#{result.build_number}"
        )
        rows.append(
            f"<tr><td>{job_cell}</td><td>{build_cell}</td><td><b>{result.result}</b></td><td>{artifacts_str}</td></tr>"
        )

    body = f"""<html><body>
<h2>Trigger Matrix Results — {status}</h2>
<p><b>Version:</b> {scylla_version}<br/>
<b>Matrix:</b> {matrix_file}<br/>
{trigger_line}<b>Overall:</b> {status}</p>
<table border="1" cellpadding="5" cellspacing="0">
<tr><th>Job</th><th>Build</th><th>Result</th><th>Artifacts</th></tr>
{"".join(rows)}
</table>
</body></html>"""

    try:
        from sdcm.utils.cloud_monitor.cloud_monitor import Email  # noqa: PLC0415 - optional dependency

        email_client = Email()
        email_client.send(subject=subject, content=body, recipients=recipients, html=True)
        logger.info("Email report sent to %s", recipients)
    except Exception as exc:  # noqa: BLE001 - email failure is non-fatal
        logger.warning("Failed to send email report: %s", exc)
