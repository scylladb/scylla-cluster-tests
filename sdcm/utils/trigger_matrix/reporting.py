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

"""Email reports for wait-mode trigger runs and the arch audit."""

import html
import logging
import re
from pathlib import Path

from sdcm.utils.cloud_monitor.cloud_monitor import Email
from sdcm.utils.trigger_matrix.audit import AuditReport
from sdcm.utils.trigger_matrix.constants import DEFAULT_EMAIL_RECIPIENTS
from sdcm.utils.trigger_matrix.models import BuildResult

logger = logging.getLogger(__name__)


def send_trigger_matrix_email(
    build_results: list[BuildResult],
    scylla_version: str,
    matrix_file: str,
    email_recipients: list[str] | None = None,
    trigger_job_url: str | None = None,
    run_params: dict | None = None,
    subject_params: list[str] = (),
) -> None:
    """Send an email report summarizing wait-mode build results.

    Args:
        build_results: List of BuildResult from JenkinsClient.wait_for_builds.
        scylla_version: Version that was tested.
        matrix_file: Path to the matrix YAML that was used.
        email_recipients: List of email addresses. Falls back to DEFAULT_EMAIL_RECIPIENTS.
        run_params: Parameters the run passed to every job, listed in the report.
        subject_params: Names of `run_params` whose value names what was tested; the last path
            segment of each value goes into the subject, e.g. the file name of a tarball URL.
    """
    recipients = email_recipients or DEFAULT_EMAIL_RECIPIENTS
    if not recipients:
        logger.warning("No email recipients configured — skipping email report")
        return

    all_passed = all(r.success for r in build_results)
    status = "PASSED" if all_passed else "FAILED"

    run_params = {k: v for k, v in (run_params or {}).items() if v is not None and v != ""}
    tested = [str(run_params[name]).rstrip("/").rsplit("/", 1)[-1] for name in subject_params if name in run_params]
    subject = f"[Trigger Matrix] {status} — {Path(matrix_file).stem}: {', '.join([*tested, scylla_version])}"

    trigger_line = (
        f'<b>Trigger job:</b> <a href="{trigger_job_url}">View trigger run</a><br/>' if trigger_job_url else ""
    )

    params_lines = "".join(
        f"<b>{html.escape(name)}:</b> {html.escape(str(value))}<br/>" for name, value in run_params.items()
    )

    body = f"""<html><body>
<h2>Trigger Matrix Results — {status}</h2>
<p><b>Version:</b> {scylla_version}<br/>
<b>Matrix:</b> {matrix_file}<br/>
{params_lines}{trigger_line}<b>Overall:</b> {status}</p>
{_results_table(build_results)}
{_failures_section(build_results)}
</body></html>"""

    try:
        email_client = Email()
        email_client.send(subject=subject, content=body, recipients=recipients, html=True)
        logger.info("Email report sent to %s", recipients)
    except Exception as exc:  # noqa: BLE001 - email failure is non-fatal
        logger.warning("Failed to send email report: %s", exc)


def _result_cell(result: BuildResult | None) -> str:
    """One build of the results table: its result linked to the build, then its artifacts."""
    if result is None:
        return "—"
    label = f"{'✅' if result.success else '❌'} {result.result}"
    cell = f'<a href="{result.build_url}">{label}</a> #{result.build_number}' if result.build_url else label
    for url in result.artifacts:
        cell += f'<br/><small><a href="{url}">{html.escape(url.rsplit("/", 1)[-1])}</a></small>'
    return cell


def _results_table(build_results: list[BuildResult]) -> str:
    """The builds as a grid: a row per job, a column per Scylla version."""
    versions = list(dict.fromkeys(result.scylla_version for result in build_results))
    jobs = list(dict.fromkeys(result.job_name for result in build_results))
    by_cell = {(result.job_name, result.scylla_version): result for result in build_results}
    header = "".join(f"<th>{html.escape(version or 'Result')}</th>" for version in versions)
    rows = "".join(
        f"<tr><td>{html.escape(job)}</td>"
        + "".join(f"<td>{_result_cell(by_cell.get((job, version)))}</td>" for version in versions)
        + "</tr>\n"
        for job in jobs
    )
    return f"""<table border="1" cellpadding="5" cellspacing="0">
<tr><th>Job</th>{header}</tr>
{rows}</table>"""


def _description_html(description: str) -> str:
    """A build's Jenkins description, made safe for the report.

    SCT jobs set it to an HTML block linking to their Argus run. Its links are kept as links,
    with their text alone; a description without links is shown as plain text.
    """

    def text(markup: str) -> str:
        return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", markup)).split())

    links = re.findall(
        r"<a\b[^>]*\bhref=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", description, flags=re.DOTALL | re.IGNORECASE
    )
    if links:
        return " ".join(f'<a href="{html.escape(url)}">{html.escape(text(label) or url)}</a>' for url, label in links)
    return html.escape(text(description))


def _failures_section(build_results: list[BuildResult]) -> str:
    """What went wrong in each failed build, or a banner saying all builds passed."""
    failed = [result for result in build_results if not result.success]
    if not failed:
        return "<p><b>✅ All builds PASSED.</b></p>"
    items = []
    for result in failed:
        version = f" ({html.escape(result.scylla_version)})" if result.scylla_version else ""
        link = f' — <a href="{result.build_url}">View build</a>' if result.build_url else ""
        reason = f"<br/><small>{_description_html(result.description)}</small>" if result.description else ""
        items.append(f"<li><b>{html.escape(result.job_name)}</b>{version} — {result.result}{link}{reason}</li>")
    return f"<h3>Failures</h3>\n<ul>{''.join(items)}</ul>"


def send_arch_audit_email(report: AuditReport, recipients: list[str], job_url: str | None = None) -> None:
    """Email the trigger-matrix entries whose declared arch differs from what their job runs on."""
    rows = "".join(
        f"<tr><td>{d.matrix}</td><td>{html.escape(d.job_name)}</td><td>{', '.join(d.versions)}</td>"
        f"<td>{d.declared}</td><td><b>{d.actual}</b></td><td>{html.escape(d.instance)}</td></tr>"
        for d in report.drifts
    )
    unchecked = "".join(
        f"<li>{u.matrix}: {html.escape(u.job_name)} ({', '.join(u.versions)}) - {html.escape(u.reason)}</li>"
        for u in report.unchecked
    )
    job_line = f'<p><a href="{job_url}">Audit run</a></p>' if job_url else ""
    body = f"""<html><body>
<h2>Trigger matrix arch drift - {len(report.drifts)} entries</h2>
<p>These entries declare an arch their job does not run on, for the listed releases (checked against
each release's SCT branch: {", ".join(report.versions)}). The x86 and ARM release runs therefore trigger
them from the wrong run, or twice. Fix the entry's <code>arch</code>, splitting it by version range where
releases differ - see docs/testing/trigger-matrix.md, "Architecture".</p>
{job_line}
<table border="1" cellpadding="5" cellspacing="0">
<tr><th>Matrix</th><th>Job</th><th>Releases</th><th>Declared</th><th>Runs on</th><th>DB instance</th></tr>
{rows}
</table>
<h3>Not checked</h3><ul>{unchecked or "<li>none</li>"}</ul>
</body></html>"""
    subject = f"[Trigger Matrix] arch drift in {len(report.drifts)} entries"
    Email().send(subject=subject, content=body, recipients=recipients, html=True)
    logger.info("Arch audit report sent to %s", recipients)
