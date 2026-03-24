"""Parse JUnit XML and pre-commit output into a structured Markdown summary for PR comments.

The output format uses explicit headings, code blocks, and structured fields so that
LLM agents (Claude Code, Copilot) can reliably extract test names, file paths, and
tracebacks to automate failure triage.
"""

import argparse
import logging
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# GitHub PR comment body limit is 65536 characters.  Reserve some room for
# the summary table and per-failure overhead so we don't silently truncate.
MAX_COMMENT_CHARS = 60000
MAX_TRACEBACK_CHARS = 2000

COMMENT_MARKER = "<!-- sct-test-summary -->"


@dataclass
class FailureDetail:
    test: str
    file: str
    failure_type: str
    message: str
    details: str


@dataclass
class SuiteResults:
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    failures: list[FailureDetail] = field(default_factory=list)


def parse_junit_xml(file_path: Path) -> SuiteResults:
    """Parse a JUnit XML file into a structured SuiteResults object.

    Args:
        file_path: Path to the JUnit XML file.

    Returns:
        Parsed test results with counts and failure details.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    suites = root.findall(".//testsuite") if root.tag == "testsuites" else [root]
    results = SuiteResults()

    for suite in suites:
        for testcase in suite.findall("testcase"):
            failure = testcase.find("failure")
            error = testcase.find("error")
            skipped = testcase.find("skipped")

            fault = None
            if failure is not None:
                results.failed += 1
                fault = ("FAILURE", failure)
            elif error is not None:
                results.errors += 1
                fault = ("ERROR", error)

            if fault:
                fault_type, element = fault
                results.failures.append(
                    FailureDetail(
                        test=f"{testcase.get('classname', '')}.{testcase.get('name', '')}",
                        file=testcase.get("file", ""),
                        failure_type=fault_type,
                        message=element.get("message", ""),
                        details=(element.text or "")[:MAX_TRACEBACK_CHARS],
                    )
                )
            elif skipped is not None:
                results.skipped += 1
            else:
                results.passed += 1

    return results


def merge_results(results_list: list[SuiteResults]) -> SuiteResults:
    """Merge multiple SuiteResults into a single aggregate.

    Args:
        results_list: List of SuiteResults to combine.

    Returns:
        Combined SuiteResults with summed counts and concatenated failures.
    """
    merged = SuiteResults()
    for results in results_list:
        merged.passed += results.passed
        merged.failed += results.failed
        merged.errors += results.errors
        merged.skipped += results.skipped
        merged.failures.extend(results.failures)
    return merged


@dataclass
class PrecommitHookResult:
    hook: str
    status: str  # "Passed", "Failed", "Skipped"
    details: str  # output lines after the status line (diffs, error messages)


# Matches lines like:
#   "ruff-format..........Failed"
#   "trailing whitespace...Passed"
#   "check yaml...................(no files to check)Skipped"
_HOOK_STATUS_RE = re.compile(r"^(?P<hook>.+?)\.{2,}(?:\([^)]*\))?(?P<status>Passed|Failed|Skipped)\s*$")


def parse_precommit_log(log_text: str) -> list[PrecommitHookResult]:
    """Parse pre-commit run output into structured hook results.

    Pre-commit outputs one status line per hook (``hook_name...Passed/Failed/Skipped``)
    followed by detail lines for failures.  This function extracts each hook's
    status and captures the detail block for failed hooks.

    Args:
        log_text: Raw stdout/stderr from ``pre-commit run -a``.

    Returns:
        List of per-hook results, preserving execution order.
    """
    results: list[PrecommitHookResult] = []
    current_hook: PrecommitHookResult | None = None
    detail_lines: list[str] = []

    def _finalize_hook():
        if current_hook is not None:
            current_hook.details = "\n".join(detail_lines).strip()
            results.append(current_hook)

    for line in log_text.splitlines():
        match = _HOOK_STATUS_RE.match(line)
        if match:
            _finalize_hook()
            detail_lines = []
            current_hook = PrecommitHookResult(
                hook=match.group("hook").strip(),
                status=match.group("status"),
                details="",
            )
        elif current_hook is not None and current_hook.status == "Failed":
            detail_lines.append(line)

    _finalize_hook()

    return results


def format_precommit_markdown(hooks: list[PrecommitHookResult], build_url: str = "") -> str:
    """Format pre-commit hook results as Markdown for a PR comment section.

    Args:
        hooks: Parsed hook results from ``parse_precommit_log``.
        build_url: URL to the full Jenkins build log.

    Returns:
        Markdown string for the precommit section of the PR comment.
    """
    passed = sum(1 for h in hooks if h.status == "Passed")
    failed_hooks = [h for h in hooks if h.status == "Failed"]
    skipped = sum(1 for h in hooks if h.status == "Skipped")
    total = len(hooks)

    has_failures = bool(failed_hooks)
    icon = ":x:" if has_failures else ":white_check_mark:"
    status = "FAILED" if has_failures else "PASSED"

    lines = [
        f"### {icon} Precommit: {status}",
        "",
        "| Total | Passed | Failed | Skipped |",
        "|-------|--------|--------|---------|",
        f"| {total} | {passed} | {len(failed_hooks)} | {skipped} |",
        "",
    ]

    if failed_hooks:
        lines.append("#### Failed Hooks")
        lines.append("")
        for hook in failed_hooks:
            lines.append(f"**`{hook.hook}`**")
            if hook.details:
                details_truncated = hook.details[:MAX_TRACEBACK_CHARS]
                lines.append("<details><summary>Output</summary>")
                lines.append("")
                lines.append(f"```\n{details_truncated}\n```")
                lines.append("</details>")
            lines.append("")

    if build_url:
        lines.append(f"[Full build log]({build_url})")

    return "\n".join(lines)


def format_markdown(results: SuiteResults, stage_name: str, build_url: str = "") -> str:
    """Format test results as LLM-friendly Markdown.

    The output is designed for two audiences:
    - Humans reviewing a PR comment on GitHub
    - LLM agents parsing the comment to identify and fix failures

    Args:
        results: Parsed test results.
        stage_name: Display name for the test stage (e.g. "Unit Tests").
        build_url: URL to the full Jenkins build log.

    Returns:
        Markdown string suitable for a GitHub PR comment.
    """
    total = results.passed + results.failed + results.errors + results.skipped
    has_failures = bool(results.failures)
    icon = ":x:" if has_failures else ":white_check_mark:"
    status = "FAILED" if has_failures else "PASSED"

    lines = [
        f"### {icon} {stage_name}: {status}",
        "",
        "| Total | Passed | Failed | Errors | Skipped |",
        "|-------|--------|--------|--------|---------|",
        f"| {total} | {results.passed} | {results.failed} | {results.errors} | {results.skipped} |",
        "",
    ]

    if results.failures:
        lines.append("### Failed Tests")
        lines.append("")
        for failure in results.failures:
            lines.append(f"#### `{failure.test}`")
            if failure.file:
                lines.append(f"**File:** `{failure.file}`")
            lines.append(f"**Type:** {failure.failure_type}")
            if failure.message:
                lines.append(f"**Message:** `{failure.message}`")
            if failure.details:
                lines.append("<details><summary>Traceback</summary>")
                lines.append("")
                lines.append(f"```\n{failure.details}\n```")
                lines.append("</details>")
            lines.append("")

    if build_url:
        lines.append(f"[Full build log]({build_url})")

    output = "\n".join(lines)
    if len(output) > MAX_COMMENT_CHARS:
        # Truncate failures from the end to fit within the limit
        truncation_notice = "\n\n> **Note:** Some failures were omitted to fit within the comment size limit.\n"
        output = output[: MAX_COMMENT_CHARS - len(truncation_notice)] + truncation_notice
    return output


def compose_comment(
    *,
    test_results: SuiteResults | None = None,
    precommit_hooks: list[PrecommitHookResult] | None = None,
    stage_name: str,
    build_url: str = "",
) -> str:
    """Build a full PR comment from multiple result sources.

    Combines the marker, top-level heading, and one or more result sections
    (precommit, test results) into a single comment body.

    Args:
        test_results: Merged JUnit test results (optional).
        precommit_hooks: Parsed pre-commit hook results (optional).
        stage_name: Top-level heading for the comment.
        build_url: URL to the full Jenkins build log.

    Returns:
        Complete Markdown comment string.
    """
    sections: list[str] = [COMMENT_MARKER]

    # Determine overall status from all sources
    any_failure = False
    if test_results and test_results.failures:
        any_failure = True
    if precommit_hooks and any(h.status == "Failed" for h in precommit_hooks):
        any_failure = True

    icon = ":x:" if any_failure else ":white_check_mark:"
    status = "FAILED" if any_failure else "PASSED"
    sections.append(f"## {icon} {stage_name}: {status}")
    sections.append("")

    if precommit_hooks:
        sections.append(format_precommit_markdown(precommit_hooks))
        sections.append("")

    if test_results:
        total = test_results.passed + test_results.failed + test_results.errors + test_results.skipped
        if total > 0:
            sections.append(format_markdown(test_results, "Tests", build_url))

    output = "\n".join(sections)
    if len(output) > MAX_COMMENT_CHARS:
        truncation_notice = "\n\n> **Note:** Some content was omitted to fit within the comment size limit.\n"
        output = output[: MAX_COMMENT_CHARS - len(truncation_notice)] + truncation_notice
    return output


def main():
    parser = argparse.ArgumentParser(
        description="Parse JUnit XML and pre-commit output into a Markdown PR comment.",
    )
    parser.add_argument(
        "--junit-xml",
        nargs="+",
        default=[],
        help="Path(s) to JUnit XML file(s)",
    )
    parser.add_argument(
        "--precommit-log",
        default="",
        help="Path to pre-commit output log file",
    )
    parser.add_argument(
        "--stage-name",
        required=True,
        help="Display name for the test stage (e.g. 'Test Summary')",
    )
    parser.add_argument(
        "--build-url",
        default="",
        help="URL to the full Jenkins build log",
    )
    args = parser.parse_args()

    test_results = None
    all_results = []
    for xml_path_str in args.junit_xml:
        xml_path = Path(xml_path_str)
        if not xml_path.exists():
            logger.warning("JUnit XML file not found: %s", xml_path)
            continue
        all_results.append(parse_junit_xml(xml_path))
    if all_results:
        test_results = merge_results(all_results)

    precommit_hooks = None
    if args.precommit_log:
        precommit_path = Path(args.precommit_log)
        if precommit_path.exists():
            precommit_hooks = parse_precommit_log(precommit_path.read_text())
        else:
            logger.warning("Pre-commit log file not found: %s", precommit_path)

    if not test_results and not precommit_hooks:
        logger.error("No results to report (no JUnit XML or pre-commit log found)")
        sys.exit(1)

    print(
        compose_comment(
            test_results=test_results,
            precommit_hooks=precommit_hooks,
            stage_name=args.stage_name,
            build_url=args.build_url,
        )
    )


if __name__ == "__main__":
    main()
