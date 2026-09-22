#!/usr/bin/env python3
"""Report SCT keystore credentials that are expired or close to expiring.

Reads the ``expires_on`` tag from every secret under the ``sct/`` prefix in
AWS Secrets Manager and classifies each one.  The tag is set by hand as part
of the rotation runbook (``docs/keystore-credential-rotation.md``); a secret
without the tag is reported so it does not sit unwatched.

Exit codes: 0 when nothing is expired, 1 when at least one secret is.
"""

import os
import sys
import json
import argparse
import datetime

import boto3
from botocore.exceptions import ClientError

WARN_DAYS_DEFAULT = 30
EXPIRY_TAG = "expires_on"

# ``secretsmanager:ListSecrets`` does not support resource-level permissions, so
# the policies that grant ``secretsmanager:*`` on ``secret:sct/*`` do not cover
# it.  When listing is denied we fall back to describing each known entry by
# name.  Keep in sync with the managed credentials table in
# docs/keystore-secrets-manager.md and the accessors in sdcm/keystore.py.
KNOWN_SECRETS = (
    "scylla_test_id_ed25519",
    "scylla_test_id_ed25519.pub",
    "gcp-sct-project-1.json",
    "gcp-scylladbaaslab.json",
    "azure.json",
    "oci.json",
    "docker.json",
    "email_config.json",
    "ldap_ms_ad.json",
    "argus_rest_credentials.json",
    "scylladb_jira.json",
    "housekeeping-db.json",
    "backup_azure_blob.json",
    "azure_kms_config.json",
    "gcp_kms_config.json",
    "scylladb_upload.json",
    "qa_users.json",
    "bucket-users.json",
)

STATUS_EXPIRED = "expired"
STATUS_EXPIRING = "expiring"
STATUS_UNTRACKED = "untracked"
STATUS_OK = "ok"

STATUS_EMOJI = {
    STATUS_EXPIRED: ":red_circle:",
    STATUS_EXPIRING: ":warning:",
    STATUS_UNTRACKED: ":grey_question:",
    STATUS_OK: ":white_check_mark:",
}


def get_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=str, default="sct/", help="Secrets Manager name prefix to scan")
    parser.add_argument(
        "--warn-days", type=int, default=WARN_DAYS_DEFAULT, help="Days before expiry at which to start warning"
    )
    parser.add_argument("--region", type=str, default=os.environ.get("AWS_REGION", "us-east-1"))
    return parser.parse_args()


def parse_expiry(value):
    """Parse an ``expires_on`` tag value into a date.

    Accepts a plain ``YYYY-MM-DD`` as well as a full ISO-8601 timestamp, which
    is what ``az ad app credential list`` prints for ``endDateTime``.
    """
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        pass
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _list_secrets(client, prefix):
    """Yield (name, tags) for every secret whose name starts with prefix."""
    paginator = client.get_paginator("list_secrets")
    for page in paginator.paginate(Filters=[{"Key": "name", "Values": [prefix]}]):
        for secret in page["SecretList"]:
            tags = {tag["Key"]: tag["Value"] for tag in secret.get("Tags", [])}
            yield secret["Name"], tags


def _describe_known_secrets(client, prefix):
    """Yield (name, tags) by describing each known entry individually."""
    for short_name in KNOWN_SECRETS:
        name = f"{prefix}{short_name}"
        try:
            secret = client.describe_secret(SecretId=name)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("ResourceNotFoundException", "AccessDeniedException"):
                print(f"skipping {name}: {code}", file=sys.stderr)
                continue
            raise
        tags = {tag["Key"]: tag["Value"] for tag in secret.get("Tags", [])}
        yield secret["Name"], tags


def collect_secrets(client, prefix):
    """Yield (name, tags) for the keystore secrets under prefix.

    Prefers a single ``ListSecrets`` call and falls back to describing the
    known entries by name when the caller is not allowed to list.
    """
    try:
        # Materialize before yielding: a denial surfaces on the first paginator
        # call, and building the list up front keeps a mid-iteration failure
        # from emitting a partial result twice.
        listed = list(_list_secrets(client, prefix))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "AccessDeniedException":
            raise
        print("ListSecrets denied, describing known secrets individually", file=sys.stderr)
        listed = list(_describe_known_secrets(client, prefix))
    yield from listed


def classify(name, tags, today, warn_days):
    """Return a result dict describing the expiry state of one secret."""
    raw = tags.get(EXPIRY_TAG)
    if raw is None:
        return {"name": name, "status": STATUS_UNTRACKED, "expires_on": None, "days_left": None}

    expires_on = parse_expiry(raw)
    if expires_on is None:
        return {"name": name, "status": STATUS_UNTRACKED, "expires_on": raw, "days_left": None}

    days_left = (expires_on - today).days
    if days_left < 0:
        status = STATUS_EXPIRED
    elif days_left <= warn_days:
        status = STATUS_EXPIRING
    else:
        status = STATUS_OK
    return {"name": name, "status": status, "expires_on": expires_on.isoformat(), "days_left": days_left}


def render_markdown(results, warn_days):
    """Render the results as a Markdown table, worst first."""
    order = {STATUS_EXPIRED: 0, STATUS_EXPIRING: 1, STATUS_UNTRACKED: 2, STATUS_OK: 3}
    rows = sorted(results, key=lambda item: (order[item["status"]], item["days_left"] is None, item["days_left"]))

    lines = [
        "| | Secret | Expires on | Days left |",
        "|---|---|---|---|",
    ]
    for item in rows:
        days = "—" if item["days_left"] is None else str(item["days_left"])
        expires = item["expires_on"] or "—"
        lines.append(f"| {STATUS_EMOJI[item['status']]} | `{item['name']}` | {expires} | {days} |")

    counts = {status: sum(1 for item in results if item["status"] == status) for status in order}
    lines.append("")
    lines.append(
        f"{counts[STATUS_EXPIRED]} expired, {counts[STATUS_EXPIRING]} expiring within {warn_days} days, "
        f"{counts[STATUS_UNTRACKED]} untracked, {counts[STATUS_OK]} healthy."
    )
    lines.append("")
    lines.append(
        "Rotation runbook: [`docs/keystore-credential-rotation.md`]"
        "(https://github.com/scylladb/scylla-cluster-tests/blob/master/docs/keystore-credential-rotation.md)"
    )
    return "\n".join(lines)


def write_github_output(name, value):
    """Append a value to the GitHub Actions output file, if running in CI."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if not output_file:
        return
    with open(output_file, "a", encoding="utf-8") as handle:
        if "\n" in value:
            handle.write(f"{name}<<__EOF__\n{value}\n__EOF__\n")
        else:
            handle.write(f"{name}={value}\n")


def write_step_summary(markdown):
    """Append the report to the GitHub Actions job summary, if running in CI."""
    summary_file = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_file:
        return
    with open(summary_file, "a", encoding="utf-8") as handle:
        handle.write("## SCT keystore credential expiry\n\n")
        handle.write(markdown)
        handle.write("\n")


def main():
    args = get_parser()
    client = boto3.client("secretsmanager", region_name=args.region)
    today = datetime.datetime.now(datetime.UTC).date()

    results = [classify(name, tags, today, args.warn_days) for name, tags in collect_secrets(client, args.prefix)]
    if not results:
        print(f"No secrets found under prefix {args.prefix!r}", file=sys.stderr)
        return 1

    markdown = render_markdown(results, args.warn_days)
    print(markdown)
    print(json.dumps(results, indent=2), file=sys.stderr)

    write_step_summary(markdown)
    needs_attention = [item for item in results if item["status"] in (STATUS_EXPIRED, STATUS_EXPIRING)]
    write_github_output("needs_attention", "true" if needs_attention else "false")
    write_github_output("report", markdown)

    expired = [item for item in results if item["status"] == STATUS_EXPIRED]
    return 1 if expired else 0


if __name__ == "__main__":
    sys.exit(main())
