#!/usr/bin/env python3
"""Report SCT keystore credentials that are expired or close to expiring.

Two sources, because neither covers everything:

* the ``expires_on`` tag on every secret under the ``sct/`` prefix in AWS
  Secrets Manager.  Broad - it covers every credential - but it is written by
  hand during rotation (``docs/keystore-credential-rotation.md``), so it can
  drift.  A secret without the tag is reported so it does not sit unwatched.
* Azure AD itself, for ``azure.json``.  Authoritative, and it needs no extra
  permission: Azure grants an application the right to read its own
  application object, so the service principal can list its own
  ``passwordCredentials`` with no Graph app role assigned.

When both are available the Azure answer wins and a disagreement with the tag
is called out, which is what catches a rotation that forgot to update the tag.

Exit codes: 0 when nothing is expired, 1 when at least one secret is.
"""

import os
import sys
import json
import uuid
import urllib.error
import urllib.parse
import urllib.request
import argparse
import datetime

import boto3
from botocore.exceptions import ClientError

WARN_DAYS_DEFAULT = 30
EXPIRY_TAG = "expires_on"

# The keystore entry holding the Azure service principal, and the Graph
# endpoints used to ask Azure AD when its client secret really expires.
AZURE_SECRET_NAME = "azure.json"
AZURE_TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
AZURE_GRAPH_SCOPE = "https://graph.microsoft.com/.default"
AZURE_GRAPH_APP_URL = (
    "https://graph.microsoft.com/v1.0/applications(appId='{client_id}')?$select=displayName,passwordCredentials"
)
HTTP_TIMEOUT = 30

# ``secretsmanager:ListSecrets`` does not support resource-level permissions, so
# the policies that grant ``secretsmanager:*`` on ``secret:sct/*`` do not cover
# it.  When listing is denied we fall back to describing each known entry by
# name.  Keep in sync with the managed credentials table in
# docs/keystore-secrets-manager.md and the accessors in sdcm/keystore.py.
#
# These are the entries of the ``sct/`` keystore specifically.  The fallback
# joins them to whatever ``--prefix`` is in force, so pointing the script at a
# different prefix while listing is denied finds nothing at all.
KNOWN_SECRETS = (
    "scylla_test_id_ed25519",
    "scylla_test_id_ed25519.pub",
    "gcp-sct-project-1.json",
    "gcp-sct-project-1_service_accounts.json",
    "gcp-local-ssd-latency.json",
    "gcp-local-ssd-latency_service_accounts.json",
    "gcp-scylladbaaslab.json",
    "azure.json",
    "oci.json",
    "docker.json",
    "email_config.json",
    "ldap_ms_ad.json",
    "argus_rest_credentials.json",
    "scylladb_jira.json",
    "CA.pem",
    "SCYLLADB.pem",
    "hytrust-kmip-cacert.pem",
    "hytrust-kmip-scylla.pem",
    "backup_azure_blob.json",
    "azure_kms_config.json",
    "gcp_kms_config.json",
    "scylladb_upload.json",
    "qa_users.json",
    "bucket-users.json",
    "aws_images_role.json",
    "github_access.json",
    "jenkins.json",
    "scylla_doctor_full.json",
)

# Entries whose names are templated per provider or per environment
# (``argus_rest_credentials_sct_{provider}.json``,
# ``scylla_cloud_sct_api_creds_{env}.json``) cannot be described by name and are
# only seen when listing is permitted.

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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=str, default="sct/", help="Secrets Manager name prefix to scan")
    parser.add_argument(
        "--warn-days", type=int, default=WARN_DAYS_DEFAULT, help="Days before expiry at which to start warning"
    )
    parser.add_argument("--region", type=str, default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--no-azure",
        action="store_true",
        help="Skip asking Azure AD for the real service principal expiry, and trust the tag instead",
    )
    return parser.parse_args(argv)


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


# AADSTS codes that mean "this credential is no longer good", as opposed to
# Graph or the network being unavailable.
CREDENTIAL_REJECTION_MARKERS = ("AADSTS7000222", "AADSTS7000215", "invalid_client", "unauthorized_client")


def _describe_http_error(exc):
    """Summarise an HTTPError, including the response body when it has one."""
    try:
        body = exc.read().decode("utf-8", "replace")[:400]
    except Exception:  # noqa: BLE001 - the body is best-effort context only
        body = ""
    return f"HTTP {exc.code} {body}".strip()


def _is_credential_rejection(exc, detail):
    """True when the failure means the stored secret itself was refused."""
    if exc.code not in (400, 401):
        return False
    return any(marker in detail for marker in CREDENTIAL_REJECTION_MARKERS)


def _post_form(url, fields):
    """POST a urlencoded form and return the parsed JSON response."""
    data = urllib.parse.urlencode(fields).encode()
    request = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return json.load(response)


def _get_json(url, token):
    """GET a bearer-authenticated JSON endpoint."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
        return json.load(response)


def azure_secret_expiry(client, secret_name):
    """Ask Azure AD when the SCT service principal's client secret expires.

    Reads ``azure.json`` out of the keystore, exchanges it for a Graph token
    and reads the application's own ``passwordCredentials``.  This needs no
    Graph app role: an application may always read its own application object.

    Returns ``(expiry_date, warning)``.  ``expiry_date`` is ``None`` when the
    app carries no client secret at all.  ``warning`` is a string when the
    stored secret could not be matched to exactly one credential on the app,
    and ``None`` otherwise.
    """
    payload = client.get_secret_value(SecretId=secret_name)
    creds = json.loads(payload.get("SecretString") or payload["SecretBinary"])

    token_response = _post_form(
        AZURE_TOKEN_URL.format(tenant_id=creds["tenant_id"]),
        {
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "scope": AZURE_GRAPH_SCOPE,
            "grant_type": "client_credentials",
        },
    )
    app = _get_json(AZURE_GRAPH_APP_URL.format(client_id=creds["client_id"]), token_response["access_token"])
    credentials = app.get("passwordCredentials", [])

    # During an overlap window the app carries several secrets, and the keystore
    # holds exactly one of them - not necessarily the newest. Picking the latest
    # expiry would report a healthy date while the secret SCT actually uses is
    # about to lapse. Graph returns a `hint` holding the first three characters
    # of each secret, which identifies the one the keystore has.
    hint = creds["client_secret"][:3]
    matched = [entry for entry in credentials if entry.get("hint") == hint]
    if len(matched) == 1:
        return parse_expiry(matched[0]["endDateTime"]), None
    if len(matched) > 1:
        # Same three-character prefix on two secrets: report the nearer expiry
        # rather than guess optimistically.
        expiries = sorted(filter(None, (parse_expiry(entry["endDateTime"]) for entry in matched)))
        return (expiries[0] if expiries else None), f"{len(matched)} app secrets share the hint {hint!r}"

    expiries = sorted(filter(None, (parse_expiry(entry["endDateTime"]) for entry in credentials)))
    if not expiries:
        return None, None
    # The stored secret is not among the app's current credentials at all, which
    # means it was replaced without the keystore being updated.
    return expiries[0], "stored secret does not match any credential on the app"


def apply_azure_truth(results, client, prefix, today, warn_days):
    """Replace the tag-derived Azure row with what Azure AD actually reports.

    Mutates and returns ``results``.  Only an outright rejection of the stored
    credential marks the row expired; anything else - DNS, a read timeout, an
    IAM denial on the keystore read - is reported as an unverified lookup and
    leaves the tag-based answer standing, so infrastructure trouble cannot be
    mistaken for a lapsed secret.
    """
    name = f"{prefix}{AZURE_SECRET_NAME}"
    row = next((item for item in results if item["name"] == name), None)
    if row is None:
        return results

    try:
        expires_on, warning = azure_secret_expiry(client, name)
    except urllib.error.HTTPError as exc:
        # The token endpoint answers 400/401 with an AADSTS code when the
        # secret itself is bad; that is the answer we are looking for. Any
        # other HTTP status is Graph being unwell, not a dead credential.
        detail = _describe_http_error(exc)
        if _is_credential_rejection(exc, detail):
            row["status"] = STATUS_EXPIRED
            row["note"] = f"Azure AD rejected the stored secret ({detail})"
        else:
            row["note"] = f"Azure lookup failed, showing tag: {detail}"
        print(f"azure lookup for {name}: {detail}", file=sys.stderr)
        return results
    except (
        urllib.error.URLError,
        TimeoutError,
        OSError,
        ClientError,
        KeyError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        # Network, TLS, socket timeout, IAM denial on the keystore read, or a
        # response that did not parse. None of these say anything about the
        # credential, so the tag-based answer stands.
        detail = f"{type(exc).__name__}: {exc}"
        print(f"azure lookup for {name}: {detail}", file=sys.stderr)
        row["note"] = f"Azure lookup failed, showing tag: {detail}"
        return results

    if expires_on is None:
        row["status"] = STATUS_EXPIRED
        row["note"] = warning or "Azure AD reports no client secret on the app"
        return results

    tagged = row.get("expires_on")
    verified = classify(name, {EXPIRY_TAG: expires_on.isoformat()}, today, warn_days)
    verified["source"] = "azure"
    notes = []
    if warning:
        notes.append(warning)
    if tagged and tagged != verified["expires_on"]:
        notes.append(f"tag says {tagged}, Azure AD says {verified['expires_on']}")
    elif not tagged:
        notes.append("no expires_on tag; value read from Azure AD")
    if notes:
        verified["note"] = "; ".join(notes)

    results[results.index(row)] = verified
    return results


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
    # Status first, then soonest expiry.  The middle element keeps the rows with
    # no known expiry from being compared against integers: they only ever sort
    # against each other, since untracked is the one status that allows None.
    rows = sorted(results, key=lambda item: (order[item["status"]], item["days_left"] is None, item["days_left"]))

    lines = [
        "| | Secret | Expires on | Days left | Source | Note |",
        "|---|---|---|---|---|---|",
    ]
    for item in rows:
        days = "—" if item["days_left"] is None else str(item["days_left"])
        expires = item["expires_on"] or "—"
        source = item.get("source", "tag")
        note = item.get("note", "")
        lines.append(f"| {STATUS_EMOJI[item['status']]} | `{item['name']}` | {expires} | {days} | {source} | {note} |")

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
            # Random delimiter: a report line that happened to equal the marker
            # would otherwise truncate the output the runner reads back.
            delimiter = f"EOF_{uuid.uuid4().hex}"
            handle.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")
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
    args = parse_args()
    client = boto3.client("secretsmanager", region_name=args.region)
    today = datetime.datetime.now(datetime.UTC).date()

    results = [classify(name, tags, today, args.warn_days) for name, tags in collect_secrets(client, args.prefix)]
    if not results:
        print(f"No secrets found under prefix {args.prefix!r}", file=sys.stderr)
        return 1

    if not args.no_azure:
        results = apply_azure_truth(results, client, args.prefix, today, args.warn_days)

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
