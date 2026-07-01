#!/usr/bin/env bash
# Validate that every keystore entry SCT needs is reachable from AWS Secrets
# Manager before enabling the ``secretsmanager`` backend in Jenkins / a runner.
# Optionally migrate the missing entries by copying them from the legacy
# ``scylla-qa-keystore`` S3 bucket.
#
# Usage:
#   ./scripts/validate-keystore-sm.sh [--python] [--sct] [--migrate-missing]
#
# Options:
#   --python            Also run a live KeyStore().get_file_contents()
#                       round-trip for every entry via sdcm.keystore.
#                       Requires the repo virtualenv to be activated.
#   --sct               After the AWS-level checks, invoke the smallest sct.py
#                       command that triggers sct.py::cli() -> key_store.sync():
#                       ``list-images``. Reproduces the Jenkins failure
#                       locally end-to-end. Equivalent manually:
#                           SCT_KEYSTORE_BACKEND=secretsmanager ./sct.py list-images
#   --migrate-missing   For each missing entry, copy the object from the S3
#                       bucket ``scylla-qa-keystore`` into Secrets Manager under
#                       the configured prefix. Uses ``--secret-binary`` so
#                       byte-for-byte content is preserved for both JSON and
#                       binary entries. Tags every created secret with
#                       ``team=sct`` plus a ``secret_type`` derived from the
#                       filename extension.
#
# Env vars:
#   AWS_PROFILE / AWS access keys  — must point at the account that owns the
#                                    ``sct/`` Secrets Manager namespace.
#   SCT_KEYSTORE_SM_PREFIX         — override the ``sct/`` prefix (rarely used).
#
# Exit status:
#   0 — every entry resolves
#   1 — at least one entry is missing or AWS call failed
set -euo pipefail

PREFIX="${SCT_KEYSTORE_SM_PREFIX:-sct/}"
REGION="us-east-1"         # matches KEYSTORE_REGION in sdcm/keystore.py
S3_BUCKET="scylla-qa-keystore"  # matches KEYSTORE_S3_BUCKET in sdcm/keystore.py

# Keys sct.py syncs at startup (see sct.py::cli() — key_store.sync(...)).
SYNC_KEYS=(
    scylla_test_id_ed25519
)

# Every keystore entry SCT references at runtime. Keep in lockstep with
# docs/keystore-secrets-manager.md so this file doubles as the canonical
# manifest for humans.
#
# Derived from: ``grep -rn 'get_json("\|get_file_contents("\|download_file(' sdcm/ sct.py``.
# Dynamic names (e.g. baremetal configs, per-region credentials) are not
# enumerable statically; add them here when you learn they exist.
MANIFEST_KEYS=(
    # SSH key (binary)
    scylla_test_id_ed25519
    scylla_test_id_ed25519.pub
    # Cloud provider service accounts / credentials
    gcp-sct-project-1.json
    gcp-sct-project-1_service_accounts.json
    gcp-scylladbaaslab.json
    azure.json
    oci.json
    aws_images_role.json
    # Infrastructure / runner automation
    jenkins.json
    github_access.json
    docker.json
    # Reporting / notifications
    email_config.json
    ldap_ms_ad.json
    argus_rest_credentials.json
    # per-cloud Argus overrides (argus_rest_credentials_sct_{aws,gce,azure}.json)
    # are intentionally optional — only read under Jenkins and wrapped in
    # try/except in get_argus_rest_credentials_per_provider(). Not enforced here.
    scylladb_jira.json
    # Storage / backup
    housekeeping-db.json
    backup_azure_blob.json
    azure_kms_config.json
    gcp_kms_config.json
    scylladb_upload.json
    scylla_doctor_full.json
    # User / ACL rosters
    qa_users.json
    bucket-users.json
    # Scylla Cloud API (per environment)
    scylla_cloud_sct_api_creds_lab.json
    # Encryption-at-rest certificates (binary)
    CA.pem
    SCYLLADB.pem
    hytrust-kmip-cacert.pem
    hytrust-kmip-scylla.pem
)

run_python_roundtrip=0
run_sct_smoke=0
run_migrate=0
for arg in "$@"; do
    case "$arg" in
        --python)           run_python_roundtrip=1 ;;
        --sct)              run_sct_smoke=1 ;;
        --migrate-missing)  run_migrate=1 ;;
        -h|--help) sed -n '2,36p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $arg" >&2; exit 2 ;;
    esac
done

# Deduplicate (scylla_test_id_ed25519 appears in both lists).
declare -A seen=()
ALL_KEYS=()
for k in "${SYNC_KEYS[@]}" "${MANIFEST_KEYS[@]}"; do
    [[ -z "${seen[$k]:-}" ]] || continue
    seen[$k]=1
    ALL_KEYS+=("$k")
done

echo "Keystore SM validation"
echo "  prefix : $PREFIX"
echo "  region : $REGION"
echo "  keys   : ${#ALL_KEYS[@]}"
echo

missing=()
for name in "${ALL_KEYS[@]}"; do
    secret_id="${PREFIX}${name}"
    if version_info=$(aws secretsmanager describe-secret \
            --region "$REGION" \
            --secret-id "$secret_id" \
            --query 'VersionIdsToStages' \
            --output json 2>/dev/null); then
        current=$(echo "$version_info" | python3 -c '
import json, sys
vs = json.load(sys.stdin) or {}
for vid, stages in vs.items():
    if "AWSCURRENT" in stages:
        print(vid); break
else:
    print("<no AWSCURRENT>")
')
        printf "OK   %-42s (VersionId=%s)\n" "$secret_id" "$current"
    else
        printf "MISS %-42s (not found or access denied)\n" "$secret_id"
        missing+=("$name")
    fi
done

echo
if [[ ${#missing[@]} -eq 0 ]]; then
    echo "All ${#ALL_KEYS[@]} entries resolved."
else
    echo "${#missing[@]} of ${#ALL_KEYS[@]} entries are missing:"
    printf '  - %s\n' "${missing[@]}"
fi

# ---------------------------------------------------------------------------
# Migration: copy missing entries from S3 to Secrets Manager.
# ---------------------------------------------------------------------------
secret_type_for() {
    # Cheap classifier used only for SM tags.
    case "$1" in
        *.pub|scylla_test_id_ed25519) echo "ssh_key" ;;
        *.pem) echo "certificate" ;;
        *.json) echo "config" ;;
        *) echo "other" ;;
    esac
}

if [[ $run_migrate -eq 1 ]] && [[ ${#missing[@]} -gt 0 ]]; then
    echo
    echo "Migrating ${#missing[@]} missing entries from s3://$S3_BUCKET/ to Secrets Manager…"
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT
    migrate_failed=()
    for name in "${missing[@]}"; do
        local_path="$tmp_dir/$(basename "$name")"
        secret_id="${PREFIX}${name}"
        if ! aws s3 cp --quiet "s3://$S3_BUCKET/$name" "$local_path" 2>/dev/null; then
            printf "SKIP %-42s (not in s3://%s)\n" "$secret_id" "$S3_BUCKET"
            migrate_failed+=("$name")
            continue
        fi
        if aws secretsmanager create-secret \
                --region "$REGION" \
                --name "$secret_id" \
                --secret-binary "fileb://$local_path" \
                --description "SCT keystore entry migrated from s3://$S3_BUCKET/$name" \
                --tags "Key=team,Value=sct" "Key=secret_type,Value=$(secret_type_for "$name")" \
                --output text --query 'ARN' >/dev/null; then
            printf "MIG  %-42s (uploaded)\n" "$secret_id"
        else
            printf "FAIL %-42s (create-secret failed)\n" "$secret_id"
            migrate_failed+=("$name")
        fi
        shred -u "$local_path" 2>/dev/null || rm -f "$local_path"
    done
    echo
    if [[ ${#migrate_failed[@]} -eq 0 ]]; then
        echo "Migrated all ${#missing[@]} missing entries. Re-run without --migrate-missing to verify."
        missing=()
    else
        echo "${#migrate_failed[@]} entries could not be migrated:"
        printf '  - %s\n' "${migrate_failed[@]}"
        missing=("${migrate_failed[@]}")
    fi
fi

if [[ $run_python_roundtrip -eq 1 ]]; then
    echo
    echo "Running live KeyStore round-trip via sdcm.keystore.KeyStore()…"
    SCT_KEYSTORE_BACKEND=secretsmanager SCT_KEYSTORE_SM_PREFIX="$PREFIX" \
        python3 - "${ALL_KEYS[@]}" <<'PY'
import sys
from sdcm.keystore import KeyStore

ks = KeyStore()
failed = []
for name in sys.argv[1:]:
    try:
        data = ks.get_file_contents(name, bypass_cache=True)
        print(f"OK   {name:<42} ({len(data)} bytes)")
    except Exception as e:  # noqa: BLE001 — report-and-continue
        print(f"FAIL {name:<42} {type(e).__name__}: {e}")
        failed.append(name)

if failed:
    print(f"\n{len(failed)} entries could not be fetched.")
    sys.exit(1)
PY
fi

if [[ $run_sct_smoke -eq 1 ]]; then
    echo
    echo "Running ./sct.py list-images with SCT_KEYSTORE_BACKEND=secretsmanager…"
    SCT_KEYSTORE_BACKEND=secretsmanager ./sct.py list-images
fi

[[ ${#missing[@]} -eq 0 ]]
