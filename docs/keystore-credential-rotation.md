# Keystore Credential Rotation

How to regenerate an SCT cloud credential and propagate it to both keystore
backends, who is allowed to do it, and how to grant that permission to
somebody else.

Background: in SCT-1042 the Azure service principal client secret expired.
Every Azure SCT run — provisioning *and* the `QA-tools/cleanup-cloud` job —
failed at its first Azure API call with `AADSTS7000222`, so orphaned Azure
resources stayed alive and kept costing money. The weekly
[`keystore-expiry-check`](../.github/workflows/keystore-expiry-check.yaml)
workflow exists to make sure that does not repeat.

See [`keystore-secrets-manager.md`](keystore-secrets-manager.md) for the
keystore's backend selection and the full list of managed credentials.

## The shape of the problem

Every credential lives in **two** places and both must be updated:

| Backend | Location |
|---|---|
| AWS Secrets Manager | `sct/<name>` in account `797456418907`, region `us-east-1` |
| S3 (legacy fallback) | `s3://scylla-qa-keystore/<name>` |

`SCT_KEYSTORE_BACKEND` selects which one a given run reads (`s3` by default),
so updating only one leaves half the fleet broken.

`KeyStore` caches in memory for the lifetime of a single process, so no cache
invalidation is needed — the next test run and the next cleanup container both
fetch fresh.

---

## Rotating the Azure service principal secret

The Azure credential is `azure.json`, holding exactly four fields:

```json
{
  "subscription_id": "6c268694-47ab-43ab-b306-3c5514bc4112",
  "tenant_id": "99f7372b-6b77-426a-9e55-8bf7c203556e",
  "client_id": "b4003937-b8c1-4bc5-91cc-66da6c7c7fff",
  "client_secret": "<rotated>"
}
```

The `client_id` is the app registration **`azure_qa_sct_sp`**. Only
`client_secret` changes during a rotation; the other three are stable.

### 1. Mint a new secret

> `az ad app credential reset` **deletes all existing passwords** unless you
> pass `--append`. Use `--append` when you need an overlap window; omit it to
> clean up an already-expired secret.

```bash
APP_ID=b4003937-b8c1-4bc5-91cc-66da6c7c7fff
TENANT_ID=99f7372b-6b77-426a-9e55-8bf7c203556e
SUB_ID=6c268694-47ab-43ab-b306-3c5514bc4112

umask 077
WORK=$(mktemp -d -p /dev/shm)   # RAM-backed, never hits disk

SECRET=$(az ad app credential reset \
    --id "$APP_ID" \
    --display-name "sct-rotation-$(date -u +%Y-%m)" \
    --years 1 \
    --query password -o tsv)
```

The tenant's default app management policy sets no `passwordCredentials`
restriction, so `--years 2` (the `az` maximum) is also accepted. One year keeps
the blast radius smaller.

Record the expiry — step 4 needs it:

```bash
EXPIRES_ON=$(az ad app credential list --id "$APP_ID" \
    --query "max_by([], &endDateTime).endDateTime" -o tsv | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2}')
echo "$EXPIRES_ON"
```

### 2. Build the new `azure.json`

```bash
jq -n \
  --arg sub "$SUB_ID" --arg ten "$TENANT_ID" \
  --arg cid "$APP_ID" --arg sec "$SECRET" \
  '{subscription_id:$sub, tenant_id:$ten, client_id:$cid, client_secret:$sec}' \
  > "$WORK/azure.json"
unset SECRET

jq 'keys' "$WORK/azure.json"   # sanity: exactly the four fields
```

### 3. Push to both backends

```bash
aws secretsmanager put-secret-value \
    --secret-id sct/azure.json \
    --secret-string "file://$WORK/azure.json"

aws s3 cp "$WORK/azure.json" s3://scylla-qa-keystore/azure.json
```

### 4. Record the expiry date

This is what the weekly workflow reads. **Skipping it means the next lapse is
silent again.**

```bash
aws secretsmanager tag-resource \
    --secret-id sct/azure.json \
    --tags Key=expires_on,Value="$EXPIRES_ON"
```

### 5. Verify through SCT's own code path

This exercises `KeyStore.get_azure_credentials()` → `AzureService.credential`
→ a live token request, exactly as `utils/cloud_cleanup/azure/clean_azure.py`
does:

```bash
for backend in s3 secretsmanager; do
  echo "== $backend =="
  SCT_KEYSTORE_BACKEND=$backend python -c "
from sdcm.utils.azure_utils import AzureService
print('resource groups:', len(list(AzureService().resource.resource_groups.list())))
"
done
```

Both must print a count. An `AADSTS7000222` from one of them means that
backend did not get the update.

### 6. Clean up and confirm end to end

```bash
shred -u "$WORK/azure.json"; rmdir "$WORK"
```

Then run the cleanup pipeline and confirm the **Clean Azure** stage passes. A
`dryRun=true` run is enough to prove the credential works, because the first
Azure call (`resource_groups.list()`) happens before any delete branching:

```bash
python staging_trigger.py -f scylla-staging/<user>/qa generate \
    -b master --repo git@github.com:scylladb/scylla-cluster-tests.git \
    qa/hydra-cleanup-cloud

python staging_trigger.py -f scylla-staging/<user>/qa trigger \
    -b master --repo git@github.com:scylladb/scylla-cluster-tests.git \
    hydra-cleanup-cloud-test \
    -s dryRun=true -s email_recipients=<user>@scylladb.com --no-update-pr
```

Both subcommands need `-b` **and** `--repo` to stay non-interactive, and the
folder passed to `trigger` must include the `qa` subpath that `generate`
created the job under.

---

## Who can do this, and how to grant it

Rotation needs two independent permissions. They are granted in different
systems, so a person can easily have one and not the other.

### Azure side — resetting the client secret

Least privilege is **ownership of the app registration**. An owner can manage
that app's credentials and nothing else in the directory.

```bash
# find the user's object id
NEW_OWNER_OID=$(az ad user show --id someone@scylladb.com --query id -o tsv)

# grant
az ad app owner add \
    --id b4003937-b8c1-4bc5-91cc-66da6c7c7fff \
    --owner-object-id "$NEW_OWNER_OID"

# verify
az ad app owner list --id b4003937-b8c1-4bc5-91cc-66da6c7c7fff \
    --query '[].userPrincipalName' -o table
```

Adding an owner itself requires you to already be an owner of the app, or to
hold the `Application Administrator` / `Cloud Application Administrator`
directory role.

The tenant-wide alternative — assigning someone `Application Administrator` —
also works but lets them manage *every* app registration in the tenant. Prefer
ownership.

Keep more than one owner. A single owner is how SCT-1042 became a surprise:
nobody else could see the expiry coming, and nobody else could fix it.

### AWS side — writing to the keystore

Writing needs `secretsmanager:PutSecretValue` + `secretsmanager:TagResource` on
`sct/*` and `s3:PutObject` on the keystore bucket.

Today this comes from membership of the **`CloudiusDev`** IAM group, whose
inline `SecretsManagerReadWrite` policy grants `secretsmanager:*` and whose
attached `AmazonS3FullAccess` covers the bucket:

```bash
aws iam add-user-to-group --group-name CloudiusDev --user-name <iam-user>
aws iam get-group --group-name CloudiusDev --query 'Users[].UserName' --output table
```

`CloudiusDev` is far broader than rotation needs (full EC2, S3, ECR, SSM). For
someone who should *only* rotate keystore entries, attach a dedicated policy
instead:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "KeystoreRotateSecretsManager",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:PutSecretValue",
                "secretsmanager:TagResource"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:sct/*"
        },
        {
            "Sid": "KeystoreListSecrets",
            "Effect": "Allow",
            "Action": ["secretsmanager:ListSecrets"],
            "Resource": "*"
        },
        {
            "Sid": "KeystoreRotateS3",
            "Effect": "Allow",
            "Action": ["s3:PutObject", "s3:GetObject"],
            "Resource": "arn:aws:s3:::scylla-qa-keystore/*"
        }
    ]
}
```

`secretsmanager:ListSecrets` accepts no resource restriction, so it needs its
own `"Resource": "*"` statement — putting it under the `sct/*` statement looks
tighter but simply does not work. It is optional: rotation only ever touches
secrets by name, and the expiry check falls back to describing known entries
when listing is denied, which is what the existing identities hit.

---

## What the weekly check does

[`keystore-expiry-check.yaml`](../.github/workflows/keystore-expiry-check.yaml)
runs Mondays at 06:00 UTC and on demand. It reads the `expires_on` tag from
every `sct/*` secret and:

- **expired** — fails the job and opens/updates a tracking issue;
- **expiring within 30 days** (`workflow_dispatch` input `warn_days` to change)
  — opens/updates the tracking issue;
- **no `expires_on` tag** — reported as untracked, so credentials do not sit
  unwatched;
- **all healthy** — closes the tracking issue.

Its source of truth is the tag, which a human sets in step 4. That is a
deliberate trade-off: it needs no Azure permissions and it covers *every* SCT
credential, not only Azure. The tag can drift from reality if someone rotates
without updating it — the untracked-secret report is the backstop.

### The Azure cross-check

For `azure.json` the check does not stop at the tag — it asks Azure AD what
the expiry really is and reports any disagreement, which is what catches a
rotation that updated the secret but forgot step 4.

This needs **no extra Azure permission and no admin consent**. Azure grants
every application the right to read its own application object, so the service
principal can list its own `passwordCredentials` with no Graph app role
assigned. The check reads `azure.json` from the keystore it already has access
to, exchanges it for a Graph token, and reads back:

```
GET https://graph.microsoft.com/v1.0/applications(appId='<client_id>')?$select=passwordCredentials
```

A secret that is expired or revoked fails the token request outright, which is
itself the answer — that row is reported as expired. A Graph outage is
reported but never overrides the tag-based answer, so it cannot turn the check
into a false alarm. `workflow_dispatch` has a `skip_azure` input for when the
live lookup is in the way.

Extending the same treatment to GCP and OCI would need their own API calls and
their own permissions; only Azure is covered today.

## Rotation cadence

`docs/plans/keystore-improvements.md` classifies cloud credentials —
`azure.json` among them — as **Tier 1, 90 days**. The `rotation_tier` tag on
`sct/azure.json` currently reads `tier2`; that is a mismatch worth correcting
when the tiering is next reviewed.
