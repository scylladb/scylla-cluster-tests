"""GCP credential and staging-bucket setup for the minicloud gce backend."""

import json
import logging
import os
from pathlib import Path

import google.auth.transport.requests
from google.cloud import storage
from google.oauth2 import service_account

from sdcm.keystore import KeyStore
from sdcm.utils.minicloud.config import MinicloudConfig, MinicloudError
from sdcm.utils.session import create_retry_session

LOGGER = logging.getLogger(__name__)


def setup_gcp_credentials(config: MinicloudConfig, backend: str) -> None:
    """Download GCP service account JSON from KeyStore and set GOOGLE_APPLICATION_CREDENTIALS.

    minicloud's Rust gcp_auth crate uses Application Default Credentials (ADC).
    Setting GOOGLE_APPLICATION_CREDENTIALS to a service account JSON file is the
    simplest way to provide credentials for GCE API passthrough.
    """
    if backend not in ("gce", "gce-siren"):
        return

    creds = None
    # Same precedence as the container mount in manager.py, and it has to stay the same: with
    # only GCS_KEY_FILE set, reading GOOGLE_APPLICATION_CREDENTIALS alone would fall through to
    # KeyStore here and create the staging bucket under that identity, while the container runs
    # under the GCS_KEY_FILE one.
    creds_path = os.environ.get("GCS_KEY_FILE") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

    if creds_path and Path(creds_path).is_file():
        LOGGER.info("Using the GCP credentials already on this host: %s", creds_path)
        with open(creds_path) as fh:
            creds = json.load(fh)
    else:
        try:
            creds = KeyStore().get_gcp_credentials()
        except Exception as exc:
            # Fatal for the gce backend: without ADC, minicloud's gcp_auth has no
            # authentication method and every GCE launch fails with an opaque 500
            # long after this point — surface the actionable cause now.
            raise MinicloudError(
                "failed to download GCP credentials from KeyStore — the gce backend "
                "requires them for image export/passthrough"
            ) from exc
        creds_path = os.path.join(config.state_dir, "gcp-credentials.json")
        os.makedirs(config.state_dir, exist_ok=True)
        # 0600: the file holds a service-account private key. A file is unavoidable —
        # the container's Rust gcp_auth ADC only reads GOOGLE_APPLICATION_CREDENTIALS
        # from a mounted path; there is no env-inline JSON form.
        fd = os.open(creds_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            # the mode argument of os.open only applies when the file is created;
            # fchmod also tightens a pre-existing, more permissive credentials file
            os.fchmod(fh.fileno(), 0o600)
            json.dump(creds, fh)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        LOGGER.info("Set GOOGLE_APPLICATION_CREDENTIALS=%s", creds_path)

    if not config.gcs_bucket and creds:
        config.gcs_bucket = ensure_gcs_bucket(creds, config.gcp_project)


def ensure_gcs_bucket(creds: dict, project_id: str) -> str:
    """Create the minicloud staging GCS bucket if it doesn't exist, return its name.

    The project comes from the config the container is started with — re-resolving it
    here would let the staging bucket and the Cloud Build check target a different
    project than the container uses.

    Deliberately talks to REAL Google Cloud Storage (no GCE_ENDPOINT_URL honoured):
    minicloud emulates the Compute API only, and stages exported images through a
    genuine GCS bucket.
    """
    bucket_name = f"{project_id}-minicloud-staging"
    credentials = service_account.Credentials.from_service_account_info(creds)
    client = storage.Client(credentials=credentials, project=project_id)

    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        LOGGER.info("Creating GCS bucket %s for minicloud image staging", bucket_name)
        bucket.storage_class = "STANDARD"
        client.create_bucket(bucket, location="us")
        bucket.lifecycle_rules = [{"action": {"type": "Delete"}, "condition": {"age": 7}}]
        bucket.patch()
        LOGGER.info("Created GCS bucket %s with 7-day lifecycle", bucket_name)
    else:
        LOGGER.info("GCS bucket %s already exists", bucket_name)

    ensure_cloudbuild_access(creds, project_id, bucket_name)
    return bucket_name


def ensure_cloudbuild_access(creds: dict, project_id: str, bucket_name: str) -> None:
    """Ensure Cloud Build API is enabled and service account has bucket access.

    minicloud exports GCP images via Cloud Build. This requires:
    1. cloudbuild.googleapis.com enabled on the project
    2. The Cloud Build service account has objectAdmin on the staging bucket
    3. The compute service account has cloudbuild.builds.builder role

    These are idempotent operations — safe to call on every run.
    """
    credentials = service_account.Credentials.from_service_account_info(creds)

    try:
        scoped_creds = credentials.with_scopes(["https://www.googleapis.com/auth/cloud-platform"])
        scoped_creds.refresh(google.auth.transport.requests.Request())

        url = f"https://serviceusage.googleapis.com/v1/projects/{project_id}/services/cloudbuild.googleapis.com:enable"
        # services.enable is idempotent (enabling an enabled API succeeds), so POST
        # retry on transient 5xx/429 is safe here.
        resp = create_retry_session().post(
            url,
            headers={"Authorization": f"Bearer {scoped_creds.token}"},
            timeout=30,
        )
        if resp.status_code in (200, 409):
            LOGGER.info("Cloud Build API enabled (or already enabled) for %s", project_id)
        else:
            LOGGER.warning(
                "Could not enable Cloud Build API (status %d): %s. "
                "Run manually: gcloud services enable cloudbuild.googleapis.com --project=%s",
                resp.status_code,
                resp.text[:200],
                project_id,
            )
    except Exception:  # noqa: BLE001
        LOGGER.warning(
            "Could not enable Cloud Build API programmatically. "
            "Run manually: gcloud services enable cloudbuild.googleapis.com --project=%s",
            project_id,
            exc_info=True,
        )

    try:
        client = storage.Client(credentials=credentials, project=project_id)
        bucket = client.bucket(bucket_name)
        policy = bucket.get_iam_policy(requested_policy_version=3)

        cloudbuild_sa = None
        for binding in policy.bindings:
            for member in binding.get("members", []):
                if "@cloudbuild.gserviceaccount.com" in member:
                    cloudbuild_sa = member
                    break

        if cloudbuild_sa:
            LOGGER.info("Cloud Build SA %s already has bucket access", cloudbuild_sa)
        else:
            LOGGER.warning(
                "Cloud Build service account not found in bucket IAM. "
                "If image export fails, run:\n"
                "  gcloud services enable cloudbuild.googleapis.com --project=%s\n"
                "  # Wait 60s, then:\n"
                "  gsutil iam ch serviceAccount:$(gcloud projects describe %s "
                "--format='value(projectNumber)')@cloudbuild.gserviceaccount.com:objectAdmin "
                "gs://%s",
                project_id,
                project_id,
                bucket_name,
            )
    except Exception:  # noqa: BLE001
        LOGGER.warning("Could not verify Cloud Build bucket access", exc_info=True)
