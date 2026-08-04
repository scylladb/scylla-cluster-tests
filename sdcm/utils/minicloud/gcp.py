"""GCP credential and staging-bucket setup for the minicloud gce backend."""

import json
import logging
import os
from pathlib import Path

import google.auth.transport.requests
from google.api_core import exceptions as google_exceptions
from google.cloud import compute_v1, storage
from google.oauth2 import service_account

from sdcm.keystore import KeyStore
from sdcm.utils.minicloud.config import (
    MINICLOUD_GCE_NETWORK,
    MINICLOUD_GCE_REGION_INDEX_OFFSET,
    MINICLOUD_GCE_REGIONS,
    MINICLOUD_GCE_SUBNET_CIDR_TMPL,
    MINICLOUD_HOST_VPC_ROUTES,
    MinicloudConfig,
    MinicloudError,
)
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


def prepare_gce_network(config: MinicloudConfig) -> None:
    """Pre-create the emulated qa-vpc network with one explicit subnet per supported region.

    Must be called only after GCE_ENDPOINT_URL is set (i.e., after start()) - the compute
    clients pick the emulator endpoint up through _gce_client_options().

    Without this, minicloud emulates GCE auto-mode: the first instance insert auto-creates
    the network's subnet with a /20 from 10.128.0.0/9, which is unroutable from the host
    (outside MINICLOUD_HOST_VPC_ROUTES) and inside the real GCE VPC space an sct-runner
    lives in - guests come up and every SSH to them times out. Custom-mode subnets with
    explicit CIDRs keep every guest inside the routed 10.176.0.0/16 .. 10.179.0.0/16.

    Idempotent: an existing network or subnet is left alone, so re-running start-minicloud
    against a live emulator (keep_alive) is safe.
    """
    # cause import at module scope creates a cyclic dependency via gce_utils -> sct_config
    from sdcm.utils.gce_utils import _gce_client_options  # noqa: PLC0415

    creds = KeyStore().get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(creds)
    client_options = _gce_client_options()
    networks = compute_v1.NetworksClient(credentials=credentials, **client_options)
    subnets = compute_v1.SubnetworksClient(credentials=credentials, **client_options)
    # The project the *VM requests* will use, which is what this network has to be reachable
    # from: GceProvisioner takes it from the credentials (provision/gce/provisioner.py), while
    # config.gcp_project is the container's own --gcp-project for GCS image staging. Those two
    # differ whenever the gce_project job parameter is empty (credentials default to
    # gcp-sct-project-1, the config default is sct-project-1), which would prepare qa-vpc in a
    # project the launches never look at.
    project = creds.get("project_id") or config.gcp_project

    try:
        networks.get(project=project, network=MINICLOUD_GCE_NETWORK)
        LOGGER.info("Emulated GCE network '%s' already exists", MINICLOUD_GCE_NETWORK)
    except google_exceptions.NotFound:
        LOGGER.info("Creating emulated GCE network '%s' (custom mode)...", MINICLOUD_GCE_NETWORK)
        networks.insert(
            project=project,
            network_resource=compute_v1.Network(name=MINICLOUD_GCE_NETWORK, auto_create_subnetworks=False),
        )

    for index, region in enumerate(MINICLOUD_GCE_REGIONS):
        subnet_name = f"{MINICLOUD_GCE_NETWORK}-{region}"
        cidr = MINICLOUD_GCE_SUBNET_CIDR_TMPL.format(MINICLOUD_GCE_REGION_INDEX_OFFSET + index)
        try:
            existing = subnets.get(project=project, region=region, subnetwork=subnet_name)
            # Existence is not enough: a reused keep_alive emulator may carry a subnet of this
            # name from an auto-mode launch (a /20 out of 10.128.0.0/9) or from an older offset,
            # and accepting it would put the guests outside the host-routed range again - the
            # failure mode this whole function exists to prevent, but silent this time. Fail
            # rather than recreate: guests may already be attached to it.
            if existing.ip_cidr_range != cidr:
                raise MinicloudError(
                    f"emulated GCE subnet '{subnet_name}' exists with {existing.ip_cidr_range}, "
                    f"expected {cidr}. Guests would land outside the host-routed range "
                    f"({MINICLOUD_HOST_VPC_ROUTES[0]}). Stop the minicloud container to drop its "
                    f"state and let it be recreated: docker rm -f minicloud"
                )
            LOGGER.info("Emulated GCE subnet '%s' already exists with %s", subnet_name, cidr)
            continue
        except google_exceptions.NotFound:
            # Expected on a fresh emulator: fall through to the insert below.
            LOGGER.debug("Emulated GCE subnet '%s' does not exist yet", subnet_name)
        LOGGER.info("Creating emulated GCE subnet '%s' (%s) in %s...", subnet_name, cidr, region)
        subnets.insert(
            project=project,
            region=region,
            subnetwork_resource=compute_v1.Subnetwork(
                name=subnet_name,
                network=f"projects/{project}/global/networks/{MINICLOUD_GCE_NETWORK}",
                ip_cidr_range=cidr,
            ),
        )
