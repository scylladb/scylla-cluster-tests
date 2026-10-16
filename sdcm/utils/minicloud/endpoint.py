"""Minicloud endpoint resolution — the bottom of the package's dependency graph.

Only stdlib imports allowed here: ``MinicloudConfig._resolve_port`` (config.py) needs
``get_minicloud_endpoint``, and everything else in the package imports config, so this
module must not import anything from the package.
"""

import os
from urllib.parse import urlparse

MINICLOUD_PORT = 5000
# the Azure SDK refuses plain http, so minicloud serves its Azure API over TLS on a separate port
MINICLOUD_TLS_PORT = 5443
# fixed fake Azure identity that minicloud accepts
MINICLOUD_AZURE_SUBSCRIPTION = "00000000-0000-0000-0000-000000000000"
MINICLOUD_AZURE_TENANT = "00000000-0000-0000-0000-000000000000"
MINICLOUD_AZURE_CLIENT_ID = "minicloud"
MINICLOUD_AZURE_CLIENT_SECRET = "minicloud"
AZURE_BACKENDS = ("azure",)


class MinicloudActivationError(RuntimeError):
    """Raised when an Azure client would be built before minicloud is activated."""


def get_minicloud_azure_endpoint() -> str | None:
    """AZURE_ENDPOINT_URL when it points at minicloud (localhost), else None."""
    azure_endpoint = os.environ.get("AZURE_ENDPOINT_URL", "")
    if azure_endpoint and "localhost" in azure_endpoint:
        return azure_endpoint
    return None


def is_minicloud_active(params=None) -> bool:
    """Check if minicloud mode is active based on env vars or SCT config.

    ``params`` is optional because some call sites (log collector, cluster teardown) have
    no SCTConfiguration to hand. Pass it wherever one is available: the
    ``minicloud_endpoint_url`` param is the only way a test-case yaml can turn minicloud on,
    and while this looked at the environment alone, a yaml-only setup silently provisioned
    against the real cloud instead.

    Note: ``MINICLOUD_DOCKER`` (the image override) deliberately does NOT activate
    minicloud — selecting an image and redirecting a run are separate decisions. The
    scripts/ wrappers export ``SCT_MINICLOUD_ENDPOINT_URL`` explicitly to activate.
    """
    aws_endpoint = os.environ.get("AWS_ENDPOINT_URL", "")
    if aws_endpoint and "localhost" in aws_endpoint:
        return True

    gce_endpoint = os.environ.get("GCE_ENDPOINT_URL", "")
    if gce_endpoint and "localhost" in gce_endpoint:
        return True

    if get_minicloud_azure_endpoint():
        return True

    if os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", ""):
        return True

    return bool(params is not None and params.get("minicloud_endpoint_url"))


def get_minicloud_endpoint(params=None) -> str:
    """Get the minicloud endpoint URL.

    Reads the same sources as is_minicloud_active(), including the localhost
    condition on the SDK endpoint overrides: a host that exports a real-cloud
    AWS_ENDPOINT_URL must not hijack the endpoint of a yaml-activated run, and
    GCE_ENDPOINT_URL activates minicloud too, so it has to resolve here as well.
    AZURE_ENDPOINT_URL is skipped by design: it names the HTTPS Azure API on the
    TLS port, not this HTTP endpoint (see get_minicloud_tls_endpoint()).
    """
    endpoint = os.environ.get("SCT_MINICLOUD_ENDPOINT_URL", "")
    if endpoint:
        return endpoint
    for sdk_endpoint_var in ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL"):
        endpoint = os.environ.get(sdk_endpoint_var, "")
        if endpoint and "localhost" in endpoint:
            return endpoint
    if params is not None and (endpoint := params.get("minicloud_endpoint_url")):
        return endpoint
    return f"http://localhost:{MINICLOUD_PORT}"


def get_minicloud_tls_endpoint(endpoint: str | None = None) -> str:
    """Get minicloud's Azure HTTPS endpoint: a localhost AZURE_ENDPOINT_URL, else the HTTP host on the TLS port."""
    if azure_endpoint := get_minicloud_azure_endpoint():
        return azure_endpoint
    return f"https://{urlparse(endpoint or get_minicloud_endpoint()).hostname}:{MINICLOUD_TLS_PORT}"
