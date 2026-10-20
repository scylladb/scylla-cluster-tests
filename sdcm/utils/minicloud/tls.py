"""CA bundle helpers for the minicloud backends that talk TLS to the emulator."""

import contextlib
import os
import tempfile

import certifi

from sdcm.utils.minicloud.config import MINICLOUD_CA_BUNDLE_NAME, MinicloudConfig, MinicloudError
from sdcm.utils.minicloud.endpoint import AZURE_BACKENDS

SYSTEM_CA_BUNDLE_PATHS = (
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/cert.pem",
)


def system_ca_bundle() -> str:
    """Return the path of the host own CA bundle.

    REQUESTS_CA_BUNDLE is ignored when it already points at the minicloud bundle.
    """
    candidates = [os.environ.get("SSL_CERT_FILE", "")]
    requests_bundle = os.environ.get("REQUESTS_CA_BUNDLE", "")
    if requests_bundle and not requests_bundle.endswith(MINICLOUD_CA_BUNDLE_NAME):
        candidates.append(requests_bundle)

    candidates.extend(SYSTEM_CA_BUNDLE_PATHS)
    candidates.append(certifi.where())
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return candidate

    raise MinicloudError("no system CA bundle found to extend with the minicloud certificate")


def build_ca_bundle(config: MinicloudConfig) -> str:
    """Merge the system CAs and the minicloud certificate into one bundle, return its path."""
    if not os.path.isfile(config.minicloud_cert_path):
        raise MinicloudError(
            f"minicloud TLS certificate {config.minicloud_cert_path} not found: the container "
            "has not published it yet, or it runs without TLS support"
        )
    bundle_path = config.ca_bundle_path
    bundle_dir = os.path.dirname(bundle_path)

    try:
        with open(system_ca_bundle()) as fh:
            system_pem = fh.read()
        with open(config.minicloud_cert_path) as fh:
            minicloud_pem = fh.read()
        os.makedirs(bundle_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=bundle_dir, prefix=f".{MINICLOUD_CA_BUNDLE_NAME}.")

        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(f"{system_pem}\n{minicloud_pem}")
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, bundle_path)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
    except OSError as exc:
        raise MinicloudError(f"failed to build the minicloud CA bundle {bundle_path}: {exc}") from exc

    return bundle_path


def ca_bundle_for(config: MinicloudConfig, backend: str) -> str | None:
    """Return the CA bundle for the backend, or None when it does not talk TLS to minicloud."""
    if backend not in AZURE_BACKENDS:
        return None
    return build_ca_bundle(config)
