"""Helpers for discovering latest Scylla package URLs from S3."""

import functools
import logging

from boto3.session import Session
from botocore import UNSIGNED
from botocore.client import Config

logger = logging.getLogger(__name__)


def aws_bucket_ls(s3_url: str) -> list[str]:
    """List files in a public S3 bucket path, sorted by last_modified.

    Args:
        s3_url: S3 URL in the form ``downloads.scylladb.com/path/to/prefix``
            (without ``https://`` scheme) or
            ``https://s3.amazonaws.com/bucket/prefix``.

    Returns:
        List of filenames (prefix stripped) sorted oldest-first.
    """
    clean_url = s3_url.replace("https://s3.amazonaws.com/", "")
    bucket_object = clean_url.split("/")
    prefix = "/".join(bucket_object[1:])

    s3_resource = Session().resource(service_name="s3", config=Config(signature_version=UNSIGNED))
    bucket = s3_resource.Bucket(bucket_object[0])

    files_in_bucket = bucket.objects.filter(Prefix=prefix)

    return [f.key.replace(prefix + "/", "") for f in sorted(files_in_bucket, key=lambda x: x.last_modified)]


@functools.lru_cache(maxsize=4)
def latest_unified_package(arch: str = "x86_64") -> str | None:
    """Find the latest scylla-unified package URL for the given architecture.

    Args:
        arch: CPU architecture — ``x86_64`` or ``aarch64``.

    Returns:
        Full HTTPS URL to the package, or ``None`` if not found.
    """
    latest_location = "downloads.scylladb.com/unstable/scylla/master/relocatable/latest"
    file_list = aws_bucket_ls(latest_location)
    for filename in file_list:
        if "scylla-unified" in filename and arch in filename:
            return f"https://{latest_location}/{filename}"
    return None
