"""Shared HTTP session factory with retry/backoff.

Every ``requests.Session`` in SCT should be created through this helper
to guarantee consistent retry behaviour across the framework.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_RETRYABLE_METHODS = frozenset(
    ["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
)

DEFAULT_STATUS_FORCELIST = frozenset([429, 500, 502, 503, 504])


def create_retry_session(
    retries: int = 3,
    backoff_factor: int = 1,
    status_forcelist: frozenset[int] = DEFAULT_STATUS_FORCELIST,
    allowed_methods: frozenset[str] = DEFAULT_RETRYABLE_METHODS,
) -> requests.Session:
    """Create a :class:`requests.Session` with a retry adapter on both schemes.

    Args:
        retries: Maximum number of retries per request.
        backoff_factor: Multiplier applied between retry attempts.
        status_forcelist: HTTP status codes that trigger a retry.
        allowed_methods: HTTP methods eligible for retry.

    Returns:
        A configured ``requests.Session``.
    """
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=allowed_methods,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
