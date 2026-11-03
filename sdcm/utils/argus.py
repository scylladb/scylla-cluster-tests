import datetime
import json
import logging
import re
import os
import tempfile
from pathlib import Path
from typing import Optional
from uuid import UUID

from botocore.exceptions import ClientError

from argus.client.sct.client import ArgusSCTClient
from argus.client.base import ArgusClientError

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

TEST_ERROR_PATTERNS = (
    re.compile(r"SpotTerminationEvent", re.IGNORECASE),
    re.compile(r"ZonalAllocationFailed", re.IGNORECASE),
    re.compile(r"source=[\w]+.SetUp\(\).+exception=403 FORBIDDEN QUOTA_EXCEEDED", re.IGNORECASE | re.DOTALL),
    re.compile(r"source=[\w]+.SetUp\(\).+InsufficientInstanceCapacity", re.IGNORECASE | re.DOTALL),
    re.compile(r"source=[\w]+.SetUp\(\).+InvalidInstanceType", re.IGNORECASE | re.DOTALL),
    re.compile(r"source=[\w]+.SetUp\(\).+is not (supported|available) in region", re.IGNORECASE | re.DOTALL),
)


def is_test_error(events: dict[str, list[str]]) -> bool:
    """
    Test whether or not the result of the test can be considered as a TEST_ERROR
    status for Argus. TEST_ERROR status is used for times when the SCT failure is caused by
    either infrastructure or problems within SCT itself.

    """
    return any(
        pattern.search(event)
        for event in [*events.get("CRITICAL"), *events.get("ERROR")]
        for pattern in TEST_ERROR_PATTERNS
    )


class Argus:
    """Process-wide handle to the current Argus client.

    Exists so that low-level, thread-based consumers - namely ArgusEventCollector
    (sdcm.sct_events.argus), which starts running before a client is available - can read
    the current client without a direct reference to TestConfig, which owns it. Importing
    TestConfig from here isn't an option: sdcm.test_config imports from
    sdcm.sct_events.argus, so a reverse import would be circular.
    """

    INSTANCE: Optional["Argus"] = None

    def __init__(self, client: ArgusSCTClient):
        self._client = client

    @classmethod
    def init_global(cls, client: ArgusSCTClient):
        """(Re-)point the process-wide Argus singleton at *client*.

        In a real SCT run this is only ever called once, from
        TestConfig.start_argus_event_pipeline() (itself called once, from
        ClusterTester.init_argus_run()). It always overwrites rather than guarding with a
        one-shot flag purely so that repeated calls stay safe/idempotent for callers
        outside that single-call path - e.g. tests that reuse the TestConfig class across
        cases, or standalone scripts (resources_cleanup.py, Argus.get(init_default=True))
        that construct a client directly via get_argus_client(init_global=True).
        """
        cls.INSTANCE = cls(client)

    @classmethod
    def get(cls, init_default=False) -> "Argus":
        # Only the lazy default below needs a "don't clobber an explicit client" guard;
        # checking INSTANCE is None (rather than a separate one-shot flag) means an
        # explicit init_global() call always wins, however many times it's called.
        if init_default and cls.INSTANCE is None:
            cls.init_global(
                get_argus_client(
                    run_id=os.environ.get("SCT_TEST_ID"), init_global=False, use_tunnel=get_argus_use_tunnel_from_env()
                )
            )
        return cls.INSTANCE

    @property
    def client(self) -> ArgusSCTClient:
        return self._client


class ArgusError(Exception):
    def __init__(self, message: str, *args: list) -> None:
        self._message = message
        super().__init__(*args)

    @property
    def message(self):
        return self._message


class ReplayOnlyArgusSCTClient(ArgusSCTClient):
    """ArgusSCTClient that records API calls to a replay log without making HTTP requests.

    Prefer this over ArgusSCTClient(replay_log_only=True) — the type itself signals
    replay-only mode, so callers can query capabilities via has_heartbeat instead of
    reaching for the private _replay_log_only attribute.
    """

    has_heartbeat: bool = False

    def __init__(self, run_id: UUID | str, log_dir: str, **kwargs) -> None:
        super().__init__(run_id=run_id, auth_token="", base_url="", log_dir=log_dir, replay_log_only=True, **kwargs)


def get_argus_use_tunnel_from_env() -> bool:
    val = os.environ.get("SCT_ARGUS_USE_SSH_TUNNEL", "false")
    return val.lower() in ("true", "yes", "y", "1")


def is_uuid(uuid) -> bool:
    if isinstance(uuid, UUID):
        return True

    try:
        UUID(uuid)
        return True
    except Exception:  # noqa: BLE001  # any parse failure means it isn't a UUID
        return False


def get_argus_client(
    run_id: UUID | str, use_tunnel: bool, init_global=True, log_dir: str | None = None
) -> ArgusSCTClient:
    if not is_uuid(run_id):
        raise ArgusError("Malformed UUID provided")

    try:
        creds = KeyStore().get_argus_rest_credentials_per_provider()
    except (ClientError, KeyError, json.JSONDecodeError) as exc:
        raise ArgusError(f"Failed to fetch Argus credentials: {exc}") from exc
    # The replay log is always created, so a real log_dir is required. Callers that
    # care where it lands (e.g. so logcollector picks it up) should pass log_dir
    # explicitly; otherwise it falls back to a temp dir.
    argus_client = ArgusSCTClient(
        run_id=run_id,
        auth_token=creds["token"],
        base_url=creds["baseUrl"],
        extra_headers=creds.get("extra_headers"),
        use_tunnel=use_tunnel,
        log_dir=log_dir or tempfile.gettempdir(),
    )

    if init_global:
        Argus.init_global(argus_client)

    return argus_client


def terminate_resource_in_argus(client: ArgusSCTClient, resource_name: str):
    try:
        client.terminate_resource(name=resource_name, reason="clean-resources: Graceful Termination")
    except ArgusClientError as exc:
        if len(exc.args) >= 3 and exc.args[2] == 404:
            LOGGER.warning("%s doesn't exist in Argus", resource_name)
        else:
            LOGGER.error("Failure to communicate resource deletion to Argus", exc_info=True)


def report_scylla_yaml_to_argus(tester):
    if not tester:
        LOGGER.warning("Unable to report scylla yaml - no tester object provided.")
        return
    if not tester.db_cluster:
        LOGGER.warning("Unable to report scylla yaml - cluster isn't set up.")
        return
    node = next(iter(tester.db_cluster.nodes), None)
    if not node:
        LOGGER.warning("Unable to report scylla yaml - no nodes available.")
        return
    if not node.remoter:
        LOGGER.warning("Unable to report scylla yaml - remoter is not available.")
        return
    with node.remote_scylla_yaml() as scylla_yml:
        content = json.dumps(scylla_yml.model_dump())
        client = node.test_config.argus_client()
        try:
            client.sct_submit_config(name="scylla_yaml", content=content)
        except ArgusClientError:
            LOGGER.warning("Backend exception reporting scylla.yaml to Argus.", exc_info=True)
        except Exception:  # noqa: BLE001
            LOGGER.warning("General error reporting scylla yaml.", exc_info=True)


def report_stress_command(client: ArgusSCTClient, stress_cmd: str, loader_name: str, log_path: str):
    try:
        client.add_stress_command(
            command=stress_cmd,
            ts=datetime.datetime.now(datetime.UTC).timestamp(),
            log_name=Path(log_path).name,
            loader_name=loader_name,
        )
    except ArgusClientError:
        LOGGER.warning("Backend exception reporting a stress command to Argus", exc_info=True)
    except Exception:  # noqa: BLE001
        LOGGER.warning("Unable to report stress command.", exc_info=True)


def create_proxy_argus_s3_url(url: str, general: bool = False) -> str:
    if general:
        if match := re.match(
            r"(https:\/\/)?(?P<bucket>[\w\-]*)\.s3(?P<region>\.[\w\-\d]*)?\.amazonaws.com\/(?P<key>.+)", url
        ):
            return f"https://argus.scylladb.com/api/v1/s3/{match.group('bucket')}/{match.group('key')}"
        return url

    if url.endswith(".png"):
        url_format = "https://argus.scylladb.com/api/v1/tests/scylla-cluster-tests/{}/screenshot/{}"
    else:
        url_format = "https://argus.scylladb.com/api/v1/tests/scylla-cluster-tests/{}/log/{}/download"

    return url_format
