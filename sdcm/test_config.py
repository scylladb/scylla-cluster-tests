from __future__ import annotations

import logging
import multiprocessing
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, TYPE_CHECKING
from uuid import uuid4

from argus.client.sct.client import ArgusSCTClient


from sdcm.keystore import KeyStore
from sdcm.provision.common.configuration_script import ConfigurationScriptBuilder
from sdcm.sct_events import Severity
from sdcm.sct_events.argus import enable_argus_posting, start_posting_argus_events
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.argus import Argus, ArgusError, ReplayOnlyArgusSCTClient, get_argus_client
from sdcm.utils.ci_tools import get_job_name
from sdcm.utils.net import get_my_ip
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.get_username import get_username
from sdcm.utils.ldap import LdapServerNotReady
from sdcm.utils.metaclasses import Singleton
from sdcm.utils.sct_agent_installer import generate_agent_api_key, save_agent_api_key, load_agent_api_key

if TYPE_CHECKING:
    from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


class TestConfig(metaclass=Singleton):
    __test__ = False  # This class is not a test case

    TEST_DURATION = 60
    TEST_WARMUP_TEARDOWN = 60
    SYSLOGNG_LOG_THROTTLE_PER_SECOND = 10000
    SYSLOGNG_SSH_TUNNEL_LOCAL_PORT = 5000
    VECTOR_SSH_TUNNEL_LOCAL_PORT = 5001
    IP_SSH_CONNECTIONS = "private"
    KEEP_ALIVE_DB_NODES = False
    KEEP_ALIVE_LOADER_NODES = False
    KEEP_ALIVE_MONITOR_NODES = False
    KEEP_ALIVE_VECTOR_STORE_NODES = False
    KEEP_ALIVE_DEDICATED_HOST = False

    REUSE_CLUSTER = False
    MIXED_CLUSTER = False
    MULTI_REGION = False
    BACKTRACE_DECODING = False
    INTRA_NODE_COMM_PUBLIC = False
    SYSLOGNG_ADDRESS = None
    VECTOR_ADDRESS = None
    LDAP_ADDRESS = None
    DECODING_QUEUE = None

    _test_id = None
    _test_name = None
    _logdir = None
    _latency_results_file_name = "latency_results.json"
    _latency_results_file_path = None
    _tester_obj = None
    _argus_client: ArgusSCTClient | None = None
    _argus_client_resolved: bool = False
    _agent_api_key = None

    backup_azure_blob_credentials = {}

    @classmethod
    def test_id(cls):
        return cls._test_id

    @classmethod
    def set_test_id_only(cls, test_id) -> bool:
        """Set the test ID in memory only, without persisting to disk.

        WARNING: This method should ONLY be used in non-main test phases such as:
        - Provisioning phase (provision-resources command)
        - Post-test reporting phase (collect-logs, update-argus-with-status, etc.)
        - Utility commands that need test_id for lookup but don't run the actual test

        For the main test execution phase (the actual test run), use set_test_id() instead,
        which also writes the test_id to disk for later reference by other tools.

        Args:
            test_id: The test ID to set

        Returns:
            bool: True if successfully set, False if test_id was already set
        """
        if not cls._test_id:
            cls._test_id = str(test_id)
            return True
        LOGGER.warning("TestID already set!")
        return False

    @classmethod
    def set_test_id(cls, test_id):
        """Set the test ID in memory and persist it to disk.

        WARNING: This method should ONLY be used in the main test execution phase
        (when actually running a test). It creates a test_id file in the log directory
        that is used by various tools (log collection, monitoring, reporting, etc.) to
        identify the main test run.

        For non-main phases (provisioning, post-test utilities, reporting), use
        set_test_id_only() instead to avoid creating multiple test_id files which
        could confuse log collection and other tools.

        Args:
            test_id: The test ID to set
        """
        if cls.set_test_id_only(test_id):
            test_id_file_path = os.path.join(cls.logdir(), "test_id")
            with open(test_id_file_path, "w", encoding="utf-8") as test_id_file:
                test_id_file.write(str(test_id))

    @classmethod
    def tester_obj(cls):
        return cls._tester_obj

    @classmethod
    def set_tester_obj(cls, tester_obj):
        if not cls._tester_obj:
            cls._tester_obj = tester_obj

    @classmethod
    def base_logdir(cls) -> str:
        return os.path.expanduser(os.environ.get("_SCT_LOGDIR", "~/sct-results"))

    @classmethod
    def make_new_logdir(cls, update_latest_symlink: bool, postfix: str = "") -> str:
        base = cls.base_logdir()
        logdir = os.path.join(base, datetime.now().strftime("%Y%m%d-%H%M%S-%f") + postfix)
        os.makedirs(logdir, exist_ok=True)
        LOGGER.info("New directory created: %s", logdir)
        if update_latest_symlink:
            latest_symlink = os.path.join(base, "latest")
            if os.path.islink(latest_symlink):
                os.remove(latest_symlink)
            os.symlink(os.path.relpath(logdir, base), latest_symlink)
            LOGGER.info("Symlink `%s' updated to `%s'", latest_symlink, logdir)
        return logdir

    @classmethod
    def logdir(cls) -> str:
        if not cls._logdir:
            cls._logdir = cls.make_new_logdir(update_latest_symlink=True)
            os.environ["_SCT_TEST_LOGDIR"] = cls._logdir
        return cls._logdir

    @classmethod
    def latency_results_file(cls):
        if not cls._latency_results_file_path:
            cls._latency_results_file_path = os.path.join(cls._logdir, cls._latency_results_file_name)
            with open(cls._latency_results_file_path, "w", encoding="utf-8"):
                pass
        return cls._latency_results_file_path

    @classmethod
    def test_name(cls):
        return cls._test_name

    @classmethod
    def set_test_name(cls, test_name):
        cls._test_name = test_name

    @classmethod
    def set_multi_region(cls, multi_region):
        cls.MULTI_REGION = multi_region

    @classmethod
    def set_decoding_queue(cls):
        cls.DECODING_QUEUE = multiprocessing.Queue()

    @classmethod
    def set_intra_node_comm_public(cls, intra_node_comm_public):
        cls.INTRA_NODE_COMM_PUBLIC = intra_node_comm_public

    @classmethod
    def set_backup_azure_blob_credentials(cls) -> None:
        cls.backup_azure_blob_credentials = KeyStore().get_backup_azure_blob_credentials()

    @classmethod
    def reuse_cluster(cls, val=False):
        cls.REUSE_CLUSTER = val

    @classmethod
    def keep_cluster(cls, node_type, val="destroy"):
        if "db_nodes" in node_type:
            cls.KEEP_ALIVE_DB_NODES = bool(val == "keep")
        elif "loader_nodes" in node_type:
            cls.KEEP_ALIVE_LOADER_NODES = bool(val == "keep")
        elif "monitor_nodes" in node_type:
            cls.KEEP_ALIVE_MONITOR_NODES = bool(val == "keep")
        elif "vector_store_nodes" in node_type:
            cls.KEEP_ALIVE_VECTOR_STORE_NODES = bool(val == "keep")
        elif "dedicated_host" in node_type:
            cls.KEEP_ALIVE_DEDICATED_HOST = bool(val == "keep")

    @classmethod
    def should_keep_alive(cls, node_type: Optional[str]) -> bool:  # noqa: PLR0911
        if node_type is None:
            return False
        if "db" in node_type:
            return cls.KEEP_ALIVE_DB_NODES
        if "loader" in node_type:
            return cls.KEEP_ALIVE_LOADER_NODES
        if "monitor" in node_type:
            return cls.KEEP_ALIVE_MONITOR_NODES
        if "vector" in node_type:
            return cls.KEEP_ALIVE_VECTOR_STORE_NODES
        if "dedicated_host" in node_type:
            return cls.KEEP_ALIVE_DEDICATED_HOST
        return False

    @classmethod
    def mixed_cluster(cls, val=False):
        cls.MIXED_CLUSTER = val

    @classmethod
    def common_tags(cls, params: "SCTConfiguration | None" = None) -> Dict[str, str]:
        job_name = os.environ.get("JOB_NAME")
        tags = dict(
            RunByUser=get_username(),
            TestName=str(cls.test_name()),
            TestId=str(cls.test_id()),
            version=job_name.split("/", 1)[0] if job_name else "unknown",
            CreatedBy="SCT",
        )

        build_tag = os.environ.get("BUILD_TAG")
        if build_tag:
            tags["JenkinsJobTag"] = build_tag
            match = re.match(r"^(.*)-(\d+)$", build_tag)
            tags["JenkinsJob"] = match.group(1) if match else build_tag

        # Add BillingProject tag if available
        if cls._tester_obj:
            billing_project = cls._tester_obj.params.get("billing_project")
            if billing_project:
                tags["billing_project"] = billing_project
        elif params:
            billing_project = params.get("billing_project")
            if billing_project:
                tags["billing_project"] = billing_project
        # If configuration creation fails, fall back to environment variable
        elif billing_project := os.environ.get("SCT_BILLING_PROJECT"):
            tags["billing_project"] = billing_project
        return tags

    @classmethod
    @retrying(n=20, sleep_time=6, allowed_exceptions=LdapServerNotReady)
    def configure_ldap(cls, node, use_ssl=False):
        ContainerManager.run_container(node, "ldap")
        if use_ssl:
            port = node.ldap_ports["ldap_ssl_port"]
        else:
            port = node.ldap_ports["ldap_port"]
        address = get_my_ip()
        cls.LDAP_ADDRESS = (address, port)
        if ContainerManager.get_container(node, "ldap").exec_run("timeout 30s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReady("LDAP server didn't finish its startup yet...")

    @classmethod
    def _link_running_syslog_logdir(cls, syslog_logdir):
        current_logdir = cls.logdir()
        if not syslog_logdir:
            raise RuntimeError("Can't fund syslog docker log directory")
        if syslog_logdir == current_logdir:
            LOGGER.debug("Syslog docker is running on the same directory where SCT is running")
            return
        LOGGER.debug(
            "Syslog docker is running on the another directory. Linking it's directory %s to %s",
            syslog_logdir,
            current_logdir,
        )
        current_logdir = Path(current_logdir) / "hosts"
        docker_logdir = Path(syslog_logdir) / "hosts"
        if current_logdir.exists():
            current_logdir.rmdir()
        current_logdir.symlink_to(target=docker_logdir)

    @classmethod
    def configure_syslogng(cls, node):
        ContainerManager.run_container(node, "syslogng", logdir=cls.logdir())
        cls._link_running_syslog_logdir(node.syslogng_log_dir)
        port = node.syslogng_port
        LOGGER.info("syslog-ng listen on port %s (config: %s)", port, node.syslogng_confpath)
        address = get_my_ip()
        cls.SYSLOGNG_ADDRESS = (address, port)

    @classmethod
    def configure_vector(cls, node):
        ContainerManager.run_container(node, "vector", logdir=cls.logdir())
        cls._link_running_syslog_logdir(node.vector_log_dir)
        port = node.vector_port
        LOGGER.info("vector listen on port %s (config: %s)", port, node.vector_confpath)
        address = get_my_ip()
        cls.VECTOR_ADDRESS = (address, port)

    @classmethod
    def ensure_agent_api_key(cls):
        """Ensure SCT agent API key is available"""
        agent_config = cls._tester_obj.params.get("agent") if cls._tester_obj else {}
        if not (agent_config.get("enabled") and agent_config.get("binary_url")):
            return

        if cls.agent_api_key():
            return

        cls.generate_and_save_agent_api_key()
        LOGGER.info("Generated new SCT agent API key for test %s", cls.test_id())

    @classmethod
    def configure_xcloud_connectivity(cls, node, params):
        if node.xcloud_connect_supported(params):
            ContainerManager.run_container(node, "xcloud_connect", params=params)
            node.xcloud_connect_wait_to_be_ready()

    @classmethod
    def get_startup_script(cls) -> str:
        host_port = cls.get_logging_service_host_port()
        if not host_port or not host_port[0]:
            host_port = None

        agent_config = cls._tester_obj.params.get("agent")
        install_agent = bool(agent_config.get("enabled", False) and agent_config.get("binary_url"))

        return ConfigurationScriptBuilder(
            syslog_host_port=host_port,
            logs_transport=cls._tester_obj.params.get("logs_transport") if cls._tester_obj else "syslog-ng",
            test_config=cls(),
            install_agent=install_agent,
        ).to_string()

    @classmethod
    def get_logging_service_host_port(cls) -> tuple[str, int] | None:
        if cls.SYSLOGNG_ADDRESS:
            if cls.IP_SSH_CONNECTIONS == "public":
                syslogng_host = "127.0.0.1"
                syslogng_port = cls.SYSLOGNG_SSH_TUNNEL_LOCAL_PORT
            else:
                syslogng_host, syslogng_port = cls.SYSLOGNG_ADDRESS  # pylint: disable=unpacking-non-sequence
            return syslogng_host, syslogng_port
        elif cls.VECTOR_ADDRESS:
            if cls.IP_SSH_CONNECTIONS == "public":
                vector_host = "127.0.0.1"
                vector_port = cls.VECTOR_SSH_TUNNEL_LOCAL_PORT
            else:
                vector_host, vector_port = cls.VECTOR_ADDRESS
            return vector_host, vector_port
        else:
            return None

    @classmethod
    def set_ip_ssh_connections(cls, ip_type):
        cls.IP_SSH_CONNECTIONS = ip_type

    @classmethod
    def set_duration(cls, duration):
        cls.TEST_DURATION = duration

    @classmethod
    def argus_client(cls) -> ArgusSCTClient:
        if cls._argus_client is None:
            cls._argus_client = cls._create_replay_only_client()
        return cls._argus_client

    @classmethod
    def _create_replay_only_client(cls, test_id: str | None = None) -> ReplayOnlyArgusSCTClient:
        """Create a replay-only Argus client.

        All API calls are recorded to a JSONL replay log but no HTTP requests
        are made. This replaces the previous MagicMock fallback, ensuring that
        all intended Argus submissions are captured for later replay.
        """
        run_id = test_id or cls.test_id() or str(uuid4())
        return ReplayOnlyArgusSCTClient(run_id=run_id, log_dir=cls.logdir())

    @classmethod
    def _close_argus_client(cls) -> None:
        # Closing avoids orphaning the client's replay-log writer thread and atexit handler.
        if cls._argus_client is not None:
            try:
                cls._argus_client.close()
            except Exception:  # noqa: BLE001
                LOGGER.warning("Failed to close existing Argus client", exc_info=True)
            cls._argus_client = None

    @classmethod
    @retrying(n=3, sleep_time=5, allowed_exceptions=(ArgusError,))
    def _connect_argus_client(cls, run_id: str, params: dict) -> ArgusSCTClient:
        return get_argus_client(
            run_id=run_id,
            use_tunnel=params.get("argus_use_ssh_tunnel"),
            log_dir=cls.logdir(),
            # Argus.init_global() must only happen in start_argus_event_pipeline() -
            # get_argus_client() defaults to wiring it as a side effect of a successful
            # connection, which would prematurely wire the singleton from this method's
            # caller (SCTConfiguration.update_argus_with_version(), during config
            # resolution), even though that caller never wires the event pipeline itself.
            init_global=False,
        )

    @classmethod
    def init_argus_client(cls, params: dict, test_id: str | None = None) -> None:
        """Resolve cls._argus_client once per process - real if Argus is enabled and
        reachable, replay-only otherwise - and stick with that decision for good.

        This is called from more than one place for the same test run
        (SCTConfiguration.update_argus_with_version() during config resolution, then
        ClusterTester.init_argus_run() at the actual start of the test); test_id is fixed
        for the whole process before either call, so there's nothing to gain from
        re-resolving on a later call - it would just be either a no-op re-connect to the
        same run, or throwing away a working connection for no reason. A transient
        connection failure is instead retried *within* this call (see
        _connect_argus_client above); once that's exhausted, the run stays in
        replay-only mode rather than trying again on a later call - every intended
        submission is still captured in the replay log for later bulk push either way.
        """
        if cls._argus_client_resolved:
            return

        run_id = test_id or cls.test_id()
        real_client = None
        if params.get("enable_argus") and get_job_name() != "local_run":
            LOGGER.info("Initializing Argus connection...")
            try:
                real_client = cls._connect_argus_client(run_id=run_id, params=params)
            except ArgusError as exc:
                LOGGER.warning("Failed to initialize argus client, falling back to replay-only mode: %s", exc.message)

        # Replace whatever's there now - e.g. a replay-only client lazily created by
        # argus_client() before this ran - closing it first so its writer thread and
        # atexit handler aren't orphaned.
        cls._close_argus_client()
        cls._argus_client = real_client or cls._create_replay_only_client(test_id=run_id)
        cls._argus_client_resolved = True

    @classmethod
    def start_argus_event_pipeline(cls) -> None:
        """Wire the real-time event pipeline (collector/aggregator/postman) to whatever
        client init_argus_client() resolved, so events raised during the run reach at
        least the replay log for later bulk push - instead of being silently dropped, the
        way they were when nothing wired the pipeline up in replay-only mode.

        Call this once, at the actual start of a test (ClusterTester.init_argus_run()) -
        not from init_argus_client()'s other caller (SCTConfiguration's config-resolution
        path), which only needs a client for a couple of synchronous submissions and has
        no business touching the live event pipeline.
        """
        try:
            Argus.init_global(cls._argus_client)
            enable_argus_posting()
            start_posting_argus_events()
        except (RuntimeError, AttributeError) as exc:
            # RuntimeError: no default events-processes registry exists yet at all.
            # AttributeError: the registry exists but the argus postman process isn't
            # registered in it yet - get_events_process() returns None instead of raising,
            # and enable_argus_posting()/start_posting_argus_events() call straight into it.
            # Either way, the events pipeline isn't ready; skip wiring it rather than block
            # setUp() waiting on it - in production the event_system fixture always starts
            # it before init_argus_run() gets here (see ArgusEventCollector.run()), so this
            # only fires for callers with a non-standard startup order.
            LOGGER.warning("Skipping setting up argus events: %s", exc)

        if isinstance(cls._argus_client, ReplayOnlyArgusSCTClient):
            TestFrameworkEvent(
                source=cls.__name__,
                source_method="start_argus_event_pipeline",
                message="Argus is disabled by configuration, using replay-only mode",
                severity=Severity.WARNING,
            ).publish_or_dump()

    @classmethod
    def agent_api_key(cls) -> str | None:
        return cls._agent_api_key

    @classmethod
    def set_agent_api_key(cls, api_key: str) -> None:
        if not api_key:
            raise ValueError("Agent API key cannot be empty")
        cls._agent_api_key = api_key

    @classmethod
    def generate_and_save_agent_api_key(cls) -> str:
        """Generate a new SCT agent API key and save it to SCT test results directory"""
        api_key = generate_agent_api_key()
        save_agent_api_key(cls.logdir(), api_key)
        cls.set_agent_api_key(api_key)
        LOGGER.info("New SCT agent API key has been generated and set for test %s", cls.test_id())
        return api_key

    @classmethod
    def load_agent_api_key_from_logdir(cls, logdir: str | None = None) -> str | None:
        """Load SCT agent API key from SCT test results directory"""
        api_key = load_agent_api_key(logdir or cls.logdir())
        if api_key:
            cls.set_agent_api_key(api_key)
        LOGGER.info("Existing SCT agent API key has been loaded and set for test %s", cls.test_id())
        return api_key
