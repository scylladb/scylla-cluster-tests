import logging
import os
import queue
from textwrap import dedent
from datetime import datetime
from typing import Optional, Dict

import requests
from sdcm.keystore import KeyStore
from sdcm.utils.common import get_my_ip
from sdcm.utils.decorators import retrying
from sdcm.utils.docker_utils import ContainerManager
from sdcm.utils.get_username import get_username
from sdcm.utils.ldap import LdapServerNotReady
from sdcm.utils.metaclasses import Singleton


LOGGER = logging.getLogger(__name__)


class TestConfig(metaclass=Singleton):  # pylint: disable=too-many-public-methods
    TEST_DURATION = 60
    RSYSLOG_SSH_TUNNEL_LOCAL_PORT = 5000
    IP_SSH_CONNECTIONS = 'private'
    KEEP_ALIVE_DB_NODES = False
    KEEP_ALIVE_LOADER_NODES = False
    KEEP_ALIVE_MONITOR_NODES = False

    REUSE_CLUSTER = False
    MIXED_CLUSTER = False
    MULTI_REGION = False
    BACKTRACE_DECODING = False
    INTRA_NODE_COMM_PUBLIC = False
    RSYSLOG_ADDRESS = None
    LDAP_ADDRESS = None
    DECODING_QUEUE = None

    _test_id = None
    _test_name = None
    _logdir = None
    _tester_obj = None

    backup_azure_blob_credentials = {}

    @classmethod
    def test_id(cls):
        return cls._test_id

    @classmethod
    def set_test_id(cls, test_id):
        if not cls._test_id:
            cls._test_id = str(test_id)
            test_id_file_path = os.path.join(cls.logdir(), "test_id")
            with open(test_id_file_path, "w") as test_id_file:
                test_id_file.write(str(test_id))
        else:
            LOGGER.warning("TestID already set!")

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
            os.environ['_SCT_TEST_LOGDIR'] = cls._logdir
        return cls._logdir

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
        cls.DECODING_QUEUE = queue.Queue()

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
    def keep_cluster(cls, node_type, val='destroy'):
        if "db_nodes" in node_type:
            cls.KEEP_ALIVE_DB_NODES = bool(val == 'keep')
        elif "loader_nodes" in node_type:
            cls.KEEP_ALIVE_LOADER_NODES = bool(val == 'keep')
        elif "monitor_nodes" in node_type:
            cls.KEEP_ALIVE_MONITOR_NODES = bool(val == 'keep')

    @classmethod
    def should_keep_alive(cls, node_type: Optional[str]) -> bool:
        if cls.TEST_DURATION >= 11 * 60:
            return True
        if node_type is None:
            return False
        if "db" in node_type:
            return cls.KEEP_ALIVE_DB_NODES
        if "loader" in node_type:
            return cls.KEEP_ALIVE_LOADER_NODES
        if "monitor" in node_type:
            return cls.KEEP_ALIVE_MONITOR_NODES
        return False

    @classmethod
    def mixed_cluster(cls, val=False):
        cls.MIXED_CLUSTER = val

    @classmethod
    def common_tags(cls) -> Dict[str, str]:
        job_name = os.environ.get('JOB_NAME')
        tags = dict(RunByUser=get_username(),
                    TestName=str(cls.test_name()),
                    TestId=str(cls.test_id()),
                    version=job_name.split('/', 1)[0] if job_name else "unknown")

        build_tag = os.environ.get('BUILD_TAG')
        if build_tag:
            tags["JenkinsJobTag"] = build_tag

        return tags

    @classmethod
    @retrying(n=20, sleep_time=6, allowed_exceptions=LdapServerNotReady)
    def configure_ldap(cls, node, use_ssl=False):
        ContainerManager.run_container(node, "ldap")
        if use_ssl:
            port = node.ldap_ports['ldap_ssl_port']
        else:
            port = node.ldap_ports['ldap_port']
        address = get_my_ip()
        cls.LDAP_ADDRESS = (address, port)
        if ContainerManager.get_container(node, 'ldap').exec_run("timeout 30s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReady("LDAP server didn't finish its startup yet...")

    @classmethod
    def configure_rsyslog(cls, node, enable_ngrok=False):
        ContainerManager.run_container(node, "rsyslog", logdir=cls.logdir())
        port = node.rsyslog_port
        LOGGER.info("rsyslog listen on port %s (config: %s)", port, node.rsyslog_confpath)

        if enable_ngrok:
            requests.delete('http://localhost:4040/api/tunnels/rsyslogd')

            tunnel = {
                "addr": port,
                "proto": "tcp",
                "name": "rsyslogd",
                "bind_tls": False
            }
            res = requests.post('http://localhost:4040/api/tunnels', json=tunnel)
            assert res.ok, "failed to add a ngrok tunnel [{}, {}]".format(res, res.text)
            ngrok_address = res.json()['public_url'].replace('tcp://', '')

            address, port = ngrok_address.split(':')
        else:
            address = get_my_ip()

        cls.RSYSLOG_ADDRESS = (address, port)

    @classmethod
    def get_startup_script(cls):
        post_boot_script = '#!/bin/bash'
        post_boot_script += dedent(r'''
               sudo sed -i 's/#MaxSessions \(.*\)$/MaxSessions 1000/' /etc/ssh/sshd_config
               sudo sed -i 's/#MaxStartups \(.*\)$/MaxStartups 60/' /etc/ssh/sshd_config
               sudo sed -i 's/#LoginGraceTime \(.*\)$/LoginGraceTime 15s/' /etc/ssh/sshd_config
               sudo systemctl restart sshd
               ''')
        if cls.RSYSLOG_ADDRESS:

            if cls.IP_SSH_CONNECTIONS == 'public' or TestConfig.MULTI_REGION:
                post_boot_script += dedent('''
                       sudo echo 'action(type="omfwd" Target="{0}" Port="{1}" Protocol="tcp")'>> /etc/rsyslog.conf
                       sudo systemctl restart rsyslog
                       '''.format('127.0.0.1', cls.RSYSLOG_SSH_TUNNEL_LOCAL_PORT))
            else:
                post_boot_script += dedent('''
                       sudo echo 'action(type="omfwd" Target="{0}" Port="{1}" Protocol="tcp")'>> /etc/rsyslog.conf
                       sudo systemctl restart rsyslog
                       '''.format(*cls.RSYSLOG_ADDRESS))  # pylint: disable=not-an-iterable

        post_boot_script += dedent(r'''
               sed -i -e 's/^\*[[:blank:]]*soft[[:blank:]]*nproc[[:blank:]]*4096/*\t\tsoft\tnproc\t\tunlimited/' \
               /etc/security/limits.d/20-nproc.conf
               echo -e '*\t\thard\tnproc\t\tunlimited' >> /etc/security/limits.d/20-nproc.conf
               ''')
        return post_boot_script

    @classmethod
    def set_ip_ssh_connections(cls, ip_type):
        cls.IP_SSH_CONNECTIONS = ip_type

    @classmethod
    def set_duration(cls, duration):
        cls.TEST_DURATION = duration
