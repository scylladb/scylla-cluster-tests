import unittest
import tempfile
import logging
import shutil
import os.path

from sdcm.cluster import BaseNode
from sdcm.sct_events import start_events_device, stop_events_device


class DummyNode(BaseNode):
    _database_log = None

    @property
    def private_ip_address(self):
        return '127.0.0.1'

    @property
    def public_ip_address(self):
        return '127.0.0.1'

    def start_task_threads(self):
        # disable all background threads
        pass

    @property
    def database_log(self):
        return self._database_log

    @database_log.setter
    def database_log(self, x):
        self._database_log = x


class DummeyOutput(object):
    def __init__(self, stdout):
        self.stdout = stdout


class DummyRemote(object):
    def run(self, *args, **kwargs):
        logging.info(args, kwargs)
        return DummeyOutput(args[0])


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestBaseNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir, timeout=5)

        cls.node = DummyNode(name='test_node', parent_cluster=None,
                             base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.remoter = DummyRemote()

        cls.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database.log')

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def test_search_database_log(self):
        critical_errors = self.node.search_database_log()
        self.assertEqual(len(critical_errors), 4)

        for line_number, line in critical_errors:
            print line

    def test_search_database_log_teardown(self):
        critical_errors = self.node.search_database_log(start_from_beginning=True, publish_events=False)
        self.assertEqual(len(critical_errors), 4)

        for line_number, line in critical_errors:
            print line

    def test_search_database_log_specific_log(self):
        errors = self.node.search_database_log(search_pattern='Failed to load schema version', start_from_beginning=True, publish_events=False)
        self.assertEqual(len(errors), 2)

        for line_number, line in errors:
            print line_number, line
