import unittest
import time
import os
import tempfile
import multiprocessing

from sdcm.services.base import NodeService, ClusterService, DetachService, \
    NodeThreadService, NodeProcessService, \
    ClusterThreadService, ClusterProcessService, \
    DetachThreadService, DetachProcessService, get_by_instance, get_from_context, get_from_all_sources
from sdcm.cluster import BaseNode
from parameterized import parameterized

import logging


class FakeCluster:
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)


class FakeNode(BaseNode):
    def __init__(self, remoter, logdir):
        self.remoter = remoter
        os.makedirs(logdir, exist_ok=True)
        self.logdir = logdir

    def wait_ssh_up(self, verbose=False):
        return True


class NodeThreadTestService(NodeThreadService):
    _interval = 0
    raise_event = False

    def __init__(self, node):
        super().__init__(node)
        self.job_completed = False

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self.job_completed = True
        return 0


class NodeProcessTestService(NodeProcessService):
    _interval = 0
    raise_event = False

    def __init__(self, cluster):
        super().__init__(cluster)
        self._job_completed = multiprocessing.Event()

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self._job_completed.set()
        return 0

    @property
    def job_completed(self):
        return self._job_completed.is_set()


class ClusterThreadTestService(ClusterThreadService):
    _interval = 0
    raise_event = False

    def __init__(self, node):
        super().__init__(node)
        self.job_completed = False

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self.job_completed = True
        return 0


class ClusterProcessTestService(ClusterProcessService):
    _interval = 0
    raise_event = False

    def __init__(self, cluster):
        super().__init__(cluster)
        self._job_completed = multiprocessing.Event()

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self._job_completed.set()
        return 0

    @property
    def job_completed(self):
        return self._job_completed.is_set()


class DetachThreadTestService(DetachThreadService):
    _interval = 0
    raise_event = False

    def __init__(self):
        super().__init__()
        self.job_completed = False

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self.job_completed = True
        return 0


class DetachProcessTestService(DetachProcessService):
    _interval = 0
    raise_event = False

    def __init__(self):
        super().__init__()
        self._job_completed = multiprocessing.Event()

    def set_raise_event(self, val: bool):
        self.raise_event = val

    def _service_body(self) -> int:
        if self.raise_event:
            raise RuntimeError("Exception occurred during service run")
        self._job_completed.set()
        return 0

    @property
    def job_completed(self):
        return self._job_completed.is_set()


class ServicesUnitTests(unittest.TestCase):
    svc = None

    def setUp(self) -> None:
        logging.basicConfig(level=logging.DEBUG)
        get_from_all_sources().kill().cleanup()

    def tearDown(self) -> None:
        # if self.svc is not None:
        if self.svc:
            self.svc.stop()
            self.svc.cleanup()

    def _init_service(self, cls, raise_event):
        if issubclass(cls, NodeService):
            self.svc = cls(FakeNode(remoter=None, logdir=tempfile.mkdtemp()))
        elif issubclass(cls, ClusterService):
            self.svc = cls(FakeCluster())
        elif issubclass(cls, DetachService):
            self.svc = cls()
        else:
            raise ValueError(f"Unknown class was provided {cls.__name__}")
        if raise_event:
            self.svc.set_raise_event(raise_event)

    def _get_services(self):
        if isinstance(self.svc, NodeService):
            return get_by_instance(self.svc._node)
        elif isinstance(self.svc, ClusterService):
            return get_by_instance(self.svc._cluster)
        elif isinstance(self.svc, DetachService):
            return get_from_context()

    @parameterized.expand([
        ("TestNodeThreadService_False", False, NodeThreadTestService),
        ("TestNodeProcessService_False", False, NodeProcessTestService),
        ("TestClusterThreadService_False", False, ClusterThreadTestService),
        ("TestClusterProcessService_False", False, ClusterProcessTestService),
        ("TestDetachThreadService_False", False, DetachThreadTestService),
        ("TestDetachProcessService_False", False, DetachProcessTestService),
        ("TestNodeThreadService_True", True, NodeThreadTestService),
        ("TestNodeProcessService_True", True, NodeProcessTestService),
        ("TestClusterThreadService_True", True, ClusterThreadTestService),
        ("TestClusterProcessService_True", True, ClusterProcessTestService),
        ("TestDetachThreadService_True", True, DetachThreadTestService),
        ("TestDetachProcessService_True", True, DetachProcessTestService)
    ])
    def test_unit_test(self, name, raise_event, cls):
        self._init_service(cls, raise_event)
        self.svc.start()
        end_time = time.perf_counter() + 0.5
        while not self.svc.is_service_alive() and end_time >= time.perf_counter():
            pass
        registered_services = self._get_services()
        self.assertEqual(1, len(registered_services),
                         msg=f"Result: {','.join(registered_services.names())}")
        self.assertEqual(1, len(registered_services.alive()),
                         msg=f"Result: {','.join(registered_services.alive().names())}")
        self.assertEqual(1, len(registered_services.find_by_name(self.svc.name)),
                         msg=f"Result: {','.join(registered_services.find_by_name(self.svc.name).names())}")
        self.assertEqual(1, len(registered_services.find_no_dependants()))
        self.assertEqual(0, len(registered_services.find_by_stop_priority(0, 50)))
        self.assertEqual(1, len(registered_services.find_by_stop_priority(50, 51)))
        self.assertEqual(0, len(registered_services.find_by_stop_priority(50, 50)))
        self.assertEqual(1, len(registered_services.find_by_stop_priority(50)))
        self.assertEqual(1, len(registered_services.find_by_stop_priority(50, 100)))
        self.assertEqual(True, self.svc.is_service_alive())
        while not self.svc.job_completed and end_time >= time.perf_counter():
            pass
        self.svc.stop()
        did_it_stop = self.svc.wait_till_stopped(0.5)
        self.assertEqual(True, did_it_stop)
        self.assertEqual(not raise_event, self.svc.job_completed)
        self.assertEqual(False, self.svc.is_service_alive())

        registered_services = self._get_services()
        self.assertEqual(1, len(registered_services))
        self.assertEqual(0, len(registered_services.alive()))
        self.assertEqual(1, len(registered_services.find_by_name(self.svc.name)))
        self.assertEqual(1, len(registered_services.find_no_dependants()))
        registered_services.kill()
        registered_services.cleanup()
        registered_services = self._get_services()
        self.assertEqual(0, len(registered_services),
                         msg=f"Result: {','.join(registered_services.names())}")
        self.assertEqual(0, len(registered_services.alive()),
                         msg=f"Result: {','.join(registered_services.alive().names())}")
        self.assertEqual(0, len(registered_services.find_by_name(self.svc.name)),
                         msg=f"Result: {','.join(registered_services.find_by_name(self.svc.name).names())}")
        self.assertEqual(0, len(registered_services.find_no_dependants()),
                         msg=f"Result: {','.join(registered_services.find_no_dependants().names())}")
