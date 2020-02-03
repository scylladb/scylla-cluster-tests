from functools import wraps
import os
import traceback
import concurrent.futures
import logging

from sdcm.utils.common import generate_random_string

LOGGER = logging.getLogger(__name__)


def raise_event_on_failure(func):
    """
    Decorate a function that is running inside a thread,
    when exception is raised in this function,
    will raise an Error severity event
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = None
        _test_pid = os.getpid()
        from sdcm.sct_events import ThreadFailedEvent
        try:
            result = func(*args, **kwargs)
        except Exception as ex:  # pylint: disable=broad-except
            ThreadFailedEvent(message=str(ex), traceback=traceback.format_exc())

        return result
    return wrapper


class DockerBasedStressThread:  # pylint: disable=too-many-instance-attributes
    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, node_list=None,  # pylint: disable=too-many-arguments
                 round_robin=False, params=None):
        self.loader_set = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.stress_num = stress_num
        self.node_list = node_list if node_list else []
        self.round_robin = round_robin
        self.params = params if params else dict()

        self.executor = None
        self.results_futures = []
        self.max_workers = 0
        self.shell_marker = generate_random_string(20)

    def run(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
            LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.loader_set.nodes

        self.max_workers = len(loaders) * self.stress_num
        LOGGER.debug("Starting %d %s Worker threads", self.max_workers, self.__class__.__name__)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(self.stress_num):
                self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader_idx, cpu_idx))]

        return self

    def _run_stress(self, loader, loader_idx, cpu_idx):
        raise NotImplementedError()

    def get_results(self):
        ret = []
        results = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return ret

    def verify_results(self):
        ret = []
        results = []
        errors = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return ret, errors

    def kill(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
        else:
            loaders = self.loader_set.nodes
        for loader in loaders:
            loader.remoter.run(cmd=f"docker rm -f `docker ps -a -q --filter label=shell_marker={self.shell_marker}`",
                               timeout=60,
                               ignore_status=True)
