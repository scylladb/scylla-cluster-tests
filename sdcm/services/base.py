from typing import Union, Optional, List, Type, Iterator, Iterable, OrderedDict
from abc import ABC, abstractmethod
import collections
import threading
import multiprocessing
import time
import logging
import sys

from sdcm.log import SDCMAdapter
from sdcm.sct_events import raise_event_on_failure
from sdcm.utils.decorators import retrying


class BaseService(ABC):
    """
    Base service abstract
    """

    stop_priority: int = 50
    tags: List[str] = []
    _time_to_wait_till_stopped = 2
    _interval = 5
    _termination_event: Union[threading.Event, multiprocessing.Event]
    _log: Union[logging.Logger, SDCMAdapter]
    _dependants: List['BaseService']

    def __init__(self):
        self._dependants = []
        self._log = self._init_logging()
        self._register_self()
        self._is_service_started = False

    @property
    def service_name(self):
        return self.__class__.__name__

    @raise_event_on_failure
    def run(self):
        next_delay = self._interval
        self._log = self._init_logging()
        self._service_on_before_start()
        while self._service_can_iterate(next_delay):
            try:
                self._service_on_before_iterate()
            except (SystemExit, KeyboardInterrupt) as ex:
                self._log.error("Stopped by %s", ex.__class__.__name__)
                return
            except Exception as ex:  # pylint: disable=broad-except
                self._log.debug(f"Service is readiness checkup failed. Error details: '{str(ex)}'")
                continue
            try:
                if next_delay := self._service_body() is None:
                    next_delay = self._interval
            except (SystemExit, KeyboardInterrupt) as ex:
                self._log.error("Stopped by %s", ex.__class__.__name__)
                return
            except Exception as ex:  # pylint: disable=broad-except
                self._log.error(f"Failed executing service body due to the error: {str(ex)}")
                next_delay = self._interval

    def _init_logging(self):
        return logging.getLogger(self.service_name)

    def _service_can_iterate(self, delay):
        """
        Service stop spinning when it return False
        """
        return not self._termination_event.wait(delay)

    @abstractmethod
    def _service_body(self) -> Optional[Union[int, float]]:
        """
        Service body, code that is actually doing the service
        """
        pass

    @abstractmethod
    def _service_on_before_iterate(self):
        """
        Handler that is started before start
        """
        pass

    @abstractmethod
    def _service_on_before_start(self):
        """
        Handler that is started before start
        """
        pass

    def _register_self(self):
        """
        Register service, so that it could be found
        """
        for inst in get_from_instance(self):
            if self not in inst._dependants:
                inst._dependants.append(self)

    def _deregister_self(self):
        """
        Remove service from registry
        """
        for inst in get_from_instance(self):
            if self in inst._dependants:
                inst._dependants.remove(self)

    def start(self):
        """
        Start service
        """
        self._log.info(f'Starting')
        self._is_service_started = True

    def stop(self):
        """
        Send stop signal to the service
        """
        self._log.info(f'Send stop signal')
        self._termination_event.set()

    def is_service_alive(self) -> bool:
        """
        Return True if service is alive
        """
        is_alive = self.is_alive()
        if self._is_service_started:
            if not is_alive:
                self._is_service_started = False
                self._log.info('Stopped')
        return is_alive

    def cleanup(self):
        """
        Cleanup service
        """
        self._deregister_self()

    @abstractmethod
    def is_alive(self) -> bool:
        pass

    def kill(self):
        self._log.info(f'Killed')
        self._is_service_started = False

    def wait_till_stopped(self, timeout: Optional[Union[float, int]] = None) -> bool:
        """
        Wait till service get stopped
        :param timeout: time to wait
        :type Optional[Union[float, int]]:
        :return: False if service is alive and True if it is not
        :rtype:
        """
        self._log.info('Waiting for being stopped')
        self.join(timeout)
        if not self.is_service_alive():
            return True
        self._log.info('Timeout reached, service still alive')
        return False

    @abstractmethod
    def join(self, timeout: Optional[Union[float, int]] = None) -> None:
        pass

    @property
    def dependants(self) -> 'ListOfServices':
        """
        Get services that depends on the service
        :return:
        :rtype: ListOfServices
        """
        return ListOfServices(self._dependants)


DETACHED_SERVICES = []


class DetachService(BaseService, ABC):
    def _service_on_before_iterate(self):
        pass

    def _service_on_before_start(self):
        pass

    def _register_self(self):
        if self not in DETACHED_SERVICES:
            DETACHED_SERVICES.append(self)
        super()._register_self()

    def _deregister_self(self):
        if self in DETACHED_SERVICES:
            DETACHED_SERVICES.remove(self)
        super()._deregister_self()


NODE_SERVICE_BINDING = {}


class NodeService(BaseService, ABC):
    def __init__(self, node):
        self._node = node

    def _init_logging(self):
        return SDCMAdapter(self._node.log, extra={"prefix": self.service_name})

    def _service_on_before_iterate(self):
        self._node.wait_ssh_up(verbose=False)

    @retrying(n=10)
    def _service_on_before_start(self):
        self._service_on_before_iterate()

    def _register_self(self):
        if self._node not in NODE_SERVICE_BINDING:
            NODE_SERVICE_BINDING[self._node] = []
        NODE_SERVICE_BINDING[self._node].append(self)
        super()._register_self()

    def _deregister_self(self):
        if self in NODE_SERVICE_BINDING[self._node]:
            NODE_SERVICE_BINDING[self._node].remove(self)
        super()._deregister_self()


CLUSTER_SERVICE_BINDING = {}


class ClusterService(BaseService, ABC):
    def __init__(self, cluster):
        self._cluster = cluster

    def _init_logging(self):
        return SDCMAdapter(self._cluster.log, extra={"prefix": self.service_name})

    def _service_on_before_iterate(self):
        pass

    def _service_on_before_start(self):
        pass

    @abstractmethod
    def _service_body(self) -> Optional[Union[int, float]]:
        pass

    def _register_self(self):
        if self._cluster not in CLUSTER_SERVICE_BINDING:
            CLUSTER_SERVICE_BINDING[self._cluster] = []
        CLUSTER_SERVICE_BINDING[self._cluster].append(self)
        super()._register_self()

    def _deregister_self(self):
        if self in CLUSTER_SERVICE_BINDING[self._cluster]:
            CLUSTER_SERVICE_BINDING[self._cluster].remove(self)
        super()._deregister_self()


class ThreadServiceMixin(BaseService, threading.Thread, ABC):
    def __init__(self):
        BaseService.__init__(self)
        self._termination_event = threading.Event()
        threading.Thread.__init__(self, name=self.service_name, daemon=True)

    def start(self):
        """
        Waits till service is stopped
        """
        BaseService.start(self)
        return threading.Thread.start(self)

    def join(self, timeout=None):
        """
        Waits till service is stopped
        """
        try:
            threading.Thread.join(self, timeout)
        except:
            pass

    def is_alive(self):
        """
        Return True if service is alive
        """
        return threading.Thread.is_alive(self)

    def kill(self):
        """
        There is no way to kill thread, you can only stop it
        :return:
        :rtype:
        """
        self._log.debug('There is no way to kill thread, you can only stop it')


class ProcessServiceMixin(BaseService, multiprocessing.Process, ABC):
    def __init__(self):
        BaseService.__init__(self)
        self._termination_event = multiprocessing.Event()
        multiprocessing.Process.__init__(self, name=self.service_name, daemon=True)

    def start(self):
        """
        Start service
        """
        BaseService.start(self)
        return multiprocessing.Process.start(self)

    def join(self, timeout=None):
        """
        Waits till service is stopped
        """
        try:
            multiprocessing.Process.join(self, timeout)
        except:
            pass

    def is_alive(self) -> bool:
        """
        Return True if service is alive
        """
        return multiprocessing.Process.is_alive(self)

    def kill(self):
        """
        Kills service immediately
        """
        try:
            multiprocessing.Process.kill(self)
        except:
            pass
        BaseService.kill(self)


class DetachThreadService(DetachService, ThreadServiceMixin, ABC):
    """
    Detached service that is sipping as thread
    This is service that is not attached to anything
    """

    def __init__(self):
        DetachService.__init__(self)
        ThreadServiceMixin.__init__(self)


class DetachProcessService(DetachService, ProcessServiceMixin, ABC):
    """
    Detached service that is sipping as process
    This is service that is not attached to anything
    """

    def __init__(self):
        DetachService.__init__(self)
        ProcessServiceMixin.__init__(self)


class NodeThreadService(NodeService, ThreadServiceMixin, ABC):
    """
    Node service that is attached to node that is sipping as thread
    This is service that is attached to any type of node
    """

    def __init__(self, node):
        NodeService.__init__(self, node)
        ThreadServiceMixin.__init__(self)


class NodeProcessService(NodeService, ProcessServiceMixin, ABC):
    """
    Node service that is attached to node that is sipping as process
    This is service that is attached to any type of node
    """

    def __init__(self, node):
        NodeService.__init__(self, node)
        ProcessServiceMixin.__init__(self)


class ClusterThreadService(ClusterService, ThreadServiceMixin, ABC):
    """
    Cluster service that is attached to node that is sipping as thread
    This is service that is attached to any type of cluster
    """

    def __init__(self, cluster):
        ClusterService.__init__(self, cluster)
        ThreadServiceMixin.__init__(self)


class ClusterProcessService(ClusterService, ProcessServiceMixin, ABC):
    """
    Cluster service that is attached to node that is sipping as process
    This is service that is attached to any type of cluster
    """

    def __init__(self, cluster):
        ClusterService.__init__(self, cluster)
        ProcessServiceMixin.__init__(self)


class ListOfServices:
    """
    Interface to list of services, allows group actions and filtering

    """
    _services: List[BaseService]

    def __init__(self, services: List[BaseService]):
        self._services = services.copy()

    def find_by_name(self, name: str) -> 'ListOfServices':
        return self.__class__([service for service in self._services if service.service_name == name])

    def find_by_tags(self, *tags: str) -> 'ListOfServices':
        output = []
        for service in self._services:
            found = True
            for tag in tags:
                if tag not in service.tags:
                    found = False
                    break
            if found:
                output.append(service)
        return self.__class__(output)

    def exclude_tags(self, *tags: str) -> 'ListOfServices':
        output = []
        for service in self._services:
            found = True
            for tag in tags:
                if tag in service.tags:
                    found = False
                    break
            if found:
                output.append(service)
        return self.__class__(output)

    def get(self, name: str, default=None) -> 'BaseService':
        for service in self._services:
            if service.service_name == name:
                return service
        return default

    def find_no_dependants(self) -> 'ListOfServices':
        return self.__class__([service for service in self._services if not service.dependants])

    def find_no_alive_dependants(self) -> 'ListOfServices':
        return self.__class__([service for service in self._services if not service.dependants.alive()])

    def group_by_stop_priority(self) -> OrderedDict[int, 'ListOfServices']:
        """
        Return OrederedDict of the services groupped by stop_priority
        """
        priorities = []
        for service in self._services:
            if service.stop_priority not in priorities:
                priorities.append(service.stop_priority)
        priorities.sort()
        output = collections.OrderedDict()
        for priority in priorities:
            output[priority] = self.find_by_stop_priority(priority)
        return output

    def find_by_stop_priority(self, lowest: int, highest: Optional[int] = None) -> 'ListOfServices':
        """
        Return list of services that has priority more of equal than lowest and less then highest
        :param lowest: lowest priority
        :type lowest: int
        :param highest: highest priority
        :type highest: Optional[int]
        :return:
        :rtype: ListOfServices
        """
        if highest is None:
            return ListOfServices([service for service in self._services if lowest == service.stop_priority])
        return ListOfServices([service for service in self._services if lowest <= service.stop_priority < highest])

    def stop(self) -> 'ListOfServices':
        """
        Sends stop signal to all services in the list
        :return:
        :rtype:
        """
        for service in self._services:
            service.stop()
        return self

    def kill(self) -> 'ListOfServices':
        """
        Kill all services in the list
        :return:
        :rtype:
        """
        for service in self._services:
            service.kill()
        return self

    def cleanup(self) -> 'ListOfServices':
        """
        Kill all services in the list
        :return:
        :rtype:
        """
        for service in self._services:
            service.cleanup()
        return self

    def is_any_alive(self) -> bool:
        """
        Return True if any of services in the list is alive
        :return:
        :rtype: bool
        """
        for service in self._services:
            if service.is_service_alive():
                return True
        return False

    def alive(self) -> 'ListOfServices':
        """
        Return list of services that are alive
        :return:
        :rtype:
        """
        return ListOfServices([service for service in self._services if service.is_service_alive()])

    def not_alive(self) -> 'ListOfServices':
        """
        Return list of services that are not alive
        :return:
        :rtype:
        """
        return ListOfServices([service for service in self._services if not service.is_service_alive()])

    def names(self) -> List[str]:
        return [service.service_name for service in self._services]

    def wait_till_stopped(self, timeout: Optional[Union[float, int]] = None) -> 'ListOfServices':
        """
        Wait till all services in the list get stopped
        :param timeout: time to wait
        :type Optional[Union[float, int]]:
        :return: True if none of the services is alive and False otherwise
        :rtype:
        """
        if timeout is None:
            while self.is_any_alive():
                time.sleep(0.1)
        else:
            end_time = time.perf_counter() + timeout
            while self.is_any_alive() and end_time >= time.perf_counter():
                time.sleep(0.1)
        return self

    def gradually_stop(self, timeout: Optional[Union[float, int]] = None) -> 'ListOfServices':
        """
        Group services by priority, go thru groups from 0 tp 100, withing the group it stop services and kill the rest
        """
        grouped_by_priorities = self.group_by_stop_priority()
        if timeout:
            end_time = time.perf_counter() + timeout
        else:
            end_time = sys.maxsize
        for _, services in grouped_by_priorities.items():
            while list_to_stop := services.alive().find_no_alive_dependants():
                time_left = end_time - time.perf_counter()
                if time_left <= 0:
                    return self
                list_to_stop.stop()
                list_to_stop.wait_till_stopped(time_left)
                if list_to_stop.alive():
                    list_to_stop.kill()
                    list_to_stop.wait_till_stopped(1)
            if not list_to_stop and services.alive():
                raise RuntimeError(
                    f'Dependency cycle found between following services: {",".join(services.alive().names())}'
                )
        return self

    def gradually_stop_and_cleanup(self, timeout: Optional[Union[float, int]] = None) -> 'ListOfServices':
        """
        Group services by priority, go thru groups from 0 tp 100, withing the group it stop services and kill the rest
        Cleanup at the end
        """
        try:
            if not self.gradually_stop(timeout).is_any_alive():
                self.cleanup()
                return self
        finally:
            self.not_alive().wait_till_stopped(2).kill().cleanup()
        return self

    def __getitem__(self, item) -> BaseService:
        return self._services.__getitem__(item)

    def __iter__(self) -> Iterator[BaseService]:
        return iter(self._services)

    def append(self, item: BaseService) -> 'ListOfServices':
        self._services.append(item)
        return self

    def extend(self, list_of_items: Union[List[BaseService], 'ListOfServices']) -> 'ListOfServices':
        if isinstance(list_of_items, ListOfServices):
            list_of_items = list_of_items._services
        self._services.extend(list_of_items)
        return self

    def __bool__(self):
        return bool(self._services)

    def __len__(self):
        return self._services.__len__()

    def __str__(self):
        return f'<{self.__class__.__name__}> {self._services.__str__()}'

    def copy(self) -> 'ListOfServices':
        return self.__class__(self._services)

    def remove(self, item: BaseService) -> 'ListOfServices':
        self._services.remove(item)
        return self

    def reduce(self, list_of_items: Union[List[BaseService], 'ListOfServices']) -> 'ListOfServices':
        if isinstance(list_of_items, ListOfServices):
            list_of_items = list_of_items._services
        for item in list_of_items:
            self._services.remove(item)
        return self


def get_from_instance(inst: object) -> ListOfServices:
    """
    Return list of services that instance holds as attributes
    :param inst:
    :type inst:
    :return:
    :rtype: ListOfServices
    """
    return ListOfServices([attr for attr in inst.__dict__.values() if isinstance(attr, BaseService)])


def get_by_instance(inst: object) -> ListOfServices:
    """
    Return list of services attached to the instance
    :param inst:
    :type inst: object
    :return:
    :rtype: ListOfServices
    """
    services = []
    if node_services := NODE_SERVICE_BINDING.get(inst, None):
        services.extend(node_services)
    if cluster_services := CLUSTER_SERVICE_BINDING.get(inst, None):
        services.extend(cluster_services)
    return ListOfServices(services)


def get_from_context() -> ListOfServices:
    """
    Return list of detached services registered at the moment
    """
    return ListOfServices(DETACHED_SERVICES)


def get_from_all_sources() -> ListOfServices:
    """
    Return list of services of any type registred at the moment
    """
    services = ListOfServices(DETACHED_SERVICES)
    for node_services in NODE_SERVICE_BINDING.values():
        services.extend(node_services)
    for node_services in CLUSTER_SERVICE_BINDING.values():
        services.extend(node_services)
    return services
