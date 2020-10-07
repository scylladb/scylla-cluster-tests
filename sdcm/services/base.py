# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import collections
import threading
import multiprocessing
import time
import logging
import sys
from typing import Union, Optional, Set, List, Iterator, Dict, Tuple, Iterable
from abc import ABC, abstractmethod

from sdcm.log import SDCMAdapter
from sdcm.sct_events import raise_event_on_failure
from sdcm.utils.decorators import retrying


class RegistryList(list):
    def remove(self, value: 'BaseService') -> None:
        if value in self:
            super().remove(value)

    def append(self, value: 'BaseService') -> None:
        if value not in self:
            super().append(value)


class Registry(dict):
    """
    Registry to keep services tracked
    """

    # TBD: Make it see threads from subprocesses

    def __init__(self, *args, **kwargs):
        self._lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def __enter__(self):
        self._lock.acquire(blocking=True)

    def __getitem__(self, item):
        if item not in self:
            value = RegistryList()
            super().__setitem__(item, value)
            return value
        return super().__getitem__(item)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()


REGISTRY = Registry()


class BaseService(ABC):
    """
    Service abstract
    """

    stop_priority: int = 50  # A number from 0 to 100. Services with low priority will be stopped first
    _default_tags: List[str] = []  # Set of string identifier that could be used to find service
    _interval = 5  # Time in seconds. Determine how ofter service_body to be ran
    _termination_event: Union[threading.Event, multiprocessing.Event]
    _log: Union[logging.Logger, SDCMAdapter]
    _dependants: 'ListOfServices'

    def __init__(self):
        self._dependants = ListOfServices()
        self._instance_tag = []
        self._log = self._init_logging()
        self._register_self()
        self._is_service_started = False

    @property
    def service_name(self):
        return self.__class__.__name__

    @raise_event_on_failure
    def run(self):
        try:
            next_delay = self._interval
            self._log = self._init_logging()
            # Service that is ran in process need separate file descriptor, i.e. logger instance
            self._service_on_before_start()
            while self._service_can_iterate(next_delay):
                try:
                    self._service_on_before_iterate()
                except (SystemExit, KeyboardInterrupt):
                    raise
                except Exception as ex:
                    self._log.debug("Service is readiness checkup failed. Error details: \n%s", ex)
                    continue
                try:
                    next_delay = self._service_body()
                    if next_delay is None:
                        next_delay = self._interval
                except (SystemExit, KeyboardInterrupt):
                    raise
                except Exception as ex:
                    self._log.error(f"Failed executing service body due to the error: \n%s", ex)
                    next_delay = self._interval
        except (SystemExit, KeyboardInterrupt) as ex:
            self._log.error("Stopped by %s", ex.__class__.__name__)

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
        for inst in get_services_from_instance_attributes(self):
            if self not in inst._dependants:
                inst._dependants.append(self)

    def _unregister_self(self):
        for inst in get_services_from_instance_attributes(self):
            if self in inst._dependants:
                inst._dependants.remove(self)

    def start(self):
        self._log.info(f'Starting')
        self._is_service_started = True

    def stop(self):
        self._log.info(f'Send stop signal')
        self._termination_event.set()

    @property
    def is_service_alive(self) -> bool:
        is_alive = self.is_alive()
        if self._is_service_started:
            if not is_alive:
                self._is_service_started = False
                self._log.info('Stopped')
        return is_alive

    def cleanup(self):
        self._unregister_self()

    @abstractmethod
    def is_alive(self) -> bool:
        pass

    def kill(self):
        self._log.info(f'Killed')
        self._is_service_started = False

    def wait_till_stopped(self, timeout: Optional[Union[float, int]] = None) -> bool:
        self._log.info('Waiting for being stopped')
        self.join(timeout)
        if not self.is_service_alive:
            return True
        self._log.info('Timeout reached, service still alive')
        return False

    @abstractmethod
    def join(self, timeout: Optional[Union[float, int]] = None) -> None:
        pass

    @property
    def dependants(self) -> 'ListOfServices':
        return self._dependants

    @property
    def tags(self) -> Set[str]:
        return set(self._default_tags + self._instance_tag)

    @tags.setter
    def tags(self, tags: Union[List[str], Set[str], Tuple[str]]):
        if isinstance(tags, (tuple, list)):
            tags = set(tags)
        elif not isinstance(tags, set):
            raise ValueError('Expect list, tuple or set')
        self._instance_tag = list(tags - set(self._default_tags))


class DetachService(BaseService, ABC):
    def _service_on_before_iterate(self):
        pass

    def _service_on_before_start(self):
        pass

    def _register_self(self):
        with REGISTRY:
            DETACHED_SERVICES = REGISTRY['__DETACHED__']
            if self not in DETACHED_SERVICES:
                DETACHED_SERVICES.append(self)
        super()._register_self()

    def _unregister_self(self):
        with REGISTRY:
            REGISTRY['__DETACHED__'].remove(self)
        super()._unregister_self()


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
        with REGISTRY:
            REGISTRY[self._node].append(self)
        super()._register_self()

    def _unregister_self(self):
        with REGISTRY:
            REGISTRY[self._node].remove(self)
        super()._unregister_self()


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
        with REGISTRY:
            REGISTRY[self._cluster].append(self)
        super()._register_self()

    def _unregister_self(self):
        with REGISTRY:
            REGISTRY[self._cluster].remove(self)
        super()._unregister_self()


class ThreadServiceMixin(BaseService, threading.Thread, ABC):
    def __init__(self):
        BaseService.__init__(self)
        self._termination_event = threading.Event()
        threading.Thread.__init__(self, name=self.service_name, daemon=True)

    def start(self):
        BaseService.start(self)
        return threading.Thread.start(self)

    def join(self, timeout=None):
        try:
            threading.Thread.join(self, timeout)
        except Exception:
            pass

    def is_alive(self):
        return threading.Thread.is_alive(self)

    def kill(self):
        self._log.debug('There is no way to kill thread, you can only stop it')


class ProcessServiceMixin(BaseService, multiprocessing.Process, ABC):
    def __init__(self):
        BaseService.__init__(self)
        self._termination_event = multiprocessing.Event()
        multiprocessing.Process.__init__(self, name=self.service_name, daemon=True)

    def start(self):
        BaseService.start(self)
        return multiprocessing.Process.start(self)

    def join(self, timeout=None):
        try:
            multiprocessing.Process.join(self, timeout)
        except Exception:
            pass

    def is_alive(self) -> bool:
        return multiprocessing.Process.is_alive(self)

    def kill(self):
        try:
            multiprocessing.Process.kill(self)
        except Exception:
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
    Interface to list of services, allows group actions and filtering in chaining manner
    """
    _services: List[BaseService]

    def __init__(self, services: Optional[Iterable[BaseService]] = None):
        if services:
            self._services = [service for service in services]
        else:
            self._services = []

    def find_by_name(self, name: str) -> 'ListOfServices':
        return type(self)([service for service in self._services if service.service_name == name])

    def find_by_tags(self, *tags: str) -> 'ListOfServices':
        if not tags:
            return self
        tags = set(tags)
        return type(self)(service for service in self._services if tags <= service.tags)

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
        return type(self)(output)

    def get(self, name: str, default=None) -> 'BaseService':
        for service in self._services:
            if service.service_name == name:
                return service
        return default

    def find_no_dependants(self) -> 'ListOfServices':
        return type(self)([service for service in self._services if not service.dependants])

    def find_no_alive_dependants(self) -> 'ListOfServices':
        return type(self)([service for service in self._services if not service.dependants.is_any_alive()])

    def group_by_stop_priority(self) -> Dict[int, 'ListOfServices']:
        """
        Group services by stop_priority, resulted dictionary is sorted from low to high priority
        """
        priorities = []
        for service in self._services:
            if service.stop_priority not in priorities:
                priorities.append(service.stop_priority)
        priorities.sort()
        output = dict()
        for priority in priorities:
            output[priority] = self.find_by_stop_priority(priority, priority + 1)
        return output

    def find_by_stop_priority(self, lowest: Optional[int] = None, highest: Optional[int] = None) -> 'ListOfServices':
        """
        Return list of services that have stop_priority more or equal to lowest and lower than highest
        default for highest is 101
        default for lowest is 0
        """
        if highest is None:
            highest = 101
        if lowest is None:
            lowest = 0
        return ListOfServices([service for service in self._services if lowest <= service.stop_priority < highest])

    def stop(self) -> 'ListOfServices':
        for service in self._services:
            service.stop()
        return self

    def kill(self) -> 'ListOfServices':
        for service in self._services:
            service.kill()
        return self

    def cleanup(self) -> 'ListOfServices':
        for service in self._services:
            service.cleanup()
        return self

    def is_any_alive(self) -> bool:
        return any(service.is_service_alive for service in self._services)

    def alive(self) -> 'ListOfServices':
        return type(self)([service for service in self._services if service.is_service_alive])

    def not_alive(self) -> 'ListOfServices':
        return type(self)([service for service in self._services if not service.is_service_alive])

    def names(self) -> List[str]:
        return [service.service_name for service in self._services]

    def wait_till_stopped(self, timeout: Optional[Union[float, int]] = None) -> 'ListOfServices':
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
                list_to_stop.stop().wait_till_stopped(time_left)
                if list_to_stop.alive():
                    list_to_stop.kill().wait_till_stopped(1)
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
        return f'<{type(self).__name__}> {self._services.__str__()}'

    def copy(self) -> 'ListOfServices':
        return type(self)(self._services)

    def remove(self, item: BaseService) -> 'ListOfServices':
        self._services.remove(item)
        return self

    def reduce(self, list_of_items: Union[List[BaseService], 'ListOfServices']) -> 'ListOfServices':
        if isinstance(list_of_items, ListOfServices):
            list_of_items = list_of_items._services
        for item in list_of_items:
            self._services.remove(item)
        return self


def get_services_from_instance_attributes(inst: object) -> ListOfServices:
    return ListOfServices([attr for attr in inst.__dict__.values() if isinstance(attr, BaseService)])


def find_serviecs_by_instance(inst: object) -> ListOfServices:
    return ListOfServices(list(REGISTRY.get(inst, [])))


def find_detached_services() -> ListOfServices:
    return ListOfServices(list(REGISTRY['__DETACHED__']))


def find_services_from_all_sources() -> ListOfServices:
    services = []
    for reg_services in REGISTRY.values():
        services.extend(reg_services)
    return ListOfServices(services)
