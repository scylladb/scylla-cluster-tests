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
# Copyright (c) 2022 ScyllaDB

import ast
import json
import logging
from datetime import datetime
from enum import Enum
from json import JSONDecodeError
from tempfile import NamedTemporaryFile
import time
from typing import Literal

import yaml
from botocore.utils import deep_merge

from sdcm.log import SDCMAdapter
from sdcm import sct_abs_path
from sdcm.utils.common import time_period_str_to_seconds
from sdcm.utils.k8s import HelmValues, get_helm_pool_affinity_values

LOGGER = logging.getLogger(__name__)


class ChaosMeshException(Exception):
    pass


class ChaosMeshExperimentException(ChaosMeshException):

    def __init__(self, msg: str, experiment: "ChaosMeshExperiment"):
        super().__init__(msg)
        self.message = f"{msg}. Search debug log about {experiment.name}"
        LOGGER.debug(experiment.describe())

    def __str__(self):
        return self.message


class ChaosMeshTimeout(ChaosMeshExperimentException):
    pass


class ChaosMesh:
    NAMESPACE = "chaos-mesh"
    VERSION = "2.5.0"
    HELM_SETTINGS = {
        'dashboard': {"create": False},
        'dnsServer': {"create": True}
    }
    ADDITIONAL_CONFIGS = sct_abs_path("sdcm/k8s_configs/chaos-mesh/gke-auth-workaround.yaml")

    def __init__(self, k8s_cluster: "sdcm.cluster_k8s.KubernetesCluster"):  # noqa: F821
        self._k8s_cluster = k8s_cluster
        self.log = SDCMAdapter(LOGGER, extra={'prefix': k8s_cluster.region_name})
        self.initialized = False

    def initialize(self) -> None:
        """Installs chaos-mesh on k8s cluster and prepares for future k8s chaos testing."""
        if self._k8s_cluster.kubectl(f"get ns {self.NAMESPACE}", ignore_status=True).ok:
            self.initialized = True
            self.log.info("Chaos Mesh is already installed. Skipping installation.")
            return
        self.log.info(
            "Installing chaos-mesh on %s k8s cluster...", self._k8s_cluster.k8s_scylla_cluster_name)
        self._k8s_cluster.helm("repo add chaos-mesh https://charts.chaos-mesh.org")
        self._k8s_cluster.helm('repo update')
        self._k8s_cluster.create_namespace(self.NAMESPACE)
        aux_node_pool_affinity = get_helm_pool_affinity_values(
            self._k8s_cluster.POOL_LABEL_NAME, self._k8s_cluster.AUXILIARY_POOL_NAME)
        scylla_node_pool_affinity = get_helm_pool_affinity_values(
            self._k8s_cluster.POOL_LABEL_NAME, self._k8s_cluster.SCYLLA_POOL_NAME)
        runtime = self._k8s_cluster.kubectl(
            "get nodes -o jsonpath='{.items[0].status.nodeInfo.containerRuntimeVersion}'").stdout
        runtime_settings = {
            "runtime": "containerd", "socketPath": "/run/containerd/containerd.sock"} if runtime.startswith("containerd") else {}
        tolerations = {'tolerations': [{'key': 'role', 'operator': 'Equal',
                                        'value': 'scylla-clusters', 'effect': 'NoSchedule'}]}
        chaos_daemon_settings = scylla_node_pool_affinity | runtime_settings | tolerations
        self._k8s_cluster.helm_install(
            target_chart_name="chaos-mesh",
            source_chart_name="chaos-mesh/chaos-mesh",
            version=self.VERSION,
            use_devel=False,
            namespace=self.NAMESPACE,
            values=HelmValues(self.HELM_SETTINGS | {
                "chaosDaemon": chaos_daemon_settings,
                "controllerManager": aux_node_pool_affinity,
                "dnsServer": aux_node_pool_affinity
            }),
            atomic=True,
            timeout="30m"
        )
        self.log.info(
            "chaos-mesh installed successfully on %s k8s cluster.",
            self._k8s_cluster.k8s_scylla_cluster_name)
        self.initialized = True

        # NOTE: following is needed to pass through the GKE's admission controller
        if self.ADDITIONAL_CONFIGS:
            self._k8s_cluster.apply_file(self.ADDITIONAL_CONFIGS)


class ExperimentStatus(Enum):
    STARTING = 0
    RUNNING = 1
    PAUSED = 2
    FINISHED = 3
    ERROR = 4
    UNKNOWN = 5


class ChaosMeshExperiment:
    """Base class for all chaos-mesh experiments."""
    API_VERSION = "chaos-mesh.org/v1alpha1"
    CHAOS_KIND = ""  # need to override it in child classes

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", name: str, timeout: int = 0):  # noqa: F821
        self._k8s_cluster = pod.k8s_cluster
        self._name = name
        self._namespace = pod.parent_cluster.namespace
        self._experiment = {
            "apiVersion": self.API_VERSION,
            "kind": self.CHAOS_KIND,
            "metadata": {
                "name": self._name,
                "namespace": self._namespace
            },
            "spec": {
                "mode": "one",
                "selector": {
                    "labelSelectors": {
                        "statefulset.kubernetes.io/pod-name": pod.name
                    }
                }
            }
        }
        self._timeout: int = timeout
        self._end_time: int = 0
        self.log = SDCMAdapter(LOGGER, extra={'prefix': self._k8s_cluster.region_name})

    @property
    def name(self):
        return self._name

    def start(self):
        """Starts experiment. Does not wait for finish."""
        self.log.debug("Starting a %s experiment %s", self.CHAOS_KIND, self._name)
        assert self._k8s_cluster, "K8s cluster hasn't been configured for this experiment."
        with NamedTemporaryFile(suffix=".yaml", mode="w") as experiment_config_file:
            yaml.dump(self._experiment, experiment_config_file)
            experiment_config_file.flush()
            self._k8s_cluster.apply_file(experiment_config_file.name)
        self.log.info("'%s' experiment '%s' has started", self.CHAOS_KIND, self._name)
        self._end_time = time.time() + self._timeout

    def get_status(self) -> ExperimentStatus:  # noqa: PLR0911
        """Gets status of chaos-mesh experiment."""
        result = self._k8s_cluster.kubectl(
            f"get {self.CHAOS_KIND} {self._name} -n {self._namespace} -o jsonpath='{{.status.conditions}}'", verbose=False)
        try:
            condition = {cond["type"]: ast.literal_eval(cond["status"]) for cond in json.loads(result.stdout)}
        except JSONDecodeError:
            # it may happen shortly after startup when command returns empty result
            return ExperimentStatus.UNKNOWN
        if not condition["Selected"] and condition["Paused"]:
            return ExperimentStatus.ERROR
        if not condition["Selected"] and not condition["Paused"] and condition["AllRecovered"] and not condition["AllInjected"]:
            return ExperimentStatus.ERROR
        if not condition["Selected"] and not condition["Paused"]:
            return ExperimentStatus.STARTING
        if condition["Selected"] and not condition["Paused"] and not condition["AllRecovered"] and condition["AllInjected"]:
            return ExperimentStatus.RUNNING
        if condition["AllRecovered"] and condition["Paused"]:
            return ExperimentStatus.PAUSED
        if condition["AllRecovered"] and not condition["Paused"] and condition["Selected"]:
            return ExperimentStatus.FINISHED
        self.log.warning("Unknown experiment status: %s", condition)
        return ExperimentStatus.UNKNOWN

    def wait_until_finished(self):
        """Waits given timeout seconds for experiment to finish.

        In case of experiment status being an error or timeout occurred, raises an exception."""
        self.log.debug("waiting until '%s' experiment ends...", self._name)
        assert self._end_time, "Experiment was not started. Use 'start()' method before waiting."
        while time.time() < self._end_time:
            status = self.get_status()
            if status == ExperimentStatus.FINISHED:
                self.log.debug("'%s' experiment ended.", self._name)
                return
            elif status == ExperimentStatus.ERROR:
                raise ChaosMeshExperimentException(msg="Experiment status error", experiment=self)
            time.sleep(2)
        raise ChaosMeshTimeout(msg="Timeout when waiting for ChaosMesh experiment to complete.", experiment=self)

    def describe(self):
        """Runs kubectl describe experiment resource"""
        return self._k8s_cluster.kubectl(f"describe {self.CHAOS_KIND} {self._name} -n {self._namespace}").stdout


class PodFailureExperiment(ChaosMeshExperiment):
    """
    This experiment works by replacing container image with dummy image.
    Then it waits for specified duration and rolls back image config.
    """
    CHAOS_KIND = "PodChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str):  # noqa: F821
        """Injects fault into a specified Pod to make the Pod unavailable for a period of time.

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long pod will be unavailable. str type in k8s notation. E.g. 10s, 5m
        """
        # timeout based on duration + 10 seconds margin
        timeout = time_period_str_to_seconds(duration) + 10
        super().__init__(
            pod=pod, name=f"pod-failure-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "pod-failure",
                "duration": duration,
            }
        })


class MemoryStressExperiment(ChaosMeshExperiment):
    """
    This experiment uses memStress https://github.com/chaos-mesh/memStress for stressing memory in provided scylla pod.
    """
    CHAOS_KIND = "StressChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str,  # noqa: F821
                 workers: int, size: str, time_to_reach: str | None = None):
        """Stresses memory on scylla pod using https://github.com/chaos-mesh/memStress

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long stress will be applied. str type in k8s notation. E.g. 10s, 5m
            :param int workers: Specifies the number of threads that apply memory stress E.g. 4
            :param str size: Specifies the memory size to be occupied or a percentage of the total memory size per worker. E.g.: 256MB / 25%
            :param str time_to_reach: time to reach the size of allocated memory (default "0s")
        """
        # timeout based on duration + 30 seconds margin
        timeout = time_period_str_to_seconds(duration) + 30
        super().__init__(
            pod=pod, name=f"memory-stress-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "duration": duration,
                "containerNames": ["scylla"],
                "stressors": {
                    "memory": {
                        "workers": workers,
                        "size": size,
                    }
                }
            }
        })
        if time_to_reach:
            self._experiment["spec"]["stressors"]["memory"]["options"] = ["-time", time_to_reach]


class DiskError(Enum):
    """
    Common disk error numbers, for more see:
    https://raw.githubusercontent.com/torvalds/linux/master/include/uapi/asm-generic/errno-base.h
    """
    OPERATION_NOT_PERMITTED = 1
    NO_SUCH_FILE_OR_DIRECTORY = 2
    IO_ERROR = 5
    NO_DEVICE_OR_ADDRESS = 6
    OUT_OF_MEMORY = 12
    DEVICE_OR_RESOURCE_BUSY = 16
    FILE_EXISTS = 17
    NOT_A_DIRECTORY = 20
    INVALID_ARGUMENT = 22
    TOO_MANY_OPEN_FILES = 24
    NO_SPACE_LEFT_ON_DEVICE = 28


# for more disk methods refer to https://docs.rs/fuser/0.7.0/fuser/trait.Filesystem.html
DiskMethod = Literal[
    "lookup", "forget", "getattr", "setattr", "readlink", "mknod", "mkdir", "unlink", "rmdir", "symlink", "rename",
    "link", "open", "read", "write", "flush", "release", "fsync", "opendir", "readdir", "releasedir", "fsyncdir",
    "statfs", "setxattr",
    "getxattr", "listxattr", "removexatr", "access", "create", "getlk", "setlk", "bmap"
]


class IOFaultChaosExperiment(ChaosMeshExperiment):
    """
    This experiment uses IOChaos: https://chaos-mesh.org/docs/simulate-io-chaos-on-kubernetes/
    To simulate disk fault.
    """
    CHAOS_KIND = "IOChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str, error: DiskError,  # noqa: F821
                 error_probability: int, methods: list[DiskMethod], volume_path: str, path: str | None = None):
        """Induces disk fault (programatically) using IOChaos: https://chaos-mesh.org/docs/simulate-io-chaos-on-kubernetes/

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long fault will be applied. str type in k8s notation. E.g. 10s, 5m
            :param DiskError error: disk returned error number
            :param int error_probability: Probability of failure per operation, in %
            :param list[DiskMethod] methods: Type of the file system call that requires injecting fault
            :param str volume_path: The mount point of volume in the target container. Must be the root directory of the mount.
            :param str path: The valid range of fault injections, either a wildcard or a single file. By Default all files.
             E.g. /var/lib/scylla/*/
        """
        # timeout based on duration + 30 seconds margin
        timeout = time_period_str_to_seconds(duration) + 30
        super().__init__(
            pod=pod, name=f"io-fault-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "fault",
                "containerNames": ["scylla"],
                "duration": duration,
                "volumePath": volume_path,
                "errno": error.value,
                "percent": error_probability
            }
        })
        if path:
            self._experiment["spec"]["path"] = path
        if methods:
            self._experiment["spec"]["methods"] = methods


class NetworkPacketLossExperiment(ChaosMeshExperiment):
    """
    Simulates packet loss in the network by randomly dropping a specified percentage of packets.
    """
    CHAOS_KIND = "NetworkChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str,  # noqa: F821
                 probability: int = 0, correlation: int = 0):
        """Simulate packet loss fault into a specified Pod for a period of time.

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long it will last. str type in k8s notation. E.g. 10s, 5m
            :param probability: Indicates the probability of packet loss. Range of value: [0, 100]
            :param correlation: Indicates the correlation between the probability of current packet loss
                                and the previous time's packet loss. Range of value: [0, 100]
        """
        # timeout based on duration + 10 seconds margin
        timeout = time_period_str_to_seconds(duration) + 10
        super().__init__(
            pod=pod, name=f"network-packet-loss-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "loss",
                "duration": duration,
                "loss": {
                    "loss": str(probability),
                    "correlation": str(correlation)
                }
            }
        })


class NetworkCorruptExperiment(ChaosMeshExperiment):
    """
    Simulates network packet corruption by altering a specified percentage of packets.
    """
    CHAOS_KIND = "NetworkChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str,  # noqa: F821
                 probability: int = 0, correlation: int = 0):
        """Simulate network corrupt fault into a specified Pod for a period of time.

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long it will last. str type in k8s notation. E.g. 10s, 5m
            :param probability: Indicates the probability of packet corruption. Range of value: [0, 100]
            :param correlation: Indicates the correlation between the probability of current packet corruption
                                and the previous time's packet corruption. Range of value: [0, 100]
        """
        # timeout based on duration + 10 seconds margin
        timeout = time_period_str_to_seconds(duration) + 10
        super().__init__(
            pod=pod, name=f"network-corrupt-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "corrupt",
                "duration": duration,
                "corrupt": {
                    "corrupt": str(probability),
                    "correlation": str(correlation)
                }
            }
        })


class NetworkDelayExperiment(ChaosMeshExperiment):
    """
    Introduces latency in network communication by adding a specified delay to packet transmission.
    """
    CHAOS_KIND = "NetworkChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str, latency: str,  # noqa: F821
                 correlation: int = 0, jitter: str = "0"):
        """Simulate network delay fault into a specified Pod for a period of time.

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long it will last. str type in k8s notation. E.g. 10s, 5m
            :param latency: Indicates the network latency (example: 10ms)
            :param correlation: Indicates the correlation between the current latency and the previous one. Range of value: [0, 100]
            :param jitter: Indicates the range of the network latency (example: 5ms)
        """
        # timeout based on duration + 10 seconds margin
        timeout = time_period_str_to_seconds(duration) + 10
        super().__init__(
            pod=pod, name=f"network-delay-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "delay",
                "duration": duration,
                "delay": {
                    "latency": latency,
                    "correlation": str(correlation),
                    "jitter": jitter,
                }
            }
        })


class NetworkBandwidthLimitExperiment(ChaosMeshExperiment):
    """
    Limits the network bandwidth by throttling the data transfer rate to a specified value.
    """
    CHAOS_KIND = "NetworkChaos"

    def __init__(self, pod: "sdcm.cluster_k8s.BasePodContainer", duration: str,  # noqa: F821
                 rate: str, limit: int, buffer: int):
        """Simulate network bandwidth limit fault into a specified Pod for a period of time.

            :param sdcm.cluster_k8s.BasePodContainer pod: affected scylla pod
            :param str duration: how long it will last. str type in k8s notation. E.g. 10s, 5m
            :param rate: Indicates the rate of bandwidth limit (example: 1mbps)
            :param limit: Indicates the number of bytes waiting in queue (example: 20971520)
            :param buffer: Indicates the maximum number of bytes that can be sent instantaneously (example: 10000)

            for more details refer to https://man7.org/linux/man-pages/man8/tc-tbf.8.html
            The limit is suggested to set to at least 2 * rate * latency,
            where the latency is the estimated latency between source and target,
            and it can be estimated through ping command.
            Too small limit can cause high loss rate and impact the throughput of the tcp connection.
        """
        # timeout based on duration + 10 seconds margin
        timeout = time_period_str_to_seconds(duration) + 10
        super().__init__(
            pod=pod, name=f"network-limit-{pod.name}-{datetime.now().strftime('%d-%H.%M.%S')}", timeout=timeout)
        deep_merge(self._experiment, {
            "spec": {
                "action": "bandwidth",
                "duration": duration,
                "bandwidth": {
                    "rate": rate,
                    "limit": limit,
                    "buffer": buffer,
                }
            }
        })
