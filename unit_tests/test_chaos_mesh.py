#!/usr/bin/env python

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
# Copyright (c) 2021 ScyllaDB

from dataclasses import dataclass, field
from typing import Dict

import yaml
from invoke import Result
import pytest

from sdcm.utils.k8s.chaos_mesh import PodFailureExperiment, ExperimentStatus, ChaosMeshTimeout, ChaosMeshExperimentException, \
    MemoryStressExperiment


@dataclass
class DummyK8sCluster:
    _commands: Dict[str, Result] = field(default_factory=dict)
    region_name: str = 'fake-region-1'

    def apply_file(self, config_path: str):
        """Parses file content to yaml and prints it."""
        with open(config_path, "r", encoding="utf-8") as config_file:
            print(yaml.safe_load(config_file))

    def kubectl(self, command, *args, **kwargs):
        result = [result for cmd, result in self._commands.items() if cmd in command]
        assert result, f"given command: {command} was not registered, failing test." \
                       f" Please register result with 'register_kubectl_result"
        return result[0]

    def register_kubectl_result(self, command: str, command_result: Result):
        self._commands[command] = command_result


@dataclass
class DummyPodCluster:
    namespace: str = "scylla"


@dataclass
class DummyPod:
    parent_cluster: DummyPodCluster
    k8s_cluster: DummyK8sCluster
    name: str


def test_podchaos_experiment_configuration_is_valid(capsys):
    k8s_cluster = DummyK8sCluster()
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")
    experiment = PodFailureExperiment(pod=pod, duration="10s")
    experiment.start()
    captured = capsys.readouterr()
    expected_config = {'apiVersion': 'chaos-mesh.org/v1alpha1',
                       'kind': 'PodChaos',
                       'metadata': {'name': 'pod-failure-dummy-pod-',
                                    'namespace': 'scylla'},
                       'spec': {'action': 'pod-failure',
                                'duration': '10s',
                                'mode': 'one',
                                'selector': {'labelSelectors':
                                             {'statefulset.kubernetes.io/pod-name': 'dummy-pod-1'}
                                             }
                                }
                       }
    actual_config = eval(captured.out)
    # strip the time from name
    actual_config["metadata"]["name"] = actual_config["metadata"]["name"][:-13]
    assert actual_config == expected_config


def test_memorystress_experiment_configuration_is_valid(capsys):
    k8s_cluster = DummyK8sCluster()
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")
    experiment = MemoryStressExperiment(pod=pod, duration="10s", workers=4, size="512MB", time_to_reach="10s")
    experiment.start()
    captured = capsys.readouterr()
    expected_config = {'apiVersion': 'chaos-mesh.org/v1alpha1',
                       'kind': 'StressChaos',
                       'metadata': {'name': 'memory-stress-dummy-pod-', 'namespace': 'scylla'},
                       'spec': {'containerNames': ['scylla'],
                                'duration': '10s',
                                'mode': 'one',
                                'selector': {'labelSelectors': {'statefulset.kubernetes.io/pod-name': 'dummy-pod-1'}},
                                'stressors':
                                    {'memory':
                                     {'options': ['-time', '10s'],
                                      'size': '512MB',
                                      'workers': 4}
                                     }
                                }
                       }
    actual_config = eval(captured.out)
    # strip the time from name
    actual_config["metadata"]["name"] = actual_config["metadata"]["name"][:-13]
    assert actual_config == expected_config


finished_conditions = '[{"status":"True","type":"AllInjected"},' \
                      '{"status":"True","type":"AllRecovered"},' \
                      '{"status":"False","type":"Paused"},' \
                      '{"status":"True","type":"Selected"}]'
running_conditions = '[{"status":"True","type":"AllInjected"},' \
                     '{"status":"False","type":"AllRecovered"},' \
                     '{"status":"False","type":"Paused"},' \
                     '{"status":"True","type":"Selected"}]'
starting_conditions = '[{"status":"False","type":"AllInjected"},' \
                      '{"status":"False","type":"AllRecovered"},' \
                      '{"status":"False","type":"Paused"},' \
                      '{"status":"False","type":"Selected"}]'
paused_conditions = '[{"status":"True","type":"AllInjected"},' \
                    '{"status":"True","type":"AllRecovered"},' \
                    '{"status":"True","type":"Paused"},' \
                    '{"status":"True","type":"Selected"}]'
error_conditions = '[{"status":"True","type":"AllInjected"},' \
                   '{"status":"False","type":"AllRecovered"},' \
                   '{"status":"True","type":"Paused"},' \
                   '{"status":"False","type":"Selected"}]'
error_conditions_2 = '[{"status":"False","type":"AllInjected"},' \
    '{"status":"True","type":"AllRecovered"},' \
    '{"status":"False","type":"Paused"},' \
    '{"status":"False","type":"Selected"}]'
unknown_conditions = ''


@pytest.mark.parametrize("conditions, status", (
    (finished_conditions, ExperimentStatus.FINISHED),
    (running_conditions, ExperimentStatus.RUNNING),
    (starting_conditions, ExperimentStatus.STARTING),
    (paused_conditions, ExperimentStatus.PAUSED),
    (error_conditions, ExperimentStatus.ERROR),
    (error_conditions_2, ExperimentStatus.ERROR),
    (unknown_conditions, ExperimentStatus.UNKNOWN)
))
def test_experiment_statuses_for_podchaos_condidtions(conditions, status):
    k8s_cluster = DummyK8sCluster()
    k8s_cluster.register_kubectl_result("get PodChaos", Result(stdout=conditions))
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")

    experiment = PodFailureExperiment(pod=pod, duration="10s")
    assert experiment.get_status() == status


def test_wait_for_finished_returns_when_status_is_finished():
    k8s_cluster = DummyK8sCluster()
    k8s_cluster.register_kubectl_result("get PodChaos", Result(stdout=finished_conditions))
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")

    experiment = PodFailureExperiment(pod=pod, duration="1s")
    experiment.start()

    experiment.wait_until_finished()


def test_wait_for_finished_raises_exception_when_status_is_error():
    k8s_cluster = DummyK8sCluster()
    k8s_cluster.register_kubectl_result("get PodChaos", Result(error_conditions))
    k8s_cluster.register_kubectl_result("describe PodChaos ", Result(stdout="pod description got from k8s"))
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")

    experiment = PodFailureExperiment(pod=pod, duration="1s")
    experiment.start()

    with pytest.raises(ChaosMeshExperimentException):
        experiment.wait_until_finished()


def test_wait_for_finished_raises_exception_when_timeout_occurs():
    k8s_cluster = DummyK8sCluster()
    k8s_cluster.register_kubectl_result("get PodChaos", Result(running_conditions))
    k8s_cluster.register_kubectl_result("describe PodChaos ", Result(stdout="pod description got from k8s"))
    pod = DummyPod(DummyPodCluster(), k8s_cluster, name="dummy-pod-1")

    experiment = PodFailureExperiment(pod=pod, duration="1s")
    experiment._timeout = 0
    experiment.start()

    with pytest.raises(ChaosMeshTimeout):
        experiment.wait_until_finished()
