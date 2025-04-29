"""This module tests specific nemesis and is heavily dependent on the implementation"""
import logging

import pytest

from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from sdcm.nemesis import CategoricalMonkey
from unit_tests.nemesis.fake_cluster import FakeTester
from unit_tests.nemesis.test_sisyphus import TestNemesisClass

LOGGER = logging.getLogger(__name__)


class FakeCategoricalMonkey(CategoricalMonkey):
    runs = []

    def __new__(cls, *_, **__):
        return object.__new__(cls)

    def __init__(self, tester_obj, termination_event, dist: dict, default_weight: float = 1):
        setattr(CategoricalMonkey, 'disrupt_m1', FakeCategoricalMonkey.disrupt_m1)
        setattr(CategoricalMonkey, 'disrupt_m2', FakeCategoricalMonkey.disrupt_m2)
        super().__init__(tester_obj, termination_event, dist, default_weight=default_weight)

    def disrupt_m1(self):
        self.runs.append(1)

    def disrupt_m2(self):
        self.runs.append(2)

    def get_runs(self):
        return self.runs


@pytest.mark.parametrize(
    "parent, result",
    [
        (LocalMinimalScyllaPodCluster, True),
        (GkeScyllaPodCluster, True),
        (EksScyllaPodCluster, True),
        (ScyllaGCECluster, False),
        (ScyllaAWSCluster, False),
        (ScyllaDockerCluster, False)
    ]
)
def test_is_it_on_kubernetes(parent, result):
    """Tests is_it_on_kubernetes on different Cluster types"""
    class FakeClass(parent):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    params = {'nemesis_interval': 10, 'nemesis_filter_seeds': 1}
    nemesis = TestNemesisClass(FakeTester(db_cluster=FakeClass(),
                                          params=params), None)
    assert nemesis._is_it_on_kubernetes() == result


def test_categorical_monkey():
    tester = FakeTester()

    nemesis = FakeCategoricalMonkey(tester, None, {'m1': 1}, default_weight=0)
    nemesis._random_disrupt()

    nemesis = FakeCategoricalMonkey(tester, None, {'m2': 1}, default_weight=0)
    nemesis._random_disrupt()

    assert nemesis.runs == [1, 2]

    nemesis = FakeCategoricalMonkey(tester, None, {'m1': 1, 'm2': 1}, default_weight=0)
    nemesis._random_disrupt()

    assert nemesis.runs in ([1, 2, 1], [1, 2, 2])
