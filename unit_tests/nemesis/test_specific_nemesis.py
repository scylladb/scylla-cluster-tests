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


class FakeCategorialMonkey(CategoricalMonkey, TestNemesisClass):
    """Override CategoricalMonkey with a new disruption tree"""


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


@pytest.mark.parametrize(
    "dist, output",
    [
        ({"CustomNemesisA": 1}, "called test function a\n"),
        ({"CustomNemesisC": 0.5}, "called test function c\n"),
        ({"CustomNemesisAD": 1, "CustomNemesisA": 0}, "called test function d\n")
    ]
)
def test_categorical_monkey_simple(dist, output, capsys):

    nemesis = FakeCategorialMonkey(FakeTester(), None, dist, default_weight=0)
    method = nemesis.select_next_nemesis()

    method.disrupt()
    captured = capsys.readouterr()
    assert output in captured.out
