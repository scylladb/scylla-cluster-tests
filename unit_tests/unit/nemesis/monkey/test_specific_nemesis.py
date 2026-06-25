"""This module tests specific nemesis and is heavily dependent on the implementation"""

import logging
import pytest

from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from unit_tests.unit.nemesis.fake_cluster import FakeTester
from unit_tests.unit.nemesis import TestNemesisClass

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "parent, result",
    [
        (LocalMinimalScyllaPodCluster, True),
        (GkeScyllaPodCluster, True),
        (EksScyllaPodCluster, True),
        (ScyllaGCECluster, False),
        (ScyllaAWSCluster, False),
        (ScyllaDockerCluster, False),
    ],
)
def test_is_it_on_kubernetes(parent, result):
    """Tests is_it_on_kubernetes on different Cluster types"""

    class FakeClass(parent):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    params = {"nemesis_interval": 10, "nemesis_filter_seeds": 1}
    nemesis = TestNemesisClass(FakeTester(db_cluster=FakeClass(), params=params), None)
    assert nemesis._is_it_on_kubernetes() == result
