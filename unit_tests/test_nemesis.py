import inspect
from dataclasses import dataclass, field

import pytest

import sdcm.utils.cloud_monitor  # pylint: disable=unused-import # import only to avoid cyclic dependency
from sdcm.nemesis import Nemesis, CategoricalMonkey, SisyphusMonkey, ToggleGcModeMonkey
from sdcm.cluster import BaseScyllaCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)


@dataclass
class Node:
    running_nemesis = None
    public_ip_address: str = '127.0.0.1'
    name: str = 'Node1'

    @property
    def scylla_shards(self):
        return 8


@dataclass
class Cluster:
    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)

    def check_cluster_health(self):
        pass


@dataclass
class FakeTester:
    params: dict = field(default_factory=lambda: PARAMS)
    loaders: list = field(default_factory=list)
    db_cluster: Cluster | BaseScyllaCluster = field(default_factory=lambda: Cluster(nodes=[Node(), Node()]))
    monitors: list = field(default_factory=list)

    def __post_init__(self):
        self.db_cluster.params = self.params

    def create_stats(self):
        pass

    def update(self, *args, **kwargs):
        pass

    def get_scylla_versions(self):
        pass

    def get_test_details(self):
        pass

    def id(self):  # pylint: disable=invalid-name,no-self-use
        return 0


class FakeNemesis(Nemesis):
    def __new__(cls, tester_obj, termination_event, *args):  # pylint: disable=unused-argument
        return object.__new__(cls)

    def disrupt(self):
        pass


class ChaosMonkey(FakeNemesis):
    ...


class FakeCategoricalMonkey(CategoricalMonkey):
    runs = []

    def __new__(cls, *_, **__):
        return object.__new__(cls)

    def __init__(self, tester_obj, termination_event, dist: dict, default_weight: float = 1):
        setattr(CategoricalMonkey, 'disrupt_m1', self.disrupt_m1)
        setattr(CategoricalMonkey, 'disrupt_m2', self.disrupt_m2)
        super().__init__(tester_obj, termination_event, dist, default_weight)

    def disrupt_m1(self):
        self.runs.append(1)

    def disrupt_m2(self):
        self.runs.append(2)

    def get_runs(self):
        return self.runs


class AddRemoveDCMonkey(FakeNemesis):
    @Nemesis.add_disrupt_method
    def disrupt_add_remove_dc(self):  # pylint: disable=no-self-use
        return 'Worked'

    def disrupt(self):
        self.disrupt_add_remove_dc()


@pytest.mark.usefixtures('events')
def test_list_nemesis_of_added_disrupt_methods():
    nemesis = ChaosMonkey(FakeTester(), None)
    assert 'disrupt_add_remove_dc' in nemesis.get_list_of_methods_by_flags(disruptive=False)
    assert nemesis.call_random_disrupt_method(disrupt_methods=['disrupt_add_remove_dc']) is None


# pylint: disable=super-init-not-called,too-many-ancestors
def test_is_it_on_kubernetes():
    class FakeLocalMinimalScyllaPodCluster(LocalMinimalScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeGkeScyllaPodCluster(GkeScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeEksScyllaPodCluster(EksScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeScyllaGCECluster(ScyllaGCECluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeScyllaAWSCluster(ScyllaAWSCluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeScyllaDockerCluster(ScyllaDockerCluster):
        def __init__(self, params: dict = None):
            self.params = params

    params = {'nemesis_interval': 10, 'nemesis_filter_seeds': 1}

    # pylint: disable=protected-access
    assert FakeNemesis(FakeTester(db_cluster=FakeLocalMinimalScyllaPodCluster(),
                       params=params), None)._is_it_on_kubernetes()
    assert FakeNemesis(FakeTester(db_cluster=FakeGkeScyllaPodCluster(), params=params), None)._is_it_on_kubernetes()
    assert FakeNemesis(FakeTester(db_cluster=FakeEksScyllaPodCluster(), params=params), None)._is_it_on_kubernetes()

    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaGCECluster(), params=params), None)._is_it_on_kubernetes()
    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaAWSCluster(), params=params), None)._is_it_on_kubernetes()
    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaDockerCluster(), params=params), None)._is_it_on_kubernetes()


# pylint: disable=protected-access
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


def test_list_topology_changes_monkey():
    expected_disrupt_method_names = [
        "disrupt_restart_with_resharding",
        "disrupt_nodetool_seed_decommission",
        "disrupt_nodetool_drain",
        "disrupt_nodetool_decommission",
        "disrupt_add_remove_dc",
        "disrupt_grow_shrink_cluster",
        "disrupt_terminate_and_replace_node",
        "disrupt_decommission_streaming_err",
        "disrupt_remove_node_then_add_node",
    ]
    tester = FakeTester()
    tester.params["nemesis_selector"] = ['topology_changes']
    sisphus = SisyphusMonkey(FakeTester(), None)

    collected_disrupt_methods_names = [disrupt.__name__ for disrupt in sisphus.disruptions_list]

    for disrupt_method in collected_disrupt_methods_names:
        assert disrupt_method in expected_disrupt_method_names, \
            f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_disrupt_method_names=}"


def test_disabled_monkey():

    ToggleGcModeMonkey.disabled = True

    tester = FakeTester()

    all_disrupt_methods = {attr[1].__name__ for attr in inspect.getmembers(Nemesis) if
                           attr[0].startswith('disrupt_') and
                           callable(attr[1])}
    tester.params["nemesis_exclude_disabled"] = True
    tester.params["nemesis_selector"] = []
    sisyphus = SisyphusMonkey(tester, None)

    collected_disrupt_methods_names = {disrupt.__name__ for disrupt in sisyphus.disruptions_list}
    # Note: this test will fail and have to be adjusted once additional 'disabled' nemeses added.
    assert collected_disrupt_methods_names == all_disrupt_methods - {'disrupt_toggle_table_gc_mode'}


def test_use_disabled_monkey():

    ToggleGcModeMonkey.disabled = True

    tester = FakeTester()

    tester.params["nemesis_exclude_disabled"] = False
    tester.params["nemesis_selector"] = []
    sisyphus = SisyphusMonkey(tester, None)

    collected_disrupt_methods_names = {disrupt.__name__ for disrupt in sisyphus.disruptions_list}

    assert 'disrupt_toggle_table_gc_mode' in collected_disrupt_methods_names
