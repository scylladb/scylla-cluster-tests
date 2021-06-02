from collections import namedtuple
import sdcm.utils.cloud_monitor  # pylint: disable=unused-import # import only to avoid cyclic dependency
from sdcm.nemesis import Nemesis, ChaosMonkey, CategoricalMonkey
from sdcm.cluster_k8s.minikube import LocalMinimalScyllaPodCluster
from sdcm.cluster_k8s.minikube import RemoteMinimalScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)
Cluster = namedtuple("Cluster", ['params'])
FakeTester = namedtuple("FakeTester", ['params', 'loaders', 'monitors', 'db_cluster'],
                        defaults=[PARAMS, {}, {}, Cluster(params=PARAMS)])


class AddRemoveDCMonkey(Nemesis):
    @Nemesis.add_disrupt_method
    def disrupt_add_remove_dc(self):  # pylint: disable=no-self-use
        return 'Worked'

    def disrupt(self):
        self.disrupt_add_remove_dc()


def test_list_nemesis_of_added_disrupt_methods():
    nemesis = ChaosMonkey(FakeTester(), None)
    assert 'disrupt_add_remove_dc' in nemesis.get_list_of_methods_by_flags(disruptive=False)
    assert nemesis.call_random_disrupt_method(disrupt_methods=['disrupt_add_remove_dc']) is None


def test_is_it_on_kubernetes():
    class FakeLocalMinimalScyllaPodCluster(LocalMinimalScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params

    class FakeRemoteMinimalScyllaPodCluster(RemoteMinimalScyllaPodCluster):
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

    class FakeTester:
        def __init__(self, db_cluster):
            self.params = {'nemesis_interval': 10, 'nemesis_filter_seeds': 1}
            db_cluster.params = self.params
            self.db_cluster = db_cluster
            self.loaders = None
            self.monitors = None

    assert Nemesis(FakeTester(FakeLocalMinimalScyllaPodCluster()), None)._is_it_on_kubernetes()
    assert Nemesis(FakeTester(FakeRemoteMinimalScyllaPodCluster()), None)._is_it_on_kubernetes()
    assert Nemesis(FakeTester(FakeGkeScyllaPodCluster()), None)._is_it_on_kubernetes()
    assert Nemesis(FakeTester(FakeEksScyllaPodCluster()), None)._is_it_on_kubernetes()

    assert not Nemesis(FakeTester(FakeScyllaGCECluster()), None)._is_it_on_kubernetes()
    assert not Nemesis(FakeTester(FakeScyllaAWSCluster()), None)._is_it_on_kubernetes()
    assert not Nemesis(FakeTester(FakeScyllaDockerCluster()), None)._is_it_on_kubernetes()

# pylint: disable=protected-access


def test_categorical_monkey():
    runs = []

    def disrupt_m1(_):
        runs.append(1)

    def disrupt_m2(_):
        runs.append(2)
    setattr(CategoricalMonkey, 'disrupt_m1', disrupt_m1)
    setattr(CategoricalMonkey, 'disrupt_m2', disrupt_m2)

    tester = FakeTester()

    nemesis = CategoricalMonkey(tester, None, {'m1': 1}, 0)
    nemesis._random_disrupt()

    nemesis = CategoricalMonkey(tester, None, {'m2': 1}, 0)
    nemesis._random_disrupt()

    assert runs == [1, 2]

    nemesis = CategoricalMonkey(tester, None, {'m1': 1, 'm2': 1}, 0)
    nemesis._random_disrupt()

    assert runs in ([1, 2, 1], [1, 2, 2])
