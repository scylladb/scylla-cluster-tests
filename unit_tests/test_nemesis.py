import inspect
import logging
from dataclasses import dataclass, field

import pytest

from sdcm.nemesis import Nemesis, CategoricalMonkey, SisyphusMonkey, ToggleGcModeMonkey, COMPLEX_NEMESIS
from sdcm.cluster import BaseScyllaCluster
from sdcm.cluster_k8s.mini_k8s import LocalMinimalScyllaPodCluster
from sdcm.cluster_k8s.gke import GkeScyllaPodCluster
from sdcm.cluster_k8s.eks import EksScyllaPodCluster
from sdcm.cluster_gce import ScyllaGCECluster
from sdcm.cluster_aws import ScyllaAWSCluster
from sdcm.cluster_docker import ScyllaDockerCluster
from sdcm.nemesis_registry import NemesisRegistry
from unit_tests.dummy_remote import LocalLoaderSetDummy
from unit_tests.test_tester import ClusterTesterForTests


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)
LOGGER = logging.getLogger(__name__)


@dataclass
class Node:
    running_nemesis = None
    public_ip_address: str = '127.0.0.1'
    name: str = 'Node1'

    @property
    def scylla_shards(self):
        return 8

    def log_message(self, *args, **kwargs):
        pass


@dataclass
class Cluster:
    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)

    def check_cluster_health(self):
        pass

    @property
    def data_nodes(self):
        return self.nodes

    @property
    def zero_nodes(self):
        return self.nodes

    def log_message(self, *args, **kwargs):
        pass


@dataclass
class FakeTester:
    params: dict = field(default_factory=lambda: PARAMS)
    loaders: LocalLoaderSetDummy = field(default_factory=LocalLoaderSetDummy)
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

    def id(self):
        return 0


class FakeNemesis(Nemesis):
    def __new__(cls, tester_obj, termination_event, *args):
        return object.__new__(cls)

    def disrupt(self):
        pass


class ChaosMonkey(FakeNemesis):

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(tester_obj, termination_event, *args, nemesis_selector=nemesis_selector,
                         nemesis_seed=nemesis_seed, **kwargs)
        self.nemesis_registry = NemesisRegistry(base_class=ChaosMonkey,
                                                excluded_list=COMPLEX_NEMESIS)


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


class AddRemoveDCMonkey(ChaosMonkey):
    @ChaosMonkey.add_disrupt_method
    def disrupt_rnd_method(self):
        print("It Works!")

    def disrupt(self):
        self.disrupt_rnd_method()


@pytest.mark.usefixtures('events')
def test_list_nemesis_of_added_disrupt_methods(capsys):
    with capsys.disabled():
        nemesis = ChaosMonkey(FakeTester(), None)
        assert 'disrupt_rnd_method' in [
            method.__name__ for method in nemesis.nemesis_registry.get_disrupt_methods()]
    nemesis.disruptions_list = nemesis.build_disruptions_by_name(['disrupt_rnd_method'])
    nemesis.call_next_nemesis()
    captured = capsys.readouterr()
    assert "It Works!" in captured.out


def test_is_it_on_kubernetes():
    class FakeLocalMinimalScyllaPodCluster(LocalMinimalScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    class FakeGkeScyllaPodCluster(GkeScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    class FakeEksScyllaPodCluster(EksScyllaPodCluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    class FakeScyllaGCECluster(ScyllaGCECluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    class FakeScyllaAWSCluster(ScyllaAWSCluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    class FakeScyllaDockerCluster(ScyllaDockerCluster):
        def __init__(self, params: dict = None):
            self.params = params
            self.nodes = []

    params = {'nemesis_interval': 10, 'nemesis_filter_seeds': 1}

    assert FakeNemesis(FakeTester(db_cluster=FakeLocalMinimalScyllaPodCluster(),
                       params=params), None)._is_it_on_kubernetes()
    assert FakeNemesis(FakeTester(db_cluster=FakeGkeScyllaPodCluster(), params=params), None)._is_it_on_kubernetes()
    assert FakeNemesis(FakeTester(db_cluster=FakeEksScyllaPodCluster(), params=params), None)._is_it_on_kubernetes()

    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaGCECluster(), params=params), None)._is_it_on_kubernetes()
    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaAWSCluster(), params=params), None)._is_it_on_kubernetes()
    assert not FakeNemesis(FakeTester(db_cluster=FakeScyllaDockerCluster(), params=params), None)._is_it_on_kubernetes()


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
    tester.params["nemesis_selector"] = ''
    sisyphus = SisyphusMonkey(tester, None)

    collected_disrupt_methods_names = {disrupt.__name__ for disrupt in sisyphus.disruptions_list}

    assert 'disrupt_toggle_table_gc_mode' in collected_disrupt_methods_names


class TestSisyphusMonkeyNemesisFilter:

    @pytest.fixture(autouse=True)
    def expected_topology_changes_methods(self):
        return [
            "disrupt_restart_with_resharding",
            "disrupt_nodetool_seed_decommission",
            "disrupt_nodetool_drain",
            "disrupt_nodetool_decommission",
            "disrupt_add_remove_dc",
            "disrupt_grow_shrink_cluster",
            "disrupt_terminate_and_replace_node",
            "disrupt_decommission_streaming_err",
            "disrupt_remove_node_then_add_node",
            "disrupt_bootstrap_streaming_error",
            "disrupt_serial_restart_elected_topology_coordinator",
            "disrupt_refuse_connection_with_block_scylla_ports_on_banned_node",
            "disrupt_refuse_connection_with_send_sigstop_signal_to_scylla_on_banned_node"

        ]

    @pytest.fixture(autouse=True)
    def expected_schema_changes_methods(self):
        return [
            "disrupt_add_drop_column",
            "disrupt_toggle_table_ics",
            "disrupt_toggle_cdc_feature_properties_on_table",
            "disrupt_modify_table",
            "disrupt_toggle_table_gc_mode",
            "disrupt_create_index",
            "disrupt_add_remove_mv",
            "disrupt_toggle_audit_syslog"
        ]

    @pytest.fixture(autouse=True)
    def expected_config_changes_methods(self):
        return [
            "disrupt_modify_table",
            "disrupt_toggle_cdc_feature_properties_on_table",
            "disrupt_hot_reloading_internode_certificate",
            "disrupt_grow_shrink_new_rack",
            "disrupt_restart_with_resharding",
            "disrupt_rolling_config_change_internode_compression",
            "disrupt_rolling_restart_cluster",
            "disrupt_switch_between_password_authenticator_and_saslauthd_authenticator_and_back",
            "disrupt_resetlocalschema",
            "disrupt_toggle_audit_syslog",
            "disrupt_end_of_quota_nemesis",
        ]

    @pytest.fixture(autouse=True)
    def expected_config_and_schema_changes_methods(self):
        return [
            "disrupt_toggle_cdc_feature_properties_on_table",
            "disrupt_toggle_audit_syslog",
        ]

    def test_list_topology_changes_monkey(self, expected_topology_changes_methods):
        tester = FakeTester()
        tester.params["nemesis_selector"] = 'topology_changes'
        sisyphus_nemesis = SisyphusMonkey(tester, None)

        collected_disrupt_methods_names = [disrupt.__name__ for disrupt in sisyphus_nemesis.disruptions_list]

        for disrupt_method in collected_disrupt_methods_names:
            assert disrupt_method in expected_topology_changes_methods, \
                f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_topology_changes_methods=}"

    def test_list_schema_changes_monkey(self, expected_schema_changes_methods):
        tester = FakeTester()
        tester.params["nemesis_selector"] = 'schema_changes'
        sisyphus_nemesis = SisyphusMonkey(tester, None)
        collected_disrupt_methods_names = [disrupt.__name__ for disrupt in sisyphus_nemesis.disruptions_list]

        for disrupt_method in collected_disrupt_methods_names:
            assert disrupt_method in expected_schema_changes_methods, \
                f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_schema_changes_methods=}"

    def test_list_config_changes_monkey(self, expected_config_changes_methods):
        tester = FakeTester()
        tester.params["nemesis_selector"] = 'config_changes'
        sisyphus_nemesis = SisyphusMonkey(tester, None)
        collected_disrupt_methods_names = [disrupt.__name__ for disrupt in sisyphus_nemesis.disruptions_list]

        for disrupt_method in collected_disrupt_methods_names:
            assert disrupt_method in expected_config_changes_methods, \
                f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_config_changes_methods=}"

    def test_list_config_and_schema_changes_monkey(self, expected_config_and_schema_changes_methods):
        tester = FakeTester()
        tester.params["nemesis_selector"] = 'config_changes and schema_changes'
        sisyphus_nemesis = SisyphusMonkey(tester, None, nemesis_selector='config_changes and schema_changes')
        collected_disrupt_methods_names = [disrupt.__name__ for disrupt in sisyphus_nemesis.disruptions_list]

        for disrupt_method in collected_disrupt_methods_names:
            assert disrupt_method in expected_config_and_schema_changes_methods, \
                f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_config_and_schema_changes_methods=}"

    def test_add_sisyphus_with_filter_in_parallel_nemesis_run(self, expected_schema_changes_methods, expected_topology_changes_methods):
        tester = ClusterTesterForTests()
        tester._init_params()
        tester.db_cluster = Cluster(nodes=[Node(), Node()])
        tester.db_cluster.params = tester.params
        tester.params["nemesis_class_name"] = "SisyphusMonkey:1 SisyphusMonkey:2"
        tester.params["nemesis_selector"] = ["topology_changes",
                                             "schema_changes and schema_changes",
                                             "schema_changes and schema_changes"]
        tester.params["nemesis_multiply_factor"] = 1
        nemesises = tester.get_nemesis_class()

        expected_selectors = ["topology_changes",
                              "schema_changes and schema_changes",  "schema_changes and schema_changes"]
        for i, nemesis_settings in enumerate(nemesises):
            assert nemesis_settings['nemesis'] == SisyphusMonkey, \
                f"Wrong instance of nemesis class {nemesis_settings['nemesis']} expected SisyphusMonkey"
            assert nemesis_settings['nemesis_selector'] == expected_selectors[i], \
                f"Wrong nemesis filter selecters {nemesis_settings['nemesis_selector']} expected {expected_selectors[i]}"

        active_nemesis = []
        for nemesis in nemesises:
            sisyphus = nemesis['nemesis'](tester, None, nemesis_selector=nemesis["nemesis_selector"])
            active_nemesis.append(sisyphus)
        expected_methods = [expected_topology_changes_methods,
                            expected_schema_changes_methods, expected_schema_changes_methods]
        LOGGER.warning(expected_methods)
        for i, nem in enumerate(active_nemesis):
            collected_disrupt_methods_names = [disrupt.__name__ for disrupt in nem.disruptions_list]

            for disrupt_method in collected_disrupt_methods_names:
                assert disrupt_method in expected_methods[i], \
                    f"{disrupt_method=} from {collected_disrupt_methods_names=} was not found in {expected_methods[i]=}"
