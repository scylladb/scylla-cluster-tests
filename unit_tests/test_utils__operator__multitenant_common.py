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

import pytest

from sdcm import sct_config
from sdcm.test_config import TestConfig
from sdcm.utils.k8s_operator.multitenant_common import get_tenants


class FakeTestBase:
    def _init_stats(self):
        pass

    def create_test_stats(self):
        pass

    def get_test_start_time(self):
        pass

    def _init_test_timeout_thread(self):
        pass

    def _init_test_duration(self):
        pass

    def _init_data_validation(self):
        pass


class FakeLongevityTest(FakeTestBase):
    pass


class FakePerformanceTest(FakeTestBase):
    pass


class FakeDbCluster:
    def __init__(self, fake_index):
        self.fake_index = fake_index
        self.k8s_clusters = [type("FakeK8SCluster", (), {"tenants_number": 2})]
        self.params = {}


class FakeMultitenantTestBase:
    def __init__(self):
        self.test_config = TestConfig()
        self.test_config.set_test_id_only("fake-test-id")
        self.params = sct_config.SCTConfiguration()
        self.params.verify_configuration()
        self.db_clusters_multitenant = [FakeDbCluster(1), FakeDbCluster(2)]
        self.db_cluster = self.db_clusters_multitenant[0]
        self.loaders_multitenant = ["fake_loaders_1", "fake_loaders_2"]
        self.monitors_multitenant = ["fake_monitoring_1", "fake_monitoring_2"]
        self.prometheus_db_multitenant = ["fake_prometheus_1", "fake_prometheus_2"]


class FakeMultitenantLongevityTest(FakeMultitenantTestBase, FakeLongevityTest):
    pass


class FakeMultitenantPerformanceTest(FakeMultitenantTestBase, FakePerformanceTest):
    pass


class TestUtilsOperatorMultitenantCommon:
    @pytest.fixture(autouse=True)
    def fixture_env(self, monkeypatch):
        self.monkeypatch = monkeypatch
        self.setup_default_env()
        yield
        self.setup_default_env()

    def setup_default_env(self):
        self.monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
        self.monkeypatch.setenv("SCT_CLUSTER_BACKEND", "k8s-eks")

    def _multitenant_class_with_shared_options(self, klass):
        self.monkeypatch.setenv(
            "SCT_CONFIG_FILES", "unit_tests/test_data/test_config/multitenant/shared_option_values.yaml"
        )

        fake_multitenant_test_class_instance = klass()
        tenants = get_tenants(fake_multitenant_test_class_instance)

        assert len(tenants) == 2
        for i, tenant in enumerate(tenants):
            assert tenant.__class__.__name__ == f"TenantFor{klass.__name__.replace('Multitenant', '')}-{i + 1}"
            if "Longevity" in klass.__name__:
                tenant_id = f"{tenant.test_config.test_id()}-{tenant._test_index}"
            else:
                tenant_id = f"id--{tenant._test_index}"
            assert tenant.id() == tenant_id
            assert tenant.params.get("prepare_write_cmd") == [
                f"fake__prepare_write_cmd__all_tenants__part{j}" for j in range(1, 3)
            ]
            # NOTE: StringOrList type always converts single strings to a list with one element
            assert tenant.params.get("prepare_verify_cmd") == ["fake__prepare_verify_cmd__all_tenants__single_str"]
            for cmd_name in ("nemesis_selector", "stress_cmd_m", "stress_cmd", "stress_cmd_r"):
                assert tenant.params.get(cmd_name) == [f"fake__{cmd_name}__all_tenants__part{j}" for j in range(1, 3)]
            for cmd_name in ("stress_read_cmd", "stress_cmd_w"):
                # NOTE: StringOrList type always converts single strings to a list with one element
                assert tenant.params.get(cmd_name) == [f"fake__{cmd_name}__all_tenants__single_str"]
            # NOTE: StringOrList type always converts single strings to a list with one element
            assert tenant.params.get("nemesis_class_name") == ["FakeNemesisClassName"]
            assert tenant.params.get("nemesis_interval") == 5
            assert tenant.params.get("nemesis_sequence_sleep_between_ops") == 3
            assert tenant.params.get("nemesis_during_prepare") is False
            assert tenant.params.get("nemesis_seed") == 13
            assert tenant.params.get("nemesis_add_node_cnt") == 1
            assert tenant.params.get("space_node_threshold") == 13531
            assert tenant.params.get("nemesis_filter_seeds") is False
            assert tenant.params.get("nemesis_exclude_disabled") is True
            assert tenant.params.get("nemesis_multiply_factor") == 5
            assert tenant.params.get("round_robin") is True

    def test_multitenant_longevity_class_with_shared_options(self):
        self._multitenant_class_with_shared_options(FakeMultitenantLongevityTest)

    def test_multitenant_performance_class_with_shared_options(self):
        self._multitenant_class_with_shared_options(FakeMultitenantPerformanceTest)

    def _multitenant_class_with_unique_options(self, klass):
        self.monkeypatch.setenv(
            "SCT_CONFIG_FILES", ("unit_tests/test_data/test_config/multitenant/unique_option_values.yaml")
        )

        fake_multitenant_longevity_test_class_instance = klass()
        tenants = get_tenants(fake_multitenant_longevity_test_class_instance)

        assert len(tenants) == 2
        for i, tenant in enumerate(tenants):
            assert tenant.get_str_index() == f"k8s-{'perf' if 'Perf' in klass.__name__ else 'longevity'}-2-tenants"
            assert tenant.__class__.__name__ == f"TenantFor{klass.__name__.replace('Multitenant', '')}-{i + 1}"
            if "Longevity" in klass.__name__:
                tenant_id = f"{tenant.test_config.test_id()}-{tenant._test_index}"
            else:
                tenant_id = f"id--{tenant._test_index}"
            assert tenant.id() == tenant_id
            assert tenant.params.get("prepare_write_cmd") == ["fake__prepare_write_cmd__all_tenants__single_str"]
            assert tenant.params.get("prepare_verify_cmd") == [
                f"fake__prepare_verify_cmd__all_tenants__part{j}" for j in range(1, 3)
            ]
            assert tenant.params.get("stress_cmd") == [f"fake__stress_cmd__all_tenants__part{j}" for j in range(1, 3)]
            assert tenant.params.get("stress_read_cmd") == ["fake__stress_read_cmd__all_tenants__single_str"]
            assert tenant.params.get("stress_cmd_w") == ["fake__stress_cmd_w__all_tenants__single_str"]

        # Process tenant-1 specific options
        for cmd_name in ("stress_cmd_r", "stress_cmd_m"):
            assert tenants[0].params.get(cmd_name) == [f"fake__{cmd_name}__tenant1__part{j}" for j in range(1, 3)]
        # NOTE: StringOrList type always converts single strings to a list with one element
        assert tenants[0].params.get("nemesis_class_name") == ["FakeNemesisClassNameForTenant1"]
        assert tenants[0].params.get("nemesis_selector") == [
            f"fake__nemesis_selector__tenant1__part{j}" for j in range(1, 3)
        ]
        assert tenants[0].params.get("nemesis_interval") == 5
        assert tenants[0].params.get("nemesis_sequence_sleep_between_ops") == 3
        assert tenants[0].params.get("nemesis_during_prepare") is False
        assert tenants[0].params.get("nemesis_seed") == 24
        assert tenants[0].params.get("nemesis_add_node_cnt") == 1
        assert tenants[0].params.get("space_node_threshold") == 1357
        assert tenants[0].params.get("nemesis_filter_seeds") is False
        assert tenants[0].params.get("nemesis_exclude_disabled") is True
        assert tenants[0].params.get("nemesis_multiply_factor") == 5
        assert tenants[0].params.get("round_robin") is True

        # Process tenant-2 specific options
        # NOTE: StringOrList type always converts single strings to a list with one element
        assert tenants[1].params.get("stress_cmd_r") == ["fake__stress_cmd_r__tenant2__single_str_in_list"]
        assert tenants[1].params.get("stress_cmd_m") == ["fake__stress_cmd_m__tenant2__single_str"]
        # NOTE: StringOrList type always converts single strings to a list with one element
        assert tenants[1].params.get("nemesis_class_name") == ["FakeNemesisClassNameForTenant2"]
        assert tenants[1].params.get("nemesis_selector") == ["fake__nemesis_selector__tenant2"]
        assert tenants[1].params.get("nemesis_interval") == 7
        assert tenants[1].params.get("nemesis_sequence_sleep_between_ops") == 5
        assert tenants[1].params.get("nemesis_during_prepare") is True
        assert tenants[1].params.get("nemesis_seed") == 135
        assert tenants[1].params.get("nemesis_add_node_cnt") == 2
        assert tenants[1].params.get("space_node_threshold") == 2468
        assert tenants[1].params.get("nemesis_filter_seeds") is True
        assert tenants[1].params.get("nemesis_exclude_disabled") is False
        assert tenants[1].params.get("nemesis_multiply_factor") == 6
        assert tenants[1].params.get("round_robin") is False

    def test_multitenant_longevity_class_with_unique_options(self):
        self._multitenant_class_with_unique_options(FakeMultitenantLongevityTest)

    def test_multitenant_performance_class_with_unique_options(self):
        self._multitenant_class_with_unique_options(FakeMultitenantPerformanceTest)
