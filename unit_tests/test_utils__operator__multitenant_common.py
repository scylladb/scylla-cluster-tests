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

import os
import unittest

from sdcm import sct_config
from sdcm.test_config import TestConfig
from sdcm.utils.operator.multitenant_common import get_tenants


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


class UtilsOperatorMultitenantCommonTests(unittest.TestCase):

    def setUp(self):
        self.setup_default_env()

    def tearDown(self):
        self.clear_sct_env_variables()
        self.setup_default_env()

    @classmethod
    def setup_default_env(cls):
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ['SCT_CLUSTER_BACKEND'] = 'k8s-eks'

    @classmethod
    def clear_sct_env_variables(cls):
        for k in os.environ:
            if k.startswith('SCT_'):
                del os.environ[k]

    def _multitenant_class_with_shared_options(self, klass):
        os.environ['SCT_CONFIG_FILES'] = (
            'unit_tests/test_data/test_config/multitenant/shared_option_values.yaml')

        fake_multitenant_test_class_instance = klass()
        tenants = get_tenants(fake_multitenant_test_class_instance)

        self.assertEqual(2, len(tenants))
        for i, tenant in enumerate(tenants):
            self.assertEqual(
                tenant.__class__.__name__,
                f"TenantFor{klass.__name__.replace('Multitenant', '')}-{i + 1}")
            if "Longevity" in klass.__name__:
                tenant_id = f"{tenant.test_config.test_id()}-{tenant._test_index}"
            else:
                tenant_id = f"id--{tenant._test_index}"
            self.assertEqual(tenant.id(), tenant_id)
            self.assertEqual(
                tenant.params.get("prepare_write_cmd"),
                [f"fake__prepare_write_cmd__all_tenants__part{j}" for j in range(1, 3)])
            self.assertEqual(
                tenant.params.get("prepare_verify_cmd"),
                "fake__prepare_verify_cmd__all_tenants__single_str")
            for cmd_name in ("nemesis_selector", "stress_cmd_m", "stress_cmd", "stress_cmd_r"):
                self.assertEqual(
                    tenant.params.get(cmd_name),
                    [f"fake__{cmd_name}__all_tenants__part{j}" for j in range(1, 3)])
            for cmd_name in ("stress_read_cmd", "stress_cmd_w"):
                self.assertEqual(
                    tenant.params.get(cmd_name), f"fake__{cmd_name}__all_tenants__single_str")
            self.assertEqual(
                tenant.params.get("nemesis_class_name"), "FakeNemesisClassName")
            self.assertEqual(tenant.params.get("nemesis_interval"), 5)
            self.assertEqual(tenant.params.get("nemesis_sequence_sleep_between_ops"), 3)
            self.assertEqual(tenant.params.get("nemesis_during_prepare"), False)
            self.assertEqual(tenant.params.get("nemesis_seed"), "013")
            self.assertEqual(tenant.params.get("nemesis_add_node_cnt"), 1)
            self.assertEqual(tenant.params.get("space_node_threshold"), 13531)
            self.assertEqual(tenant.params.get("nemesis_filter_seeds"), False)
            self.assertEqual(tenant.params.get("nemesis_exclude_disabled"), True)
            self.assertEqual(tenant.params.get("nemesis_multiply_factor"), 5)
            self.assertEqual(tenant.params.get("round_robin"), True)

    def test_multitenant_longevity_class_with_shared_options(self):
        self._multitenant_class_with_shared_options(FakeMultitenantLongevityTest)

    def test_multitenant_performance_class_with_shared_options(self):
        self._multitenant_class_with_shared_options(FakeMultitenantPerformanceTest)

    def _multitenant_class_with_unique_options(self, klass):
        os.environ['SCT_CONFIG_FILES'] = (
            'unit_tests/test_data/test_config/multitenant/unique_option_values.yaml')

        fake_multitenant_longevity_test_class_instance = klass()
        tenants = get_tenants(fake_multitenant_longevity_test_class_instance)

        self.assertEqual(2, len(tenants))
        for i, tenant in enumerate(tenants):
            self.assertEqual(
                tenant.get_str_index(),
                f"k8s-{'perf' if 'Perf' in klass.__name__ else 'longevity'}-2-tenants")
            self.assertEqual(
                tenant.__class__.__name__,
                f"TenantFor{klass.__name__.replace('Multitenant', '')}-{i + 1}")
            if "Longevity" in klass.__name__:
                tenant_id = f"{tenant.test_config.test_id()}-{tenant._test_index}"
            else:
                tenant_id = f"id--{tenant._test_index}"
            self.assertEqual(tenant.id(), tenant_id)
            self.assertEqual(
                tenant.params.get("prepare_write_cmd"),
                "fake__prepare_write_cmd__all_tenants__single_str")
            self.assertEqual(
                tenant.params.get("prepare_verify_cmd"),
                [f"fake__prepare_verify_cmd__all_tenants__part{j}" for j in range(1, 3)])
            self.assertEqual(
                tenant.params.get("stress_cmd"),
                [f"fake__stress_cmd__all_tenants__part{j}" for j in range(1, 3)])
            self.assertEqual(
                tenant.params.get("stress_read_cmd"),
                "fake__stress_read_cmd__all_tenants__single_str")
            self.assertEqual(
                tenant.params.get("stress_cmd_w"), "fake__stress_cmd_w__all_tenants__single_str")

        # Process tenant-1 specific options
        for cmd_name in ("stress_cmd_r", "stress_cmd_m"):
            self.assertEqual(
                tenants[0].params.get(cmd_name),
                [f"fake__{cmd_name}__tenant1__part{j}" for j in range(1, 3)])
        self.assertEqual(
            tenants[0].params.get("nemesis_class_name"), "FakeNemesisClassNameForTenant1")
        self.assertEqual(
            tenants[0].params.get("nemesis_selector"),
            [f"fake__nemesis_selector__tenant1__part{j}" for j in range(1, 3)])
        self.assertEqual(tenants[0].params.get("nemesis_interval"), 5)
        self.assertEqual(tenants[0].params.get("nemesis_sequence_sleep_between_ops"), 3)
        self.assertEqual(tenants[0].params.get("nemesis_during_prepare"), False)
        self.assertEqual(tenants[0].params.get("nemesis_seed"), "024")
        self.assertEqual(tenants[0].params.get("nemesis_add_node_cnt"), 1)
        self.assertEqual(tenants[0].params.get("space_node_threshold"), 1357)
        self.assertEqual(tenants[0].params.get("nemesis_filter_seeds"), False)
        self.assertEqual(tenants[0].params.get("nemesis_exclude_disabled"), True)
        self.assertEqual(tenants[0].params.get("nemesis_multiply_factor"), 5)
        self.assertEqual(tenants[0].params.get("round_robin"), True)

        # Process tenant-2 specific options
        self.assertEqual(
            tenants[1].params.get("stress_cmd_r"), "fake__stress_cmd_r__tenant2__single_str_in_list")
        self.assertEqual(
            tenants[1].params.get("stress_cmd_m"), "fake__stress_cmd_m__tenant2__single_str")
        self.assertEqual(
            tenants[1].params.get("nemesis_class_name"), "FakeNemesisClassNameForTenant2")
        self.assertEqual(
            tenants[1].params.get("nemesis_selector"), ["fake__nemesis_selector__tenant2"])
        self.assertEqual(tenants[1].params.get("nemesis_interval"), 7)
        self.assertEqual(tenants[1].params.get("nemesis_sequence_sleep_between_ops"), 5)
        self.assertEqual(tenants[1].params.get("nemesis_during_prepare"), True)
        self.assertEqual(tenants[1].params.get("nemesis_seed"), "135")
        self.assertEqual(tenants[1].params.get("nemesis_add_node_cnt"), 2)
        self.assertEqual(tenants[1].params.get("space_node_threshold"), "2468")
        self.assertEqual(tenants[1].params.get("nemesis_filter_seeds"), True)
        self.assertEqual(tenants[1].params.get("nemesis_exclude_disabled"), False)
        self.assertEqual(tenants[1].params.get("nemesis_multiply_factor"), 6)
        self.assertEqual(tenants[1].params.get("round_robin"), False)

    def test_multitenant_longevity_class_with_unique_options(self):
        self._multitenant_class_with_unique_options(FakeMultitenantLongevityTest)

    def test_multitenant_performance_class_with_unique_options(self):
        self._multitenant_class_with_unique_options(FakeMultitenantPerformanceTest)


if __name__ == "__main__":
    unittest.main()
