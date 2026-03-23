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
# Copyright (c) 2024 ScyllaDB

import pytest

from sdcm import sct_config

_CFG_FILES = "unit_tests/test_configs"


def make_config(monkeypatch, **env_vars) -> sct_config.SCTConfiguration:
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return sct_config.SCTConfiguration()


def cfg_files(*names) -> str:
    return str([f"{_CFG_FILES}/{n}" for n in names]).replace("'", '"')


# ---------------------------------------------------------------------------
# a) stress_duration / prepare_stress_duration coercion
# ---------------------------------------------------------------------------


class TestCoerceStressDuration:
    @pytest.mark.parametrize(
        "value,expected",
        [
            pytest.param("60", 60, id="string-int"),
            pytest.param("-30", 30, id="string-negative-becomes-abs"),
            pytest.param("0", 0, id="zero"),
        ],
    )
    def test_valid_values(self, monkeypatch, value, expected):
        cfg = make_config(monkeypatch, SCT_STRESS_DURATION=value)
        assert cfg.get("stress_duration") == expected

    def test_non_numeric_string_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_STRESS_DURATION", "abc")
        with pytest.raises(ValueError, match="unable to parse string as an integer"):
            sct_config.SCTConfiguration()

    def test_prepare_duration_coercion(self, monkeypatch):
        cfg = make_config(monkeypatch, SCT_PREPARE_STRESS_DURATION="-45")
        assert cfg.get("prepare_stress_duration") == 45

    def test_prepare_duration_invalid_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PREPARE_STRESS_DURATION", "not_a_number")
        with pytest.raises(ValueError, match="unable to parse string as an integer"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# b) run_fullscan validation
# ---------------------------------------------------------------------------


class TestValidateRunFullscan:
    def test_not_set_skips_validation(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert not cfg.get("run_fullscan")

    def test_valid_list_with_json_config(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("positive_fullscan_param.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("run_fullscan")

    def test_invalid_json_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_fullscan_invalid_json.yaml"))
        with pytest.raises(ValueError, match="must be JSON"):
            sct_config.SCTConfiguration()

    def test_wrong_field_values_in_json_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("negative_fullscan_param.yaml"))
        with pytest.raises(ValueError):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# c) scylla_network_config validation
# ---------------------------------------------------------------------------


class TestValidateScyllaNetworkConfig:
    def test_none_skips_validation(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert cfg.get("scylla_network_config") is None

    def test_valid_config_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_valid_network_config.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("scylla_network_config") is not None

    def test_missing_nic_param_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_network_missing_nic.yaml"))
        with pytest.raises(ValueError, match="'nic' parameter value .* is not defined"):
            sct_config.SCTConfiguration()

    def test_ipv4_nic1_public_true_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("network_config_interface_param_public_not_primary.yaml"))
        with pytest.raises(ValueError, match="primary network interface"):
            sct_config.SCTConfiguration()

    def test_undefined_required_addresses_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("network_config_interface_not_defined.yaml"))
        with pytest.raises(ValueError, match="Interface address.es. were not defined"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# d) authenticator parameters validation
# ---------------------------------------------------------------------------


class TestValidateAuthenticatorParams:
    def test_no_authenticator_passes(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert cfg.get("authenticator") is None

    def test_password_authenticator_with_credentials_passes(self, monkeypatch):
        cfg = make_config(
            monkeypatch,
            SCT_AUTHENTICATOR="PasswordAuthenticator",
            SCT_AUTHENTICATOR_USER="admin",
            SCT_AUTHENTICATOR_PASSWORD="secret",
        )
        assert cfg.get("authenticator") == "PasswordAuthenticator"

    def test_password_authenticator_missing_user_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_AUTHENTICATOR", "PasswordAuthenticator")
        monkeypatch.setenv("SCT_AUTHENTICATOR_PASSWORD", "secret")
        with pytest.raises(ValueError, match="authenticator_user and authenticator_password"):
            sct_config.SCTConfiguration()

    def test_password_authenticator_missing_password_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_AUTHENTICATOR", "PasswordAuthenticator")
        monkeypatch.setenv("SCT_AUTHENTICATOR_USER", "admin")
        with pytest.raises(ValueError, match="authenticator_user and authenticator_password"):
            sct_config.SCTConfiguration()

    def test_alternator_enforce_without_authenticator_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_ALTERNATOR_ENFORCE_AUTHORIZATION", "true")
        with pytest.raises(ValueError, match="both `authenticator` and `authorizer` should be defined"):
            sct_config.SCTConfiguration()

    def test_alternator_enforce_without_authorizer_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_AUTHENTICATOR", "PasswordAuthenticator")
        monkeypatch.setenv("SCT_AUTHENTICATOR_USER", "admin")
        monkeypatch.setenv("SCT_AUTHENTICATOR_PASSWORD", "secret")
        monkeypatch.setenv("SCT_ALTERNATOR_ENFORCE_AUTHORIZATION", "true")
        with pytest.raises(ValueError, match="both `authenticator` and `authorizer` should be defined"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# e) endpoint_snitch validation with simulated regions/racks
# ---------------------------------------------------------------------------


class TestValidateEndpointSnitch:
    def test_simulated_regions_sets_gossip_snitch_on_docker(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_simulated_regions_gossip.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("endpoint_snitch") == "org.apache.cassandra.locator.GossipingPropertyFileSnitch"

    def test_no_simulated_regions_snitch_unchanged(self, monkeypatch):
        cfg = make_config(monkeypatch, SCT_ENDPOINT_SNITCH="SimpleSnitch")
        assert cfg.get("endpoint_snitch") == "SimpleSnitch"

    def test_wrong_snitch_with_simulated_regions_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_simulated_regions_wrong_snitch.yaml"))
        with pytest.raises((ValueError, AssertionError), match="GossipingPropertyFileSnitch"):
            sct_config.SCTConfiguration()

    def test_simulated_regions_sets_gossip_snitch_on_aws(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_simulated_regions_gossip.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("endpoint_snitch") == "org.apache.cassandra.locator.GossipingPropertyFileSnitch"


# ---------------------------------------------------------------------------
# f) use_dns_names validation
# ---------------------------------------------------------------------------


class TestValidateUseDnsNames:
    def test_use_dns_names_not_set_passes(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert not cfg.get("use_dns_names")

    def test_use_dns_names_aws_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("use_dns_names") is True

    def test_use_dns_names_gce_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
        monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
        with pytest.raises(ValueError, match="use_dns_names is not supported for gce"):
            sct_config.SCTConfiguration()

    def test_use_dns_names_docker_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_USE_DNS_NAMES", "true")
        with pytest.raises(ValueError, match="use_dns_names is not supported for docker"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# g) scylla_network_config multi-NIC multi-region validation
# ---------------------------------------------------------------------------


class TestValidateScyllaNetworkConfigMultiNic:
    def test_single_nic_multi_region_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_REGION_NAME", "us-east-1 us-west-2")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_valid_network_config.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert len(cfg.region_names) == 2

    def test_multi_nic_single_region_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_multi_nic_single_region.yaml"))
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("scylla_network_config") is not None

    def test_multi_nic_multi_region_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_CONFIG_FILES", cfg_files("validators_multi_nic_multi_region.yaml"))
        with pytest.raises(ValueError, match="Multiple network interfaces aren't supported for multi region"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# h) k8s_enable_sni validation
# ---------------------------------------------------------------------------


class TestValidateK8sTlsSni:
    def test_sni_not_set_passes(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert not cfg.get("k8s_enable_sni")

    def test_sni_true_tls_true_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_K8S_ENABLE_SNI", "true")
        monkeypatch.setenv("SCT_K8S_ENABLE_TLS", "true")
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("k8s_enable_sni") is True

    def test_sni_true_tls_false_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_K8S_ENABLE_SNI", "true")
        monkeypatch.setenv("SCT_K8S_ENABLE_TLS", "false")
        with pytest.raises(ValueError, match="k8s_enable_sni=true.*requires.*k8s_enable_tls"):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# i) zero token nodes validation
# ---------------------------------------------------------------------------


class TestValidateZeroTokenNodes:
    def test_use_zero_nodes_not_set_passes(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert not cfg.get("use_zero_nodes")

    def test_non_aws_backend_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_USE_ZERO_NODES", "true")
        with pytest.raises(AssertionError, match="Only AWS supports zero nodes"):
            sct_config.SCTConfiguration()

    def test_aws_backend_matching_single_dc_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_USE_ZERO_NODES", "true")
        monkeypatch.setenv("SCT_N_DB_ZERO_TOKEN_NODES", "1")
        monkeypatch.setenv("SCT_N_DB_NODES", "3")
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("use_zero_nodes") is True

    def test_aws_backend_mismatched_dc_count_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_USE_ZERO_NODES", "true")
        monkeypatch.setenv("SCT_N_DB_ZERO_TOKEN_NODES", "1 2")
        monkeypatch.setenv("SCT_N_DB_NODES", "3")
        with pytest.raises((AssertionError, ValueError)):
            sct_config.SCTConfiguration()


# ---------------------------------------------------------------------------
# j) performance throughput params validation
# ---------------------------------------------------------------------------


class TestValidatePerformanceThroughputParams:
    def test_no_throttle_steps_passes(self, monkeypatch):
        cfg = make_config(monkeypatch)
        assert cfg.get("perf_gradual_throttle_steps") is None

    def test_valid_single_thread_int_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000, 30000]}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": 16}')
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("perf_gradual_throttle_steps") is not None

    def test_valid_matching_thread_list_passes(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000, 30000]}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": [8, 16, 32]}')
        cfg = sct_config.SCTConfiguration()
        assert cfg.get("perf_gradual_throttle_steps") is not None

    def test_missing_gradual_threads_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000]}')
        with pytest.raises(ValueError, match="perf_gradual_threads should be defined"):
            sct_config.SCTConfiguration()

    def test_workload_not_in_threads_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000]}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"read": 16}')
        with pytest.raises(ValueError, match="Gradual threads for 'write' test is not defined"):
            sct_config.SCTConfiguration()

    def test_non_integer_thread_count_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000]}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": ["not-an-int", 16]}')
        with pytest.raises(ValueError, match="Invalid thread count type"):
            sct_config.SCTConfiguration()

    def test_mismatched_thread_list_length_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": [10000, 20000, 30000]}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": [8, 16]}')
        with pytest.raises(ValueError, match="same length as perf_gradual_throttle_steps"):
            sct_config.SCTConfiguration()

    def test_throttle_steps_not_list_raises(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
        monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": "not-a-list"}')
        monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": 16}')
        with pytest.raises(ValueError, match="should be a list"):
            sct_config.SCTConfiguration()
