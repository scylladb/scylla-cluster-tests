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
# Copyright (c) 2025 ScyllaDB
import json

import pytest

from sdcm.utils.gce_network_params import (
    GcpInstanceNetworkInfo,
    _GCP_NET_PARAMS_FILE,
    calculate_stream_io_throughput,
    get_gcp_instance_network_info,
)


GCP_NET_PARAMS_FILE = _GCP_NET_PARAMS_FILE.resolve()


class TestGcpNetParamsJson:
    """Validate the structure and content of gcp_net_params.json."""

    @pytest.fixture(scope="class")
    def raw_data(self):
        with open(GCP_NET_PARAMS_FILE, encoding="utf-8") as fh:
            return json.load(fh)

    def test_json_is_list(self, raw_data):
        assert isinstance(raw_data, list)

    def test_entries_have_valid_structure(self, raw_data):
        for entry in raw_data:
            assert isinstance(entry, list), f"Entry is not a list: {entry}"
            assert len(entry) == 4, f"Entry does not have 4 elements: {entry}"
            assert isinstance(entry[0], str), f"Instance type is not a string: {entry}"
            assert isinstance(entry[1], str), f"Description is not a string: {entry}"
            assert isinstance(entry[2], (int, float)), f"Bandwidth is not a number: {entry}"
            assert entry[3] is None or isinstance(entry[3], (int, float)), (
                f"Tier1 bandwidth is not a number or null: {entry}"
            )

    def test_all_instance_types_are_unique(self, raw_data):
        instance_types = [entry[0] for entry in raw_data]
        assert len(instance_types) == len(set(instance_types)), "Duplicate instance types found"

    def test_contains_n2_family(self, raw_data):
        n2_types = [e[0] for e in raw_data if e[0].startswith("n2-")]
        assert len(n2_types) >= 9, f"Expected at least 9 N2 types, found {len(n2_types)}"

    def test_contains_n2d_family(self, raw_data):
        n2d_types = [e[0] for e in raw_data if e[0].startswith("n2d-")]
        assert len(n2d_types) >= 9, f"Expected at least 9 N2D types, found {len(n2d_types)}"

    def test_contains_z3_family(self, raw_data):
        z3_types = [e[0] for e in raw_data if e[0].startswith("z3-")]
        assert len(z3_types) >= 5, f"Expected at least 5 Z3 types, found {len(z3_types)}"

    def test_bandwidth_values_are_positive(self, raw_data):
        for entry in raw_data:
            assert entry[2] > 0, f"Default bandwidth must be positive: {entry}"
            if entry[3] is not None:
                assert entry[3] > 0, f"Tier1 bandwidth must be positive if set: {entry}"
                assert entry[3] >= entry[2], f"Tier1 bandwidth must be >= default: {entry}"


class TestGetGcpInstanceNetworkInfo:
    """Test the lookup function for GCP instance network info."""

    def test_known_instance_type(self):
        info = get_gcp_instance_network_info("n2-highmem-8")
        assert info is not None
        assert isinstance(info, GcpInstanceNetworkInfo)
        assert info.instance_type == "n2-highmem-8"
        assert info.default_bandwidth_gbps == 16.0
        assert info.tier1_bandwidth_gbps is None

    def test_known_instance_with_tier1(self):
        info = get_gcp_instance_network_info("n2-highmem-32")
        assert info is not None
        assert info.default_bandwidth_gbps == 32.0
        assert info.tier1_bandwidth_gbps == 50.0

    def test_z3_instance(self):
        info = get_gcp_instance_network_info("z3-highmem-88-standardlssd")
        assert info is not None
        assert info.default_bandwidth_gbps == 62.0
        assert info.tier1_bandwidth_gbps == 100.0

    def test_z3_highlssd_instance(self):
        info = get_gcp_instance_network_info("z3-highmem-8-highlssd")
        assert info is not None
        assert info.default_bandwidth_gbps == 23.0
        assert info.tier1_bandwidth_gbps is None

    def test_unknown_instance_type_returns_none(self):
        info = get_gcp_instance_network_info("nonexistent-machine-42")
        assert info is None


class TestCalculateStreamIoThroughput:
    """Test the stream_io_throughput_mb_per_sec calculation."""

    @pytest.mark.parametrize(
        "instance_type, tier1, expected",
        [
            # n2-highmem-8: 16 Gbps → 16 * 125 = 2000 MB/s → 2000 * 0.75 = 1500
            ("n2-highmem-8", False, 1500),
            # n2-highmem-32: 32 Gbps → 32 * 125 = 4000 → 4000 * 0.75 = 3000
            ("n2-highmem-32", False, 3000),
            # n2-highmem-32 with tier1: 50 Gbps → 50 * 125 = 6250 → 6250 * 0.75 = 4687
            ("n2-highmem-32", True, 4687),
            # z3-highmem-88-standardlssd: 62 Gbps → 62 * 125 = 7750 → 7750 * 0.75 = 5812
            ("z3-highmem-88-standardlssd", False, 5812),
            # z3-highmem-88-standardlssd with tier1: 100 Gbps → 12500 → 9375
            ("z3-highmem-88-standardlssd", True, 9375),
            # z3-highmem-176-standardlssd with tier1: 200 Gbps → 25000 → 18750
            ("z3-highmem-176-standardlssd", True, 18750),
            # n2-standard-2: 10 Gbps → 1250 → 937
            ("n2-standard-2", False, 937),
            # z3-highmem-8-highlssd: 23 Gbps → 2875 → 2156 (tier1 N/A → uses default)
            ("z3-highmem-8-highlssd", True, 2156),
        ],
    )
    def test_calculation(self, instance_type, tier1, expected):
        result = calculate_stream_io_throughput(instance_type, tier1=tier1)
        assert result == expected

    def test_unknown_instance_returns_none(self):
        result = calculate_stream_io_throughput("nonexistent-machine-42")
        assert result is None

    def test_tier1_requested_but_not_available_falls_back_to_default(self):
        """When tier1 is requested but not available, the default bandwidth is used."""
        result_default = calculate_stream_io_throughput("n2-highmem-8", tier1=False)
        result_tier1 = calculate_stream_io_throughput("n2-highmem-8", tier1=True)
        assert result_default == result_tier1
