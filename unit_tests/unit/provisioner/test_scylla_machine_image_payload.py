import json

from sdcm.sct_provision.user_data_objects.scylla import ScyllaUserDataObject
from sdcm.test_config import TestConfig


def test_can_generate_valid_scylla_machine_image_payload():
    test_config = TestConfig()
    test_config.set_test_id("12345678-87654321")
    test_id_short = test_config.test_id()[:8]
    sud = ScyllaUserDataObject(
        test_config=test_config,
        params={"data_volume_disk_num": 2, "user_prefix": "unit-test"},
        instance_name="test-instance",
        node_type="scylla-db",
    )
    assert sud.scylla_machine_image_json == json.dumps(
        {
            "start_scylla_on_first_boot": False,
            "data_device": "attached",
            "raid_level": 0,
            "scylla_yaml": {"cluster_name": f"unit-test-{test_id_short}"},
        }
    )


def test_can_generate_valid_scylla_machine_image_payload_with_minimum_params():
    test_config = TestConfig()
    test_config.set_test_id("12345678-87654321")
    test_id_short = test_config.test_id()[:8]
    params = {"user_prefix": "test_user"}
    sud = ScyllaUserDataObject(
        test_config=test_config, params=params, instance_name="test-instance", node_type="scylla-db"
    )
    assert sud.scylla_machine_image_json == json.dumps(
        {
            "start_scylla_on_first_boot": False,
            "data_device": "instance_store",
            "raid_level": 0,
            "scylla_yaml": {"cluster_name": f"test_user-{test_id_short}"},
        }
    )
