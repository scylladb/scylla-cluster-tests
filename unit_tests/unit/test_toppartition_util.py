from unittest import mock

from sdcm.utils.common import MAX_TABLES_IN_CMD
from sdcm.utils.toppartition_util import NewApiTopPartitionCmd


def test_new_api_filter_ks_cf_caps_table_count():
    cmd = NewApiTopPartitionCmd(ks_cf_list=[f"ks.t{i}" for i in range(5000)])
    # randint picks the upper bound, the most tables allowed
    with mock.patch("sdcm.utils.toppartition_util.random.randint", side_effect=lambda low, high: high):
        assert len(cmd._filter_ks_cf()) == MAX_TABLES_IN_CMD
