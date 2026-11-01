from types import SimpleNamespace

import pytest

from sdcm.utils.loader_utils import LoaderUtilsMixin

ROLES = [
    SimpleNamespace(name='"role300_0"', password="pass300"),
    SimpleNamespace(name='"role700_1"', password="pass700"),
]


@pytest.mark.parametrize(
    "cmd,expected",
    [
        pytest.param(
            "cassandra-stress read cl=QUORUM -mode cql3 native <sla credentials 1> -rate threads=10",
            "cassandra-stress read cl=QUORUM -mode cql3 native user=role700_1 password=pass700 -rate threads=10",
            id="cassandra-stress",
        ),
        pytest.param(
            "latte run --function write <sla credentials 0> --threads 8 data_dir/latte/latte_cs_alike.rn",
            "latte run --function write --user role300_0 --password pass300 --threads 8 data_dir/latte/latte_cs_alike.rn",
            id="latte",
        ),
        pytest.param(
            "latte run --function read --threads 8 data_dir/latte/latte_cs_alike.rn",
            "latte run --function read --threads 8 data_dir/latte/latte_cs_alike.rn",
            id="no-placeholder",
        ),
    ],
)
def test_set_sla_credentials_to_cmd(cmd, expected):
    assert LoaderUtilsMixin.set_sla_credentials_to_cmd(cmd=cmd, roles=ROLES) == expected


@pytest.mark.parametrize(
    "cmd",
    [
        "latte run --user someone <sla credentials 0> data_dir/latte/latte_cs_alike.rn",
        "cassandra-stress read user=someone <sla credentials 0>",
    ],
)
def test_set_sla_credentials_to_cmd_rejects_explicit_user(cmd):
    with pytest.raises(EnvironmentError, match="Credentials already applied"):
        LoaderUtilsMixin.set_sla_credentials_to_cmd(cmd=cmd, roles=ROLES)
