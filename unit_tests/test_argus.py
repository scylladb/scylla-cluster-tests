import os
import logging
from unittest import mock

import pytest

from sdcm.argus_test_run import ArgusTestRun

log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.need_network
@mock.patch.dict(os.environ, {
    "JOB_NAME": "test_connectivity",
})
@pytest.mark.sct_config(files='test-cases/PR-provision-test-docker.yaml')
def test_connectivity(params):
    argus_test_run = ArgusTestRun.from_sct_config(test_id='test_connectivity', sct_config=params)
    assert argus_test_run.argus.session
    session = argus_test_run.argus.session
    assert session
    res = session.execute("SELECT * FROM system.local").all()
    log.debug(res)

    assert len(res) == 1
