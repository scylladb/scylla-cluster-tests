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
# Copyright (c) 2026 ScyllaDB

"""Which provisioning failures the generic `provision_with_retry` wrapper re-issues."""

from unittest.mock import MagicMock

import pytest

from sdcm.provision.provisioner import (
    InstanceConfigurationError,
    PricingModel,
    ProvisionError,
)
from sdcm.sct_provision.instances_provider import provision_with_retry


def test_config_error_is_not_retried():
    """An unsupported machine-type/disk-type combination can only fail again - issue the request once."""
    provisioner = MagicMock()
    provisioner.get_or_create_instances.side_effect = InstanceConfigurationError(
        "[pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance."
    )
    with pytest.raises(InstanceConfigurationError):
        provision_with_retry(provisioner, definitions=[], pricing_model=PricingModel.ON_DEMAND)
    assert provisioner.get_or_create_instances.call_count == 1


def test_generic_provision_error_is_still_retried(monkeypatch):
    """Guard the exemption above: ordinary ProvisionErrors keep their three attempts."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    provisioner = MagicMock()
    provisioner.get_or_create_instances.side_effect = ProvisionError("transient API failure")
    with pytest.raises(ProvisionError):
        provision_with_retry(provisioner, definitions=[], pricing_model=PricingModel.ON_DEMAND)
    assert provisioner.get_or_create_instances.call_count == 3
