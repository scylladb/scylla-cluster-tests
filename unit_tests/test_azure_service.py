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

import re

import pytest

from sdcm.utils.azure_utils import AzureService

UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
ZERO_UUID = "00000000-0000-0000-0000-000000000000"


@pytest.mark.integration
def test_azure_service_tenant_id_is_valid():
    """Test that AzureService.tenant_id returns a valid tenant ID from Azure API.

    This test verifies that the tenant_id property correctly fetches the tenant ID
    from the Azure API, and that it's a valid UUID (not all zeros which would indicate
    a placeholder or invalid value).
    """
    azure_service = AzureService()

    tenant_id = azure_service.tenant_id

    # Verify it's a valid UUID format
    assert UUID_PATTERN.match(tenant_id), f"tenant_id '{tenant_id}' is not a valid UUID format"

    # Verify it's not the zero UUID (which indicates invalid/placeholder value)
    assert tenant_id != ZERO_UUID, "tenant_id should not be all zeros"


@pytest.mark.integration
def test_azure_service_tenant_id_matches_credentials():
    """Test that the fetched tenant_id matches the one in credentials (if valid).

    This test verifies that when credentials contain a valid tenant_id,
    the API-fetched tenant_id matches it.
    """
    azure_service = AzureService()

    api_tenant_id = azure_service.tenant_id
    credential_tenant_id = azure_service.azure_credentials.get("tenant_id")

    # If credentials have a valid (non-zero) tenant_id, it should match the API result
    if credential_tenant_id and credential_tenant_id != ZERO_UUID:
        assert api_tenant_id == credential_tenant_id, (
            f"API tenant_id '{api_tenant_id}' does not match credential tenant_id '{credential_tenant_id}'"
        )
