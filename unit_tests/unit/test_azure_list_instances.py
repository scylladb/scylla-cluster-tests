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

from unittest.mock import patch

from azure.core.exceptions import ResourceNotFoundError

from sdcm.utils.azure_utils import list_instances_azure


def test_list_instances_azure_skips_vms_deleted_after_graph_query():
    """Resource Graph may still list a VM that ARM already deleted (SCT-992)."""

    def get_vm(resource_group_name, vm_name):
        if vm_name == "gone":
            raise ResourceNotFoundError("The Resource was not found")
        return vm_name

    with patch("sdcm.utils.azure_utils.AzureService") as azure_service:
        azure_service.return_value.resource_graph_query.return_value = [
            {"resourceGroup": "SCT-1", "name": "alive"},
            {"resourceGroup": "SCT-2", "name": "gone"},
        ]
        azure_service.return_value.compute.virtual_machines.get.side_effect = get_vm

        assert list_instances_azure(tags_dict={"NodeType": "sct-runner"}) == ["alive"]
