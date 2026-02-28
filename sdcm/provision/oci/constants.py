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

TAG_NAMESPACE = "sct"
SCT_TAG_KEYS = (
    "Name",
    "Version",
    "TestId",
    "TestName",
    "NodeType",
    "NodeIndex",
    "keep",
    "keep_action",
    "UserName",
    "bastion",
    "launch_time",
    "RunByUser",
    "RestoredTestId",
    "CreatedBy",
    "JenkinsJobTag",
    "version",
    "ssh_user",
    "ssh_key",
    "logs_collected",
)

# NOTE: Oracle cloud uses short region identifiers in its APIs, so match it to the user-facing ones
#       See: https://docs.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm
OCI_REGION_NAMES_MAPPING = {
    "iad": "us-ashburn-1",
    "us-ashburn-1": "us-ashburn-1",
    "phx": "us-phoenix-1",
    "us-phoenix-1": "us-phoenix-1",
    # TODO: add key-values pairs for other regions
}
