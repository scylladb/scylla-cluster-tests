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

"""Shared constants for the oracle Scylla cluster (db_type: mixed_scylla)."""

ORACLE_USER_PREFIX_SUFFIX = "-oracle"

ORACLE_PARAMS_BY_BACKEND = {
    "aws": {"image": "ami_id_db_oracle", "instance_type": "instance_type_db_oracle"},
    "gce": {"image": "gce_image_db_oracle", "instance_type": "gce_instance_type_db_oracle"},
    "azure": {"image": "azure_image_db_oracle", "instance_type": "azure_instance_type_db_oracle"},
    "oci": {"image": "oci_image_db_oracle", "instance_type": "oci_instance_type_db_oracle"},
}

ORACLE_IMAGE_PARAMS = {backend: params["image"] for backend, params in ORACLE_PARAMS_BY_BACKEND.items()}
ORACLE_INSTANCE_TYPE_PARAMS = {backend: params["instance_type"] for backend, params in ORACLE_PARAMS_BY_BACKEND.items()}
