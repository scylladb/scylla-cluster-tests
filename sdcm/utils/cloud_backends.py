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

"""Cloud/backend lookups shared by the SCT config and by code the config imports.

Kept outside the ``sdcm.sct_config`` package, so modules imported while the config is still being
built (cluster, provisioning, nemesis) can use them without importing the config package.
"""

AWS_SUPPORTED_REGIONS: list[str] = [
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "us-west-2",
    "us-east-1",
    "us-east-2",
    "eu-north-1",
    "eu-central-1",
]

BACKEND_TO_CLOUD: dict[str, str] = {
    "aws": "aws",
    "aws-siren": "aws",
    "k8s-eks": "aws",
    "gce": "gce",
    "gce-siren": "gce",
    "k8s-gke": "gce",
    "azure": "azure",
    "oci": "oci",
}


SIZING_ROLE_PARAMS: dict[str, dict[str, str]] = {
    "aws": {
        "db": "instance_type_db",
        "db_oracle": "instance_type_db_oracle",
        "zero_token": "zero_token_instance_type_db",
        "loader": "instance_type_loader",
        "monitor": "instance_type_monitor",
    },
    "gce": {
        "db": "gce_instance_type_db",
        "db_oracle": "gce_instance_type_db_oracle",
        "loader": "gce_instance_type_loader",
        "monitor": "gce_instance_type_monitor",
    },
    "azure": {
        "db": "azure_instance_type_db",
        "db_oracle": "azure_instance_type_db_oracle",
        "loader": "azure_instance_type_loader",
        "monitor": "azure_instance_type_monitor",
    },
    "oci": {
        "db": "oci_instance_type_db",
        "db_oracle": "oci_instance_type_db_oracle",
        "loader": "oci_instance_type_loader",
        "monitor": "oci_instance_type_monitor",
    },
}


def oracle_cluster_in_use(db_type: str | None) -> bool:
    """Whether a run has an oracle cluster at all.

    The oracle node-count parameter defaults to 1 even when no oracle cluster exists, so the
    db_type is what actually decides. Shared so the rule has one definition rather than being
    restated wherever roles are walked.
    """
    return str(db_type or "") in ("mixed_scylla", "mixed_cassandra")


def backend_to_cloud(backend: str | None, xcloud_provider: str | None = None) -> str | None:
    if backend == "xcloud":
        return xcloud_provider or None
    return BACKEND_TO_CLOUD.get(str(backend))
