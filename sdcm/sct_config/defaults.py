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

"""
Backend lists and per-backend requirement tables for the SCT configuration.

These are the data-only lookup tables that used to sit in the ``SCTConfiguration`` class body.
They stay the class-field defaults there (Pydantic deep-copies mutable defaults per instance,
which ``_check_backend_defaults`` relies on) -- this module just holds the literals.
"""

from sdcm import sct_abs_path

available_backends: list[str] = [
    "azure",
    "baremetal",
    "docker",
    # TODO: remove 'aws-siren' and 'gce-siren' backends completely when
    #       'siren-tests' project gets switched to the 'aws' and 'gce' ones.
    #       Such a switch must be fast change.
    "aws",
    "aws-siren",
    "k8s-local-kind-aws",
    "k8s-eks",
    "gce",
    "gce-siren",
    "k8s-local-kind-gce",
    "k8s-gke",
    "k8s-local-kind",
    "xcloud",
    "oci",
]

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

# Maps each cloud backend to the SCT config field that holds its machine image.
# Used by the pipeline linter to generate placeholder values for validation.
BACKEND_IMAGE_FIELD: dict[str, str] = {
    "aws": "ami_id_db_scylla",
    "gce": "gce_image_db",
    "azure": "azure_image_db",
    "docker": "docker_image",
    "oci": "oci_image_db",
}

REQUIRED_PARAMS: list = [
    "cluster_backend",
    "test_duration",
    "n_db_nodes",
    "n_loaders",
    "use_preinstalled_scylla",
    "user_credentials_path",
    "root_disk_size_db",
    "root_disk_size_monitor",
    "root_disk_size_loader",
]

# those can be added to a json scheme to validate / or write the validation code for it to be a bit clearer output
BACKEND_REQUIRED_PARAMS: dict = {
    "aws": [
        "user_prefix",
        "instance_type_loader",
        "instance_type_monitor",
        "instance_type_db",
        "region_name",
        "ami_id_db_scylla",
        "ami_id_loader",
        "ami_id_monitor",
        "aws_root_disk_name_monitor",
        "ami_db_scylla_user",
        "ami_monitor_user",
        "scylla_network_config",
    ],
    "gce": [
        "user_prefix",
        "gce_network",
        "gce_image_db",
        "gce_image_username",
        "gce_instance_type_db",
        "gce_root_disk_type_db",
        "gce_n_local_ssd_disk_db",
        "gce_instance_type_loader",
        "gce_root_disk_type_loader",
        "gce_instance_type_monitor",
        "gce_root_disk_type_monitor",
        "gce_datacenter",
    ],
    "azure": [
        "user_prefix",
        "azure_image_db",
        "azure_image_username",
        "azure_instance_type_db",
        "azure_instance_type_loader",
        "azure_instance_type_monitor",
        "azure_region_name",
    ],
    "oci": [
        "user_prefix",
        "oci_image_db",
        "oci_image_username",
        "oci_instance_type_db",
        "oci_instance_type_loader",
        "oci_instance_type_monitor",
        "oci_region_name",
    ],
    "docker": ["user_credentials_path", "scylla_version"],
    "baremetal": ["s3_baremetal_config", "user_credentials_path"],
    "aws-siren": [
        "user_prefix",
        "instance_type_loader",
        "region_name",
        "cloud_credentials_path",
    ],
    "gce-siren": [
        "user_prefix",
        "gce_network",
        "gce_image_username",
        "gce_instance_type_db",
        "gce_root_disk_type_db",
        "gce_n_local_ssd_disk_db",
        "gce_instance_type_loader",
        "gce_root_disk_type_loader",
        "gce_instance_type_monitor",
        "gce_root_disk_type_monitor",
        "gce_datacenter",
    ],
    "k8s-local-kind": [
        "user_credentials_path",
        "scylla_version",
        "scylla_mgmt_agent_version",
        "k8s_scylla_operator_helm_repo",
        "k8s_scylla_cluster_name",
        "k8s_scylla_disk_gi",
        "mini_k8s_version",
        "mgmt_docker_image",
    ],
    "k8s-local-kind-aws": [
        "user_credentials_path",
        "scylla_version",
        "scylla_mgmt_agent_version",
        "k8s_scylla_operator_helm_repo",
        "k8s_scylla_cluster_name",
        "k8s_scylla_disk_gi",
        "mini_k8s_version",
        "mgmt_docker_image",
    ],
    "k8s-local-kind-gce": [
        "user_credentials_path",
        "scylla_version",
        "scylla_mgmt_agent_version",
        "k8s_scylla_operator_helm_repo",
        "k8s_scylla_cluster_name",
        "k8s_scylla_disk_gi",
        "mini_k8s_version",
        "mgmt_docker_image",
    ],
    "k8s-gke": [
        "gke_cluster_version",
        "gce_instance_type_db",
        "gce_root_disk_type_db",
        "gce_n_local_ssd_disk_db",
        "user_credentials_path",
        "scylla_version",
        "scylla_mgmt_agent_version",
        "k8s_scylla_operator_helm_repo",
        "k8s_scylla_cluster_name",
        "k8s_loader_cluster_name",
        "gce_instance_type_loader",
        "gce_image_monitor",
        "gce_instance_type_monitor",
        "gce_root_disk_type_monitor",
        "gce_n_local_ssd_disk_monitor",
        "mgmt_docker_image",
    ],
    "k8s-eks": [
        "instance_type_loader",
        "instance_type_monitor",
        "instance_type_db",
        "region_name",
        "ami_id_db_scylla",
        "ami_id_monitor",
        "aws_root_disk_name_monitor",
        "ami_db_scylla_user",
        "ami_monitor_user",
        "user_credentials_path",
        "scylla_version",
        "scylla_mgmt_agent_version",
        "k8s_scylla_operator_docker_image",
        "k8s_scylla_cluster_name",
        "k8s_loader_cluster_name",
        "mgmt_docker_image",
        "eks_service_ipv4_cidr",
        "eks_vpc_cni_version",
        "eks_role_arn",
        "eks_admin_arn",
        "eks_cluster_version",
        "eks_nodegroup_role_arn",
    ],
    "xcloud": ["user_prefix", "xcloud_provider", "scylla_version"],
}

DEFAULTS_CONFIG_FILES: dict = {
    "aws": [sct_abs_path("defaults/aws_config.yaml"), sct_abs_path("defaults/aws_emr_config.yaml")],
    "gce": [sct_abs_path("defaults/gce_config.yaml")],
    "azure": [sct_abs_path("defaults/azure_config.yaml")],
    "oci": [sct_abs_path("defaults/oci_config.yaml")],
    "docker": [sct_abs_path("defaults/docker_config.yaml")],
    "baremetal": [sct_abs_path("defaults/baremetal_config.yaml")],
    "aws-siren": [sct_abs_path("defaults/aws_config.yaml")],
    "gce-siren": [sct_abs_path("defaults/gce_config.yaml")],
    "k8s-local-kind": [sct_abs_path("defaults/k8s_local_kind_config.yaml")],
    "k8s-local-kind-aws": [
        sct_abs_path("defaults/aws_config.yaml"),
        sct_abs_path("defaults/k8s_local_kind_aws_config.yaml"),
        sct_abs_path("defaults/k8s_local_kind_config.yaml"),
    ],
    "k8s-local-kind-gce": [
        sct_abs_path("defaults/k8s_local_kind_gce_config.yaml"),
        sct_abs_path("defaults/k8s_local_kind_config.yaml"),
    ],
    "k8s-gke": [sct_abs_path("defaults/gce_config.yaml"), sct_abs_path("defaults/k8s_gke_config.yaml")],
    "k8s-eks": [sct_abs_path("defaults/aws_config.yaml"), sct_abs_path("defaults/k8s_eks_config.yaml")],
    "xcloud": [sct_abs_path("defaults/cloud_config.yaml")],
}

PER_PROVIDER_MULTI_REGION_PARAMS: dict = {
    "aws": ["region_name", "ami_id_db_scylla", "ami_id_loader"],
    "gce": ["gce_datacenter"],
}

XCLOUD_PER_PROVIDER_REQUIRED_PARAMS: dict = {
    # There are two types of Cloud clusters available - Standard and XCloud
    # For XCloud clusters, the scaling policy (xcloud_scaling_config) includes instance type,
    # so it won't be provided in the params
    "standard": {
        "aws": ["region_name", "instance_type_db"],
        "gce": ["gce_datacenter", "gce_instance_type_db"],
    },
    "xcloud": {
        "aws": ["region_name"],
        "gce": ["gce_datacenter"],
    },
}

STRESS_CMD_PARAMS: list = [
    # this list is used for variouse checks against stress commands, such as:
    # 1. Check if all c-s profile files existing that are referred in the commands
    # 2. Check what stress tools test is needed when loader is prepared
    "gemini_cmd",
    "stress_cmd",
    "stress_read_cmd",
    "stress_cmd_w",
    "stress_cmd_r",
    "stress_cmd_m",
    "prepare_write_cmd",
    "stress_cmd_no_mv",
    "stress_cmd_no_mv_profile",
    "prepare_stress_cmd",
    "stress_cmd_1",
    "stress_cmd_complex_prepare",
    "prepare_write_stress",
    "stress_cmd_read_10m",
    "stress_cmd_read_cl_one",
    "stress_cmd_complex_verify_read",
    "stress_cmd_complex_verify_more",
    "write_stress_during_entire_test",
    "verify_data_after_entire_test",
    "stress_cmd_read_cl_quorum",
    "verify_stress_after_cluster_upgrade",
    "stress_cmd_complex_verify_delete",
    "stress_cmd_lwt_mixed",
    "stress_cmd_lwt_de",
    "stress_cmd_lwt_dc",
    "stress_cmd_lwt_ue",
    "stress_cmd_lwt_uc",
    "stress_cmd_lwt_ine",
    "stress_cmd_lwt_d",
    "stress_cmd_lwt_u",
    "stress_cmd_lwt_i",
]
AMI_ID_PARAMS: list = [
    "ami_id_db_scylla",
    "ami_id_loader",
    "ami_id_monitor",
    "ami_id_db_cassandra",
    "ami_id_db_oracle",
    "ami_id_vector_store",
]
