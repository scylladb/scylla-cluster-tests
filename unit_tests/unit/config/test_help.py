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
# Copyright (c) 2020 ScyllaDB

import pytest


@pytest.mark.parametrize(
    ("field_name", "default_value"),
    [
        pytest.param("collect_logs", "False", id="collect_logs-false"),
        pytest.param("cloud_credentials_path", "N/A", id="cloud_credentials_path-na"),
        pytest.param("sizing_db", "N/A", id="sizing_db-na"),
    ],
)
def test_dump_help_config_yaml_defaults(help_yaml, field_name, default_value):
    assert f"{field_name}: {default_value}" in help_yaml


@pytest.mark.parametrize(
    ("field_name", "default_value"),
    [
        pytest.param("collect_logs", "False", id="collect_logs-false"),
        pytest.param("cloud_credentials_path", "N/A", id="cloud_credentials_path-na"),
        pytest.param("sizing_db", "N/A", id="sizing_db-na"),
        pytest.param("instance_type_loader", "N/A", id="instance_type_loader-na"),
    ],
)
def test_dump_help_config_markdown_defaults(help_markdown, field_name, default_value):
    assert f"## **{field_name}** / SCT_{field_name.upper()}" in help_markdown
    assert f"**default:** {default_value}" in help_markdown


@pytest.mark.parametrize(
    ("field_name", "type_text"),
    [
        pytest.param("collect_logs", "bool", id="collect_logs-bool"),
        pytest.param("instance_type_loader", "str (appendable)", id="instance_type_loader-appendable"),
    ],
)
def test_dump_help_config_markdown_types(help_markdown, field_name, type_text):
    assert f"## **{field_name}** / SCT_{field_name.upper()}" in help_markdown
    assert f"**type:** {type_text}" in help_markdown
    assert "* appendable" not in help_markdown


@pytest.mark.parametrize(
    ("field_name", "backend_override_text", "should_exist"),
    [
        pytest.param(
            "instance_provision_fallback_on_demand",
            "**backend overrides:**\n- `True`: aws, azure, aws-siren, k8s-local-kind-aws, k8s-eks",
            True,
            id="instance_provision_fallback_on_demand-true-overrides",
        ),
        pytest.param(
            "use_preinstalled_scylla",
            "**backend overrides:**\n- `True`: aws, gce, azure, oci, docker, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks",
            True,
            id="use_preinstalled_scylla-true-overrides",
        ),
        pytest.param(
            "backup_bucket_backend",
            "**backend overrides:**\n- `s3`: aws, oci, aws-siren, k8s-local-kind-aws, k8s-gke, k8s-eks\n- `gcs`: gce, gce-siren\n- `azure`: azure",
            True,
            id="backup_bucket_backend-multi-value-overrides",
        ),
        pytest.param(
            "user_credentials_path",
            "**backend overrides:**\n- `N/A`: aws, gce, azure, oci, docker, baremetal, aws-siren, gce-siren, k8s-local-kind, k8s-local-kind-aws, k8s-local-kind-gce, k8s-gke, k8s-eks",
            False,
            id="user_credentials_path-omits-na-overrides",
        ),
        pytest.param(
            "instance_type_loader",
            "**backend overrides:**\n- `N/A`: aws, aws-siren, k8s-local-kind-aws, k8s-eks",
            False,
            id="instance_type_loader-omits-na-overrides",
        ),
    ],
)
def test_dump_help_config_markdown_backend_overrides(help_markdown, field_name, backend_override_text, should_exist):
    assert f"## **{field_name}** / SCT_{field_name.upper()}" in help_markdown
    if should_exist:
        assert backend_override_text in help_markdown
    else:
        assert backend_override_text not in help_markdown
