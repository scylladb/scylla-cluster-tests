from unittest.mock import MagicMock, patch

import pytest

from sdcm.keystore import SSHKey
from sdcm.utils.azure_builder import AzureBuilder, groovy_str
from sdcm.utils.azure_region import AzureRegion

AZURE_CREDS = {"subscription_id": "sub", "client_id": "cid", "client_secret": "s'e\\c$ret", "tenant_id": "tid"}


@pytest.fixture(name="builder")
def fixture_builder():
    with patch("sdcm.utils.azure_builder.KeyStore"), patch("sdcm.utils.azure_builder.AzureSctRunner"):
        builder = AzureBuilder(AzureRegion(region_name="eastus", azure_service=MagicMock()))
    builder.jenkins_info = {"url": "https://jenkins", "username": "u", "password": "p"}
    builder.runner.GALLERY_IMAGE_NAME = "sct-runner"
    builder.runner.GALLERY_IMAGE_VERSION = "1.21.0"
    return builder


@pytest.fixture(name="keystore")
def fixture_keystore():
    with patch("sdcm.utils.azure_builder.KeyStore") as keystore:
        keystore.return_value.get_azure_credentials.return_value = AZURE_CREDS
        keystore.return_value.get_ssh_key_pair.return_value = SSHKey(
            name="k", public_key=b"pub", private_key=b"-----BEGIN KEY-----\nabc\n-----END KEY-----\n"
        )
        yield keystore


def test_names(builder):
    assert builder.name == "azure-eastus-qa-builder-v1"
    assert builder.jenkins_labels == "azure-sct-builders-eastus-v1"
    assert builder.resource_group_name == "SCT-builders-eastus"


def test_groovy_str_escapes_quotes_backslashes_newlines():
    assert groovy_str("a'b\\c\nd$e") == "'a\\'b\\\\c\\nd$e'"


@pytest.mark.usefixtures("keystore")
def test_groovy_script_has_no_unquoted_values(builder):
    script = builder.groovy_script()
    assert "withGalleryImage('SCT_IMAGES', 'sct-runner', '1.21.0', true, 'sub', 'SCT_IMAGES-eastus')" in script
    assert "withExistingResourceGroupName('SCT-builders-eastus')" in script
    assert "withLabels('azure-sct-builders-eastus-v1')" in script
    assert "withSubnetName('builders')" in script
    assert "new AzureVMCloudOnceRetentionStrategy()" in script
    assert "new AzureTagPair('keep', '168')" in script
    assert "withMaxVirtualMachinesLimit(20)" in script
    assert "Secret.fromString('s\\'e\\\\c$ret')" in script
    assert "'-----BEGIN KEY-----\\nabc\\n-----END KEY-----\\n'" in script


@pytest.mark.usefixtures("keystore")
def test_script_console_error_with_http_200_fails(builder):
    """The script console returns 200 when the groovy throws; that must not pass as success."""
    response = MagicMock(status_code=200, text="groovy.lang.MissingMethodException: endRetentionStrategy()")
    with (
        patch("sdcm.utils.azure_builder.requests.post", return_value=response),
        pytest.raises(RuntimeError, match="MissingMethodException"),
    ):
        builder._add_cloud_configuration_to_jenkins()


@pytest.mark.usefixtures("keystore")
def test_script_console_success(builder):
    response = MagicMock(status_code=200, text="Azure cloud configured programmatically: azure-eastus-qa-builder-v1")
    with patch("sdcm.utils.azure_builder.requests.post", return_value=response) as post:
        builder._add_cloud_configuration_to_jenkins()
    assert post.call_args.kwargs["url"] == "https://jenkins/scriptText"


@pytest.mark.parametrize(
    "plugins, error",
    [
        ([], "not installed"),
        ([{"shortName": "azure-vm-agents", "version": "1124.v89b_2f7a_76f35", "active": False}], "not active"),
        ([{"shortName": "azure-vm-agents", "version": "1000.vabc", "active": True}], "older than 1124"),
    ],
)
def test_check_plugin_fails(builder, plugins, error):
    response = MagicMock(json=MagicMock(return_value={"plugins": plugins}))
    with (
        patch("sdcm.utils.azure_builder.requests.get", return_value=response),
        pytest.raises(RuntimeError, match=error),
    ):
        builder.check_plugin()


def test_check_plugin_passes(builder):
    plugins = [{"shortName": "azure-vm-agents", "version": "1124.v89b_2f7a_76f35", "active": True}]
    response = MagicMock(json=MagicMock(return_value={"plugins": plugins}))
    with patch("sdcm.utils.azure_builder.requests.get", return_value=response):
        builder.check_plugin()


@pytest.mark.parametrize(
    "exists, tags, expected",
    [
        (False, None, {"keep": "alive"}),
        (True, {"owner": "qa"}, {"owner": "qa", "keep": "alive"}),
        (True, {"keep": "alive"}, None),
    ],
)
def test_ensure_resource_group_sets_keep_alive(builder, exists, tags, expected):
    groups = builder.region.azure_service.resource.resource_groups
    groups.check_existence.return_value = exists
    groups.get.return_value = MagicMock(tags=tags)
    builder._ensure_resource_group()
    if expected is None:
        groups.create_or_update.assert_not_called()
    else:
        groups.create_or_update.assert_called_once_with("SCT-builders-eastus", {"location": "eastus", "tags": expected})


def test_ensure_runner_image_builds_missing_image(builder):
    builder.runner.image = None
    builder.runner.create_image.side_effect = lambda: setattr(builder.runner, "image", "img")
    builder._ensure_runner_image()
    builder.runner.create_image.assert_called_once()


def test_configure_in_all_region_fails_after_trying_every_region():
    configured = []

    def fake_configure(self):
        configured.append(self.region.region_name)
        if self.region.region_name == "westus":
            raise RuntimeError("script console error")

    with (
        patch(
            "sdcm.utils.azure_builder.AzureRegion", side_effect=lambda region_name: MagicMock(region_name=region_name)
        ),
        patch.object(AzureBuilder, "__init__", lambda self, region: setattr(self, "region", region)),
        patch.object(AzureBuilder, "configure", fake_configure),
        pytest.raises(RuntimeError, match="in: westus$"),
    ):
        AzureBuilder.configure_in_all_region(regions=["eastus", "westus", "northeurope"])

    assert configured == ["eastus", "westus", "northeurope"]


def test_ensure_builder_network_allows_ssh_only_from_sources():
    service = MagicMock()
    region = AzureRegion(region_name="eastus", azure_service=service)
    region.ensure_builder_network({"jenkins": ["10.1.0.0/16"], "vpn": ["192.0.2.0/24", "10.0.0.1/32"]})

    nsg_call = service.network.network_security_groups.begin_create_or_update.call_args.kwargs
    assert nsg_call["network_security_group_name"] == "SCT-builder-nsg"
    rules = nsg_call["parameters"].security_rules
    assert [(r.source_address_prefix, r.destination_port_range, r.priority) for r in rules] == [
        ("10.0.0.1/32", "22", 300),
        ("10.1.0.0/16", "22", 301),
        ("192.0.2.0/24", "22", 302),
    ]
    subnet_call = service.network.subnets.begin_create_or_update.call_args.kwargs
    assert subnet_call["subnet_name"] == "builders"
    assert subnet_call["subnet_parameters"].address_prefix == "10.0.1.0/24"


def test_configure_adds_missing_tags_to_existing_resource_group():
    service = MagicMock()
    groups = service.resource.resource_groups
    groups.check_existence.return_value = True
    groups.get.return_value = MagicMock(tags={"a": "b"}, location="eastus")
    AzureRegion(region_name="eastus", azure_service=service).create_sct_resource_group(tags={"keep": "alive"})
    groups.create_or_update.assert_called_once_with(
        resource_group_name="SCT_IMAGES-eastus", parameters={"location": "eastus", "tags": {"a": "b", "keep": "alive"}}
    )
