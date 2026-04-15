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

import json
import logging
from functools import cached_property
from typing import Optional
from string import Template
from pathlib import Path

import click
import requests

from sdcm.keystore import KeyStore
from sdcm.sct_runner import OciSctRunner
from sdcm.utils.oci_region import OciRegion
from sdcm.utils.sct_cmd_helpers import get_all_regions

LOGGER = logging.getLogger(__name__)

OCI_JENKINS_CONFIG_TEMPLATE = Template(
    (Path(__file__).parent / "builder_setup_groovy/oci_jenkind_plugin_config.groovy.tmpl").read_text()
)


class OciBuilder:
    """Configure OCI compute instances as Jenkins builders.

    This class creates instance configurations based on OCI compute resources
    and adds the configuration to Jenkins using the OCI Compute Cloud plugin.
    Following patterns from AwsBuilder and GceBuilder.
    """

    NUM_OCPUS = 2
    MEMORY_GB = 16
    NUM_EXECUTORS = 1
    VERSION = "v1"

    def __init__(
        self,
        region: OciRegion,
        params: Optional[dict] = None,
        number: int = 1,
    ):
        """Initialize OCI Builder.

        Args:
            region: OciRegion instance for region-specific configuration
            params: Optional parameters for builder configuration
            number: Builder instance number (for multiple builders in same region)
        """
        self.region = region
        self.number = number
        self.params = params or {}
        self.jenkins_info = KeyStore().get_json("jenkins.json")

        # Use the first availability domain from the region
        first_ad = region.availability_domains[0] if region.availability_domains else "a"
        self.runner = OciSctRunner(region_name=self.region.region_name, availability_zone=first_ad, params=None)

    @cached_property
    def name(self) -> str:
        """Generate builder name.

        Example: oci-us-ashburn-1-qa-builder-v1-1
        """
        return f"oci-{self.region.region_name}-qa-builder-{self.VERSION}-{self.number}"

    @cached_property
    def jenkins_labels(self) -> str:
        """Generate Jenkins labels for job routing."""
        return f"oci-sct-builders-{self.region.region_name}-{self.VERSION}"

    def _create_instance_configuration(self) -> dict:
        """Create OCI instance configuration for Jenkins.

        Returns:
            Dictionary with instance configuration parameters
        """
        click.secho(f"{self.region.region_name}: creating instance configuration")

        # Ensure SCT runner image exists
        if not self.runner.image:
            click.secho(f"{self.region.region_name}: building SCT runner image")
            self.runner.create_image()

        image_id = self.runner.image.id if self.runner.image else None
        if not image_id:
            raise ValueError(f"Unable to determine image ID for region {self.region.region_name}")

        # Ensure VCN and internet gateway exist
        _ = self.region.vcn
        _ = self.region.internet_gateway

        # Get or create subnet
        subnet = self.region.subnet(public=True)
        if not subnet:
            click.secho(f"{self.region.region_name}: creating a public subnet")
            subnet = self.region.create_subnet(public=True)
        if not subnet:
            raise ValueError(f"Unable to create or find a public subnet in {self.region.region_name}")

        return {
            "image_id": image_id,
            "shape": self.params.get("oci_shape", "VM.Standard.E4.Flex"),
            "vcn_id": self.region.vcn.id,
            "subnet_id": subnet.id,
            "compartment_id": self.region.compartment_id,
        }

    def _create_instance_pool(self) -> dict:
        """Create instance pool configuration.

        Returns:
            Dictionary with instance pool parameters
        """
        click.secho(f"{self.region.region_name}: creating instance pool configuration")

        return {
            "min_instances": self.params.get("oci_min_instances", 0),
            "max_instances": self.params.get("oci_max_instances", 5),
        }

    def _jenkins_crumb_headers(self) -> dict:
        """Fetch the Jenkins CSRF crumb header for POST requests.

        Returns an empty dict if crumb issuer is disabled on this Jenkins.
        """
        try:
            resp = requests.get(
                url=f"{self.jenkins_info['url']}/crumbIssuer/api/json",
                auth=(self.jenkins_info["username"], self.jenkins_info["password"]),
                timeout=10,
            )
            if resp.ok:
                data = resp.json()
                return {data["crumbRequestField"]: data["crumb"]}
        except (requests.RequestException, ValueError, KeyError) as exc:
            LOGGER.debug("No Jenkins crumb available (probably disabled): %s", exc)
        return {}

    def _upload_regional_oci_credential(self) -> str:
        """Ensure a per-region OCI credential exists in Jenkins; return its ID.

        Why: BaremetalCloud has no regionId field — the cloud's region comes
        from its credential's regionId (see JENKINS-76436). To target a
        region other than the SCT keystore credential's home region, we
        need a credential clone with regionId overridden.

        Only creates the credential if it doesn't already exist — skips the
        upload on subsequent runs to avoid re-encrypting the private key
        every time and to keep the operation cheap.

        The credential data (tenancy, user, fingerprint, private key) is
        read from the SCT keystore via KeyStore().get_oci_credentials() —
        which transparently reads from S3 or ~/.oci/config.

        Returns:
            The credential ID (e.g. "oci-sct-user-us-phoenix-1") that the
            Groovy template should reference.
        """
        cred_id = f"oci-sct-user-{self.region.region_name}"
        auth = (self.jenkins_info["username"], self.jenkins_info["password"])
        base_url = self.jenkins_info["url"]

        # Skip if the credential already exists. GET returns 200 if present,
        # 404 if missing. We don't validate the region matches — if someone
        # manually changed it, deleting it first is the user's call.
        check_url = f"{base_url}/credentials/store/system/domain/_/credential/{cred_id}/api/json"
        check_resp = requests.get(check_url, auth=auth, timeout=10)
        if check_resp.status_code == 200:
            click.secho(
                f"{self.region.region_name}: credential '{cred_id}' already exists — reusing",
                fg="cyan",
            )
            return cred_id
        if check_resp.status_code != 404:
            check_resp.raise_for_status()  # unexpected status — surface it

        oci_creds = KeyStore().get_oci_credentials()

        # Use Jenkins' stapler form endpoint so the plugin's DataBoundConstructor
        # runs — it calls getEncryptedValue(apikey) / getEncryptedValue(passphrase)
        # to encrypt secrets. Posting XML directly bypasses the constructor and
        # leaves apikey/passphrase as plaintext, which the getter later fails to
        # decrypt — that's why the UI showed them as empty.
        credential_payload = {
            "": "0",
            "credentials": {
                "scope": "GLOBAL",
                "id": cred_id,
                "description": (
                    f"OCI SCT user for {self.region.region_name} (auto-generated by SCT; JENKINS-76436 workaround)"
                ),
                "fingerprint": oci_creds["fingerprint"],
                "apikey": oci_creds["key_content"],
                "passphrase": "",
                "tenantId": oci_creds["tenancy"],
                "userId": oci_creds["user"],
                "regionId": self.region.region_name,
                "instancePrincipals": False,
                "$class": "com.oracle.cloud.baremetal.jenkins.credentials.BaremetalCloudCredentialsImpl",
            },
        }

        headers = self._jenkins_crumb_headers()
        create_url = f"{base_url}/credentials/store/system/domain/_/createCredentials"
        resp = requests.post(
            create_url,
            auth=auth,
            headers=headers,
            data={"json": json.dumps(credential_payload)},
            timeout=30,
        )
        resp.raise_for_status()

        click.secho(
            f"{self.region.region_name}: uploaded OCI credential '{cred_id}' (regionId={self.region.region_name})",
            fg="green",
        )
        return cred_id

    def _add_cloud_configuration_to_jenkins(self) -> None:
        """Add OCI cloud configuration to Jenkins via Script Console.

        Uses Jenkins Script Console to execute Groovy script that configures
        the OCI Compute Cloud plugin.

        This method is idempotent - running it multiple times will:
        1. Ensure the per-region OCI credential exists (only creates it if
           missing — see JENKINS-76436)
        2. Remove any existing cloud configuration with the same name
        3. Create a fresh configuration with the current parameters
        """
        click.secho(f"{self.region.region_name}: adding cloud configuration to Jenkins")

        credentials_id = self._upload_regional_oci_credential()

        instance_config = self._create_instance_configuration()
        instance_pool = self._create_instance_pool()

        groovy_script = OCI_JENKINS_CONFIG_TEMPLATE.substitute(
            name=self.name,
            region_name=self.region.region_name,
            compartment_id=instance_config["compartment_id"],
            vcn_id=instance_config["vcn_id"],
            availability_domain=self.runner._full_availability_domain,
            subnet_id=instance_config["subnet_id"],
            image_id=instance_config["image_id"],
            shape=instance_config["shape"],
            min_instances=instance_pool["min_instances"],
            max_instances=instance_pool["max_instances"],
            num_executors=self.NUM_EXECUTORS,
            num_ocpus=self.params.get("oci_ocpus", self.NUM_OCPUS),
            memory_gb=self.params.get("oci_memory_gb", self.MEMORY_GB),
            jenkins_labels=self.jenkins_labels,
            credentials_id=credentials_id,
        )

        try:
            response = requests.post(
                url=f"{self.jenkins_info['url']}/scriptText",
                auth=(self.jenkins_info["username"], self.jenkins_info["password"]),
                data={"script": groovy_script},
                timeout=30,
            )
            response.raise_for_status()

        except requests.RequestException as e:
            click.secho(
                f"Error adding cloud configuration to Jenkins: {e}\n{response.text}",
                fg="red",
                err=True,
            )
            raise

    def configure_instance_pool(self) -> None:
        """Configure OCI instance pool for Jenkins.

        This is the main entry point that orchestrates the configuration:
        1. Create instance configuration (image, shape, network)
        2. Create instance pool (auto-scaling settings)
        3. Add configuration to Jenkins
        """
        click.secho(f"\n{self.region.region_name}: Configuring OCI instance pool")
        click.secho("=" * 60)

        try:
            self._create_instance_configuration()
            self._create_instance_pool()
            self._add_cloud_configuration_to_jenkins()

            click.secho(f"{self.region.region_name}: Instance pool configuration complete", fg="green")
        except Exception as e:
            click.secho(
                f"Error during instance pool configuration: {e}",
                fg="red",
                err=True,
            )
            LOGGER.exception("Exception during OCI builder configuration")
            raise

    def configure(self) -> None:
        """Configure OCI instance pool for Jenkins.

        Alias for configure_instance_pool() to match pattern from other builders.
        """
        self.configure_instance_pool()

    @classmethod
    def configure_in_all_region(cls, regions=None) -> None:
        """Configure OCI builders in all specified regions.

        Args:
            regions: List of region names to configure. If None, uses all supported OCI regions.
        """

        regions = regions or get_all_regions(cloud_provider="oci")
        for region_name in regions:
            try:
                click.secho(f"\nConfiguring OCI builder in region: {region_name}", fg="cyan")
                oci_region = OciRegion(
                    region_name=region_name,
                )
                builder = cls(region=oci_region)
                builder.configure()
            except Exception as e:
                click.secho(
                    f"Error configuring OCI builder in region {region_name}: {e}",
                    fg="red",
                    err=True,
                )
                LOGGER.exception(f"Exception configuring OCI builder in {region_name}")
                # Continue with next region instead of failing completely
                continue

    @staticmethod
    def validate_jenkins_connectivity(jenkins_info: dict) -> bool:
        """Validate that Jenkins is accessible.

        Args:
            jenkins_info: Dictionary with Jenkins connection information

        Returns:
            True if Jenkins is accessible, False otherwise
        """
        try:
            response = requests.get(
                url=f"{jenkins_info['url']}/api/json",
                auth=(jenkins_info["username"], jenkins_info["password"]),
                timeout=10,
            )
            response.raise_for_status()
            return True
        except (requests.RequestException, KeyError) as e:
            LOGGER.error(f"Unable to connect to Jenkins: {e}")
            return False
