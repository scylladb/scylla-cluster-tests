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

import logging
from functools import cached_property
from typing import Optional
from string import Template
from ipaddress import ip_network

import click
import requests

from sdcm.utils.oci_region import OciRegion
from sdcm.sct_runner import OciSctRunner
from sdcm.keystore import KeyStore
from sdcm.utils.sct_cmd_helpers import get_all_regions

LOGGER = logging.getLogger(__name__)

OCI_JENKINS_CONFIG_TEMPLATE = Template(r"""
// OCI Compute Cloud configuration for Jenkins
// Uses BaremetalCloudAgentTemplate with proper constructor signature
// If instantiation fails, it will print config for manual setup: Manage Jenkins -> System -> OCI Compute Cloud
//
// IDEMPOTENT: This script can be run multiple times safely. It will:
// 1. Remove any existing cloud configuration with the same name
// 2. Create a fresh configuration with current parameters
// 3. Report whether it was an update or new creation
//
// NOTE: You may see OCI SDK warnings like "Using an unknown runtime" or "Received unknown value 'GENERIC_BM'"
// These are harmless - the OCI SDK logs warnings for new shape types it doesn't recognize yet.

import jenkins.model.Jenkins
import hudson.model.Node
import java.util.Collections

config = [
    name: "$name",
    region: "$region_name",
    compartmentId: "$compartment_id",
    vcnId: "$vcn_id",
    availabilityDomain: "$availability_domain",
    imageId: "$image_id",
    shape: "$shape",
    subnetId: "$subnet_id",
    labels: "$jenkins_labels",
    numExecutors: "$num_executors",
]

println("=== OCI Builder Configuration ===")
config.each { k, v -> println("$${k}: $${v}") }
println("==================================")

def tryConfigureOciCloud() {
    try {
        // Get Jenkins instance
        def jenkins = Jenkins.getInstance()
        if (jenkins == null) {
            println("ERROR: Unable to get Jenkins instance")
            return false
        }
        println("Successfully obtained Jenkins instance")

        def tmplClass = Class.forName("com.oracle.cloud.baremetal.jenkins.BaremetalCloudAgentTemplate")
        def cloudClass = Class.forName("com.oracle.cloud.baremetal.jenkins.BaremetalCloud")

        def nsgList = Collections.emptyList()
        def tagList = Collections.emptyList()

        // Correct DataBoundConstructor signature from plugin
        def ctor = tmplClass.getDeclaredConstructor(
            String,                    // compartmentId
            String,                    // availableDomain
            String,                    // vcnCompartmentId
            String,                    // vcnId
            String,                    // subnetCompartmentId
            String,                    // subnetId
            List,                      // nsgIds (List<BaremetalCloudNsgTemplate>)
            String,                    // imageCompartmentId
            String,                    // imageId
            String,                    // shape
            String,                    // sshCredentialsId
            String,                    // description
            String,                    // remoteFS
            Boolean,                   // assignPublicIP
            Boolean,                   // usePublicIP
            String,                    // numExecutors
            Node.Mode,                 // mode
            String,                    // labelString
            String,                    // idleTerminationMinutes
            int,                       // templateId
            String,                    // jenkinsAgentUser
            String,                    // customJavaPath
            String,                    // customJVMOpts
            String,                    // initScript
            Boolean,                   // exportJenkinsEnvVars
            String,                    // sshConnectTimeoutSeconds
            Boolean,                   // verificationStrategy
            String,                    // startTimeoutSeconds
            String,                    // initScriptTimeoutSeconds
            String,                    // instanceCap
            String,                    // numberOfOcpus
            Boolean,                   // autoImageUpdate
            Boolean,                   // stopOnIdle
            List,                      // tags (List<BaremetalCloudTagsTemplate>)
            String,                    // instanceNamePrefix
            String,                    // memoryInGBs
            Boolean,                   // doNotDisable
            String                     // retryTimeoutMins
        )

        def tmpl = ctor.newInstance(
            config.compartmentId,           // compartmentId
            config.availabilityDomain,      // availableDomain
            config.compartmentId,           // vcnCompartmentId
            config.vcnId,                   // vcnId
            config.compartmentId,           // subnetCompartmentId
            config.subnetId,                // subnetId
            nsgList,                        // nsgIds
            config.compartmentId,           // imageCompartmentId
            config.imageId,                 // imageId
            config.shape,                   // shape
            "user-jenkins_scylla-qa-ec2-rsa.pem", // sshCredentialsId - cause of https://issues.jenkins.io/browse/JENKINS-76347
            config.name,                    // description
            "/tmp/jenkins",                // remoteFS
            true,                           // assignPublicIP
            true,                           // usePublicIP
            config.numExecutors,            // numExecutors (String, not int!)
            Node.Mode.NORMAL,               // mode
            config.labels,                  // labelString
            "6",                           // idleTerminationMinutes
            0,                              // templateId
            "ubuntu",                      // jenkinsAgentUser
            null,                           // customJavaPath
            "-Djdk.httpclient.maxLiteralWithIndexing=0 -Djdk.httpclient.maxNonFinalResponses=0", // customJVMOpts
            "",                            // initScript
            false,                          // exportJenkinsEnvVars
            "60",                          // sshConnectTimeoutSeconds
            false,                          // verificationStrategy
            "600",                         // startTimeoutSeconds
            "300",                         // initScriptTimeoutSeconds
            "20",                          // instanceCap
            "2",                           // numberOfOcpus
            false,                          // autoImageUpdate
            false,                          // stopOnIdle
            tagList,                        // tags
            "oci-builder",                 // instanceNamePrefix
            null,                           // memoryInGBs
            false,                          // doNotDisable
            "30"                           // retryTimeoutMins
        )

        println("Successfully created BaremetalCloudAgentTemplate")

        // Create cloud instance
        def cloud = cloudClass.getDeclaredConstructor(String, String, String, String, int, List).newInstance(
            config.name,
            "oci-sct-user", // credentialsId (set Jenkins credential ID if available)
            "20",        // instanceCapStr
            "10",        // maxAsyncThreads
            0,           // nextTemplateId
            [tmpl]
        )

        println("Successfully created BaremetalCloud instance")

        // Try to set region if setter exists
        if (cloud.metaClass.getMetaMethod("setRegion", String)) {
            cloud.setRegion(config.region)
            println("Successfully set region: $${config.region}")
        }

        // clear old configuration
        jenkins.clouds.removeAll { it.name == "oci-compute-$${config.name}" }

        // Add the new cloud
        jenkins.clouds.add(cloud)
        jenkins.save()
        println("OCI cloud configured programmatically: " + config.name)

        return true
    } catch (Throwable t) {
        println("Automatic OCI cloud configuration failed: " + t.getClass().getName())
        println("Message: " + t.message)
        println("Cause: " + (t.cause ? t.cause.toString() : "None"))
        t.printStackTrace()
        return false
    }
}

if (!tryConfigureOciCloud()) {
    println("")
    println("Manual Configuration Required:")
    println("Go to: Manage Jenkins -> System -> OCI Compute Cloud")
    println("Configure with these settings:")
    println("  Name: $${config.name}")
    println("  Region: $${config.region}")
    println("  Compartment ID: $${config.compartmentId}")
    println("  VCN ID: $${config.vcnId}")
    println("  Subnet ID: $${config.subnetId}")
    println("  Image ID: $${config.imageId}")
    println("  Shape: $${config.shape}")
}
""")


class OciBuilder:
    """Configure OCI compute instances as Jenkins builders.

    This class creates instance configurations based on OCI compute resources
    and adds the configuration to Jenkins using the OCI Compute Cloud plugin.
    Following patterns from AwsBuilder and GceBuilder.
    """

    NUM_CPUS = 2
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
        self.runner = OciSctRunner(
            region_name=self.region.region_name,
            availability_zone=first_ad,
        )

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

        # Use runner's full availability domain for subnet lookup
        full_ad = self.runner._full_availability_domain

        # Get or create subnet
        subnet = self.region.subnet(ad=full_ad)
        if not subnet:
            click.secho(f"{self.region.region_name}: creating subnet for {full_ad}")

            region_index = self.region._region_index()
            vcn_cidr = ip_network(self.region.SCT_VCN_CIDR_TMPL.format(region_index))
            ad_index = self.region.availability_domains.index(full_ad)
            subnet_cidr = list(vcn_cidr.subnets(prefixlen_diff=8))[ad_index]
            self.region.create_subnet(ad=full_ad, ipv4_cidr=subnet_cidr)
            subnet = self.region.subnet(ad=full_ad)

        if not subnet:
            raise ValueError(f"Unable to create or find subnet in {full_ad}")

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

    def _add_cloud_configuration_to_jenkins(self) -> None:
        """Add OCI cloud configuration to Jenkins via Script Console.

        Uses Jenkins Script Console to execute Groovy script that configures
        the OCI Compute Cloud plugin.

        This method is idempotent - running it multiple times will:
        1. Remove any existing cloud configuration with the same name
        2. Create a fresh configuration with the current parameters
        3. Report whether it was an update or new creation
        """
        click.secho(f"{self.region.region_name}: adding cloud configuration to Jenkins")

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
            jenkins_labels=self.jenkins_labels,
        )

        try:
            response = requests.post(
                url=f"{self.jenkins_info['url']}/scriptText",
                auth=(self.jenkins_info["username"], self.jenkins_info["password"]),
                data={"script": groovy_script},
                timeout=30,
            )
            response.raise_for_status()

            # Log the full response
            if response.text:
                click.secho(f"\n{self.region.region_name}: Jenkins Script Console output:", fg="cyan")
                click.secho("-" * 60)
                click.echo(response.text)
                click.secho("-" * 60)

                # Check if configuration was successful
                if "OCI cloud configured programmatically" in response.text:
                    # Check if it was an update or new creation
                    if "Configuration updated (replaced existing)" in response.text:
                        click.secho(
                            f"{self.region.region_name}: Jenkins configuration updated (idempotent) ✓",
                            fg="green",
                        )
                    elif "Configuration created (new)" in response.text:
                        click.secho(
                            f"{self.region.region_name}: Jenkins configuration created ✓",
                            fg="green",
                        )
                    else:
                        click.secho(
                            f"{self.region.region_name}: Jenkins configuration added successfully ✓",
                            fg="green",
                        )
                elif "Manual Configuration Required" in response.text:
                    click.secho(
                        f"{self.region.region_name}: Automatic configuration failed - manual setup required",
                        fg="yellow",
                    )
                    LOGGER.warning(f"Jenkins Script Console output for {self.region.region_name}:\n{response.text}")
                else:
                    click.secho(
                        f"{self.region.region_name}: Configuration status unclear - check output above",
                        fg="yellow",
                    )
            else:
                click.secho(
                    f"{self.region.region_name}: No output from Jenkins (script may have failed silently)",
                    fg="yellow",
                )
        except requests.RequestException as e:
            click.secho(
                f"Error adding cloud configuration to Jenkins: {e}",
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
