#!/usr/bin/env python
import json
import logging
import pprint
from functools import cached_property
from textwrap import dedent

import boto3

from argus.client.sct.types import Package

from sdcm.cluster import BaseNode
from sdcm.remote.remote_file import remote_file
from sdcm.test_config import TestConfig

LOGGER = logging.getLogger(__name__)


class ScyllaDoctorException(Exception):
    pass


class ScyllaDoctor:
    SCYLLA_DOCTOR_OFFLINE_DOWNLOAD_URI = "https://downloads.scylladb.com/"
    SCYLLA_DOCTOR_OFFLINE_BUCKET_NAME = "downloads.scylladb.com"
    SCYLLA_DOCTOR_OFFLINE_BUCKET_PREFIX = "downloads/scylla-doctor/tar/"
    SCYLLA_DOCTOR_OFFLINE_BIN = "scylla_doctor.pyz"
    SCYLLA_DOCTOR_OFFLINE_CONF = "scylla_doctor.conf"
    SCYLLA_DOCTOR_DISABLED_OFFLINE_COLLECTORS = dedent("""
        [GossipInfoCollector]
        ; Doesn't work with systemd-user service
        run = no
        [SystemTopologyCollector]
        ; Depends on GossipInfoCollector
        run = no
        [TokenMetadataHostsMappingCollector]
        ; Doesn't work with systemd-user service
        run = no
        [RaftTopologyRPCStatusCollector]
        ; Doesn't work with systemd-user service
        run = no
        [ScyllaClusterStatusCollector]
        ; Doesn't work with systemd-user service
        run = no
        [ScyllaClusterSchemaCollector]
        ; Doesn't work with systemd-user service
        run = no
        [KernelRingBufferCollector]
        ; Requires root privileges
        run = no
        [ScyllaLimitNOFILECollector]
        ; Requires root privileges
        run = no
        [ScyllaSystemConfigurationFilesCollector]
        ; Requires root installation with configs in /etc
        run = no
        [PerftuneYamlDefaultCollector]
        ; Depends on ScyllaSystemConfigurationFilesCollector
        run = no
        [PerftuneSystemConfigurationCollector]
        ; perftune script requires root
        run = no
        [ScyllaTablesCompressionInfoCollector]
        ; skip until https://scylladb.atlassian.net/browse/DOCTOR-19 is figured out
        run = no
        [ScyllaTablesUsedDiskCollector]
        ; skip until https://scylladb.atlassian.net/browse/DOCTOR-19 is figured out
        run = no
    """)

    def __init__(self, node: BaseNode, test_config: TestConfig, offline_install=False):
        self.node = node
        self.test_config = test_config
        self.offline_install = offline_install
        self.scylla_doctor_exec = "scylla-doctor"
        self.json_result_file = ""
        self.scylla_logs_file = ""
        self.python3_path = ""

    def run(self, sd_command):
        if not self.node.is_nonroot_install:
            result = self.node.remoter.sudo(sd_command, verbose=False)
        else:
            result = self.node.remoter.run(f"bash -lce '{self.python3_path} {sd_command}'", verbose=False)
        return result.stdout.strip()

    @cached_property
    def current_dir(self):
        result = self.node.remoter.run("pwd", verbose=False)
        return result.stdout.strip()

    @cached_property
    def version(self):
        version = self.run(f"{self.scylla_doctor_exec} --version")
        LOGGER.info("Scylla doctor version: %s", version)
        return version

    @cached_property
    def configured_version(self):
        """Get the configured scylla-doctor version from test config."""
        return self.test_config.tester_obj().params.get("scylla_doctor_version")

    def locate_scylla_doctor_package(self, version: str = None):
        """
        Locate scylla-doctor package in S3.

        Args:
            version: Specific version to locate (e.g., "1.9"). If None, returns the latest version.

        Returns:
            Package information dict from S3, or None if not found.
        """
        s3 = boto3.client("s3")
        packages = s3.list_objects(
            Bucket=self.SCYLLA_DOCTOR_OFFLINE_BUCKET_NAME, Prefix=self.SCYLLA_DOCTOR_OFFLINE_BUCKET_PREFIX, MaxKeys=5000
        )

        if not packages.get("Contents"):
            return None

        if version:
            # Look for specific version
            version_prefix = f"{self.SCYLLA_DOCTOR_OFFLINE_BUCKET_PREFIX}scylla-doctor-{version}"
            matching_packages = [pkg for pkg in packages["Contents"] if pkg["Key"].startswith(version_prefix)]
            if not matching_packages:
                LOGGER.warning("No scylla-doctor package found for version %s", version)
                return None
            # Return the latest modified package for this version
            return next(
                iter(sorted(matching_packages, key=lambda package: package["LastModified"], reverse=True)), None
            )
        else:
            # Return the latest version
            return next(
                iter(sorted(packages["Contents"], key=lambda package: package["LastModified"], reverse=True)), None
            )

    def download_scylla_doctor(self):
        if self.node.remoter.run("curl --version", ignore_status=True).ok:
            LOGGER.info("curl already installed, proceeding...")
        else:
            self.node.install_package("curl")

        if self.configured_version:
            LOGGER.info("Using configured scylla-doctor version: %s", self.configured_version)
            package = self.locate_scylla_doctor_package(version=self.configured_version)
        else:
            LOGGER.info("No scylla-doctor version configured, using latest available")
            package = self.locate_scylla_doctor_package()

        if not package:
            version_msg = f"version {self.configured_version}" if self.configured_version else "latest version"
            raise ScyllaDoctorException(f"Unable to find scylla-doctor package for {version_msg}")

        package_path = package["Key"]
        package_filename = package_path.split("/")[-1]
        LOGGER.info("Downloading %s...", package_filename)
        self.node.remoter.run(f"curl -JL {self.SCYLLA_DOCTOR_OFFLINE_DOWNLOAD_URI}{package_path} | tar -xvz")
        self.scylla_doctor_exec = f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_BIN}"

    def update_scylla_doctor_config(self, prefix: str, additional_config=""):
        with remote_file(self.node.remoter, f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_CONF}") as f:
            config = dedent(f"""
                [DefaultPaths]
                scylla_directory = {prefix}/scylladb
                scylla_directory_config = {prefix}/scylladb/etc/scylla
                scylla_directory_configs = {prefix}/scylladb/etc/scylla.d
                scylla_directory_var = {prefix}/scylladb

                {additional_config}
            """)
            LOGGER.info("Updating scylla-doctor-config file...\n%s", config)

            f.seek(0)
            f.truncate()
            f.write(config)
        self.scylla_doctor_exec += f" -cf {self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_CONF} "

    def install_scylla_doctor(self):
        if self.node.parent_cluster.cluster_backend == "docker":
            self.node.install_package("ethtool")
            self.node.install_package("tar")

        if self.offline_install or self.node.parent_cluster.cluster_backend == "docker":
            self.download_scylla_doctor()
            if self.node.is_nonroot_install:
                self.python3_path = self.find_local_python3_binary(self.current_dir)
                self.update_scylla_doctor_config(
                    self.current_dir, additional_config=self.SCYLLA_DOCTOR_DISABLED_OFFLINE_COLLECTORS
                )
        else:
            # Install via package manager (apt/yum/dnf) with configured version
            if self.configured_version:
                LOGGER.info("Installing scylla-doctor version %s via package manager", self.configured_version)
            else:
                LOGGER.info("Installing latest scylla-doctor via package manager")
            self.node.install_package("scylla-doctor", package_version=self.configured_version)

    def argus_collect_sd_package(self):
        try:
            sd_package = Package(name="scylla-doctor", date="", version=self.version, revision_id="", build_id="")
            LOGGER.info("Saving Scylla doctor package in Argus...")
            self.test_config.argus_client().submit_packages([sd_package])
        except Exception:
            LOGGER.error("Unable to collect Scylla Doctor package version for Argus - skipping...", exc_info=True)

    def find_local_python3_binary(self, user_home: str):
        if not (
            python3_path := self.node.remoter.run(
                f"ls {user_home}/scylladb/python3/bin/python3", verbose=False
            ).stdout.strip()
        ):
            python3_path = self.node.remoter.run(
                "for i in `find scylladb -name python3`;do [ -f $i ] && echo $i;done"
            ).stdout.strip()
        LOGGER.debug("Local python3 binary path: %s", python3_path)
        assert python3_path, f"Python3 binary is not found under local Scylla installation '{user_home}/scylladb'"
        return python3_path

    def run_scylla_doctor_and_collect_results(self):
        auth_options = ""
        if credentials := self.node.parent_cluster.get_db_auth():
            auth_options = "-sov CQL,user,{} -sov CQL,password,{} ".format(*credentials)

        json_name = f"{self.node.public_dns_name}.vitals.json"
        self.run(sd_command=f"{self.scylla_doctor_exec} {auth_options} --save-vitals {json_name}")

        # Search for json file
        result = self.node.remoter.run(f"ls {json_name}", verbose=False)
        self.json_result_file = result.stdout.strip()
        assert self.json_result_file, (
            f"Vitals result json file {json_name} has not been created. Scylla doctor version: {self.version}"
        )

        # Search for created scylla-logs tar.gz
        # Scylla Docker does not collect Scylla cluster logs - https://github.com/scylladb/field-engineering/issues/2288
        if self.node.parent_cluster.cluster_backend != "docker":
            result = self.node.remoter.run("ls scylla_logs_*.tar.gz", verbose=False)
            self.scylla_logs_file = result.stdout.strip()
            assert self.scylla_logs_file, (
                f"Scylla log archive {self.scylla_logs_file} has not been created. "
                f"Scylla doctor version: {self.version}"
            )

    def analyze_vitals(self):
        LOGGER.info("Analyze vitals")
        result = self.run(sd_command=f"{self.scylla_doctor_exec} --load-vitals {self.json_result_file} --verbose")
        LOGGER.debug(pprint.pformat(result))

    def filter_out_failed_collectors(self, collector):
        # FirewallRulesCollector return empty result - https://github.com/scylladb/field-engineering/issues/2248
        if collector == "FirewallRulesCollector":
            return True

        # https://github.com/scylladb/field-engineering/issues/2288
        if self.node.parent_cluster.cluster_backend == "docker" and collector in [
            "StorageConfigurationCollector",
            "PerftuneSystemConfigurationCollector",
        ]:
            return True

        if (
            self.node.distro.is_debian
            and self.offline_install
            and collector in ["RAIDSetupCollector", "SysctlCollector"]
        ):
            # Debian does not have mdstat by default and sysctl is not found
            return True

        # https://github.com/scylladb/scylladb/issues/18631
        # if self.node.distro.is_amazon2 and collector in ["CPUSetCollector", "PerftuneSystemConfigurationCollector"]:
        #    return True

        return False

    def analyze_and_verify_results(self):
        scylla_doctor_result = json.loads(self.node.remoter.sudo(f"cat {self.json_result_file}").stdout.strip())

        LOGGER.debug("Scylla-doctor output: %s", pprint.pformat(scylla_doctor_result))

        failed_collectors = {}
        for collector, value in scylla_doctor_result.items():
            # Status 0 - succeeded
            # Status 1 - failed
            # Status 2 - the collector cannot be proceeded, skipped
            if value["status"] == 1:
                if not self.filter_out_failed_collectors(collector=collector):
                    failed_collectors[collector] = value
        assert not failed_collectors, f"Failed collectors: {failed_collectors}. Scylla doctor version: {self.version}"
