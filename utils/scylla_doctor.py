#!/usr/bin/env python
import json
import logging
import pprint
import re
from functools import cached_property
from textwrap import dedent

import boto3

from argus.client.sct.types import Package

from sdcm.cluster import BaseNode
from sdcm.keystore import KeyStore
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
        if self.python3_path:
            sd_command = f"{self.python3_path} {sd_command}"
        if not self.node.is_nonroot_install:
            result = self.node.remoter.sudo(sd_command, verbose=False)
        else:
            result = self.node.remoter.run(f"bash -lce '{sd_command}'", verbose=False)
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

    @cached_property
    def configured_edition(self):
        return self.test_config.tester_obj().params.get("scylla_doctor_edition")

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

    def locate_full_scylla_doctor_package(self, version: str = None):
        """Locate scylla-doctor package in the full edition S3 bucket.

        The bucket name and prefix are retrieved from the keystore.

        Args:
            version: Specific version to locate. If None, returns the latest version.

        Returns:
            Tuple of (package metadata dict, bucket_name) or (None, None) if not found.
        """
        ks = KeyStore()
        config = ks.get_scylla_doctor_full_bucket_config()
        bucket_name = config["bucket"]
        prefix = config["prefix"]

        s3 = boto3.client("s3")
        packages = s3.list_objects(Bucket=bucket_name, Prefix=prefix, MaxKeys=5000)

        if not packages.get("Contents"):
            return None, None

        if version:
            version_prefix = f"{prefix}scylla-doctor-{version}"
            matching = [p for p in packages["Contents"] if p["Key"].startswith(version_prefix)]
            if not matching:
                LOGGER.warning("No full scylla-doctor package found for version %s", version)
                return None, None
            package = next(iter(sorted(matching, key=lambda p: p["LastModified"], reverse=True)), None)
        else:
            package = next(iter(sorted(packages["Contents"], key=lambda p: p["LastModified"], reverse=True)), None)

        return package, bucket_name

    @staticmethod
    def _get_bucket_region(bucket_name: str) -> str:
        """Determine the AWS region of an S3 bucket.

        Pre-signed URLs must be signed with the bucket's actual region;
        using the wrong region causes S3 to return a small XML error
        (``SignatureDoesNotMatch`` / ``AccessDenied``) instead of the file.

        Strategy:
        1. Extract region from the bucket name (e.g.
           ``fe-artifacts-297607762119-eu-central-1`` → ``eu-central-1``).
        2. Fall back to ``get_bucket_location`` API (requires
           ``s3:GetBucketLocation`` permission).
        3. Fall back to ``us-east-1`` as last resort.
        """
        # Pattern matches standard AWS region names at the end of the bucket name
        match = re.search(r"((?:us|eu|ap|sa|ca|me|af|il)-[a-z]+-\d+)$", bucket_name)
        if match:
            region = match.group(1)
            LOGGER.info("Extracted region %s from bucket name %s", region, bucket_name)
            return region

        try:
            s3 = boto3.client("s3")
            location = s3.get_bucket_location(Bucket=bucket_name)
            # get_bucket_location returns None for us-east-1
            return location.get("LocationConstraint") or "us-east-1"
        except Exception:  # noqa: BLE001
            LOGGER.warning("Could not determine region for bucket %s, defaulting to us-east-1", bucket_name)
            return "us-east-1"

    def _download_and_extract_tarball(self, url: str, description: str = "scylla-doctor"):
        """Download a tarball from *url* and extract it on the remote node.

        Downloads to a temporary file first so that HTTP errors (e.g. S3
        returning a short XML error page) are detected before ``tar`` runs.

        Args:
            url: Full URL (may be a pre-signed S3 URL).
            description: Human-readable label for log messages.

        Raises:
            Exception: Propagated from ``remoter.run`` on download or
                extraction failure.
        """
        tmp_tarball = "/tmp/scylla_doctor_download.tar.gz"
        # -f/--fail  → exit code 22 on HTTP 4xx/5xx instead of saving the error page
        # -S/--show-error → print error message even when -f is used
        # -L/--location  → follow redirects
        self.node.remoter.run(
            f"curl -fSL -o {tmp_tarball} '{url}'",
        )
        # Sanity-check: a valid tarball is at least a few KB
        check = self.node.remoter.run(
            f"test -s {tmp_tarball} && file {tmp_tarball}",
            verbose=False,
            ignore_status=True,
        )
        if not check.ok or "gzip" not in check.stdout.lower():
            body_head = self.node.remoter.run(
                f"head -c 500 {tmp_tarball}",
                verbose=False,
                ignore_status=True,
            ).stdout.strip()
            self.node.remoter.run(f"rm -f {tmp_tarball}", verbose=False, ignore_status=True)
            raise ScyllaDoctorException(
                f"Downloaded {description} file is not a valid gzip tarball. First 500 bytes of response:\n{body_head}"
            )
        LOGGER.info("Extracting %s tarball...", description)
        self.node.remoter.run(f"tar -xvzf {tmp_tarball} && rm -f {tmp_tarball}")

    def download_full_scylla_doctor(self):
        """Download the full scylla-doctor edition using an S3 pre-signed URL.

        Generates a pre-signed URL on the SCT runner side (which has AWS credentials)
        and passes it to curl on the remote node, avoiding the need for AWS credentials on the node.
        """
        if self.node.remoter.run("curl --version", ignore_status=True).ok:
            LOGGER.info("curl already installed, proceeding...")
        else:
            self.node.install_package("curl")

        version = self.configured_version
        if version:
            LOGGER.info("Locating full scylla-doctor version: %s", version)
            package, bucket_name = self.locate_full_scylla_doctor_package(version=version)
        else:
            LOGGER.info("Locating latest full scylla-doctor")
            package, bucket_name = self.locate_full_scylla_doctor_package()

        if not package:
            version_msg = f"version {version}" if version else "latest version"
            raise ScyllaDoctorException(f"Unable to find full scylla-doctor package for {version_msg}")

        package_key = package["Key"]
        package_filename = package_key.split("/")[-1]
        LOGGER.info("Downloading full scylla-doctor package %s from bucket %s...", package_filename, bucket_name)

        # Generate a short-lived pre-signed URL (300s) so the remote node can download without AWS creds.
        # IMPORTANT: The S3 client MUST be created with the bucket's actual region.
        # Pre-signed URLs are signed with a specific region; if the signing region
        # doesn't match the bucket's region, S3 returns a small XML error response
        # (e.g. SignatureDoesNotMatch / AccessDenied) instead of the file.
        bucket_region = self._get_bucket_region(bucket_name)
        LOGGER.info("Bucket %s is in region %s", bucket_name, bucket_region)
        s3 = boto3.client("s3", region_name=bucket_region)
        presigned_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": package_key},
            ExpiresIn=300,
        )

        self._download_and_extract_tarball(presigned_url, description=f"full scylla-doctor ({package_filename})")
        self.scylla_doctor_exec = f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_BIN}"
        self._full_edition_downloaded = True

    def download_scylla_doctor(self):
        if self.configured_edition == "full":
            self.download_full_scylla_doctor()
            return

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
        self._download_and_extract_tarball(
            f"{self.SCYLLA_DOCTOR_OFFLINE_DOWNLOAD_URI}{package_path}",
            description=f"scylla-doctor ({package_filename})",
        )
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

        # Always download from S3 — package repos are not updated at the same
        # time as S3 releases, and specific versions may not be available via repo.
        self.download_scylla_doctor()
        self.python3_path = self.find_local_python3_binary(self.current_dir)
        if self.node.is_nonroot_install:
            self.update_scylla_doctor_config(
                self.current_dir, additional_config=self.SCYLLA_DOCTOR_DISABLED_OFFLINE_COLLECTORS
            )

        # TODO: optionally install via package manager (apt/yum/dnf) instead of S3
        #  download. Needs an SCT configuration toggle to enable this path.
        # if self.configured_version:
        #     LOGGER.info("Installing scylla-doctor version %s via package manager", self.configured_version)
        # else:
        #     LOGGER.info("Installing latest scylla-doctor via package manager")
        # self.node.install_package("scylla-doctor", package_version=self.configured_version)

    def argus_collect_sd_package(self):
        try:
            sd_package = Package(name="scylla-doctor", date="", version=self.version, revision_id="", build_id="")
            LOGGER.info("Saving Scylla doctor package in Argus...")
            self.test_config.argus_client().submit_packages([sd_package])
        except Exception:
            LOGGER.error("Unable to collect Scylla Doctor package version for Argus - skipping...", exc_info=True)

    @staticmethod
    def _check_system_python3(node) -> str | None:
        """Check if the system python3 is available and version > 3.9.

        Returns:
            The path to the system python3 binary if suitable, or None.
        """
        result = node.remoter.run(
            "python3 -c 'import sys; print(sys.version_info.major, sys.version_info.minor)'",
            verbose=False,
            ignore_status=True,
        )
        if not result.ok:
            LOGGER.debug("System python3 not available")
            return None

        try:
            major, minor = result.stdout.strip().split()
            if int(major) >= 3 and int(minor) > 9:
                python3_path = node.remoter.run("which python3", verbose=False, ignore_status=True).stdout.strip()
                if python3_path:
                    LOGGER.info("Using system python3 (%s.%s) at %s", major, minor, python3_path)
                    return python3_path
        except (ValueError, IndexError):
            LOGGER.debug("Could not parse system python3 version from: %s", result.stdout.strip())

        return None

    def _find_python3_via_package_manager(self) -> str | None:
        """Query the OS package manager for the scylla-python3 package files.

        Tries ``rpm -ql`` (for RPM-based distros) and ``dpkg -L`` (for
        Debian-based distros) to locate the ``python3`` binary shipped by
        the ``scylla-python3`` package, avoiding hardcoded install paths.

        Returns:
            Path to the bundled python3 binary, or None if not found.
        """
        for query_cmd in (
            "rpm -ql scylla-python3 2>/dev/null | grep '/bin/python3$' | head -1",
            "dpkg -L scylla-python3 2>/dev/null | grep '/bin/python3$' | head -1",
        ):
            result = self.node.remoter.sudo(query_cmd, verbose=False, ignore_status=True)
            python3_path = result.stdout.strip()
            if python3_path:
                LOGGER.debug("Found scylla-python3 via package manager: %s", python3_path)
                return python3_path
        LOGGER.debug("scylla-python3 package not found via package manager")
        return None

    def _find_scylla_bundled_python3(self, user_home: str) -> str | None:
        """Find the python3 binary bundled with the Scylla installation.

        For nonroot installs the binary may live under a versioned directory,
        e.g. ``{user_home}/scylla-2026.2.0~dev/scylla-python3/bin/python3``,
        so we search broadly under *user_home* for any ``python3`` inside a
        ``bin/`` directory whose path contains "scylla".

        For root installs the binary is typically under ``/opt/scylladb`` and
        may require elevated privileges to locate, so ``remoter.sudo`` is used.

        Args:
            user_home: Home directory where Scylla is installed.

        Returns:
            Path to the bundled python3 binary, or None if not found.
        """
        if self.node.is_nonroot_install:
            # Fast path: check the traditional location first
            python3_path = self.node.remoter.run(
                f"ls {user_home}/scylladb/python3/bin/python3", verbose=False, ignore_status=True
            ).stdout.strip()
            if not python3_path:
                # Broad search: covers versioned dirs like scylla-VERSION/scylla-python3/bin/python3
                python3_path = self.node.remoter.run(
                    f"find {user_home} -path '*/bin/python3' \\( -type f -o -type l \\) 2>/dev/null"
                    " | grep -i scylla | head -1",
                    verbose=False,
                    ignore_status=True,
                ).stdout.strip()
            LOGGER.debug("Nonroot bundled python3 search result: %s", python3_path or "(not found)")
        else:
            # Root install: query the package manager for scylla-python3 files
            python3_path = self._find_python3_via_package_manager()
            if not python3_path:
                # Fallback: broad filesystem search under standard Scylla install locations
                python3_path = self.node.remoter.sudo(
                    "find /opt/scylladb /usr/lib/scylladb 2>/dev/null"
                    " -path '*/bin/python3' \\( -type f -o -type l \\) | head -1",
                    verbose=False,
                    ignore_status=True,
                ).stdout.strip()
            LOGGER.debug("Root bundled python3 search result: %s", python3_path or "(not found)")
        if not python3_path:
            return None

        # Verify the bundled python3 is > 3.9
        check_cmd = f"{python3_path} -c 'import sys; print(sys.version_info.major, sys.version_info.minor)'"
        if self.node.is_nonroot_install:
            result = self.node.remoter.run(check_cmd, verbose=False, ignore_status=True)
        else:
            result = self.node.remoter.sudo(check_cmd, verbose=False, ignore_status=True)
        if result.ok:
            try:
                major, minor = result.stdout.strip().split()
                if int(major) >= 3 and int(minor) > 9:
                    LOGGER.info("Using Scylla-bundled python3 (%s.%s) at %s", major, minor, python3_path)
                    return python3_path
                LOGGER.warning("Scylla-bundled python3 at %s is %s.%s (<= 3.9), skipping", python3_path, major, minor)
            except (ValueError, IndexError):
                LOGGER.debug("Could not parse version from bundled python3 at %s", python3_path)
        else:
            LOGGER.debug("Could not check version of bundled python3 at %s", python3_path)

        return None

    def find_local_python3_binary(self, user_home: str):
        """Find a suitable python3 binary for running scylla-doctor.

        Strategy:
        1. Check if the system python3 is > 3.9 — use it if so.
        2. Fall back to the python3 bundled with the Scylla installation.

        Args:
            user_home: Home directory where Scylla is installed.

        Returns:
            Path to a suitable python3 binary.

        Raises:
            AssertionError: If no suitable python3 binary is found.
        """
        python3_path = self._check_system_python3(self.node)
        if not python3_path:
            LOGGER.debug("System python3 not suitable, looking for Scylla-bundled python3...")
            python3_path = self._find_scylla_bundled_python3(user_home)

        LOGGER.debug("Local python3 binary path: %s", python3_path)
        if not python3_path:
            raise ScyllaDoctorException(
                f"No suitable python3 (> 3.9) found on {self.node.name}: "
                f"system python3 is missing or <= 3.9, "
                f"and no Scylla-bundled python3 found via package manager or filesystem search"
            )
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
