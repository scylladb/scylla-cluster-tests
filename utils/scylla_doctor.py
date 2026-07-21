#!/usr/bin/env python
import base64
import json
import logging
import pprint
import re
from functools import cached_property
from textwrap import dedent
from urllib.parse import urlparse

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
    SCYLLA_DOCTOR_ANALYZER_CONFIG = dedent("""
        [RAMAnalyzer]
        ; change recommended ram value for this analyzer as it is not relevant for the test and should not cause test failures
        ram_recommended_total = 7340032
        ram_recommended_per_lcore = 3145728
        [SwapAnalyzer]
        ; change Recommended RAM to swap ratio for this analyzer as it is not relevant for the test and should not cause test failures
        ram_swap_ratio = 3.5
        [StorageRAMRatioAnalyzer]
        ; Maximum recommended Disk to RAM ratio
        ratio = 120
        [ScyllaServicesAnalyzer]
        ; disable this analyzer
        run = no
        [CPUScalingAnalyzer]
        ; @Vladislav Zolotarov request
        run = no
        [RsyslogAnalyzer]
        ; do not need to configure it, SD check that /etc/rsyslog.d/scylla.conf file exists only
        run = no
        [ScyllaInternodeCompressionAnalyzer]
        ; not relevant for this test
        run = no
        [ScyllaSnitchAnalyzer]
        ; It's default snitch, not relevant to change for this test. Scylla issue https://github.com/scylladb/scylladb/issues/2370
        run = no
        [ScyllaUpdateAnalyzer]
        ; not relevant for this test
        run = no
        [ScyllaClusterSystemKeyspacesReplicationAnalyzer]
        ; in the production the NetworkTopologyStrategy has to be configured, skip for now with Switch internal distributed keyspaces from SimpleStrategy to NetworkTopologyStrategy / EverywhereStrategy / Raft Group 0  · Issue #1796 · scylladb/scylladb
        run = no
        [ScyllaConfigurationConsistencyAnalyzer]
        ; skip
        skip_source_validation_keys = abort_on_ebadf, abort_on_internal_error, abort_on_lsa_bad_alloc, enable_sstable_key_validation
        skip_persisted_validation_keys = kms_hosts
    """)

    def __init__(self, node: BaseNode, test_config: TestConfig, offline_install=False):
        self.node = node
        self.test_config = test_config
        self.offline_install = offline_install
        self.scylla_doctor_exec = "scylla-doctor"
        self.json_result_file = ""
        self.scylla_logs_file = ""
        self.analysis_report_file = ""
        self.python3_path = ""
        self._full_edition_downloaded = False

    @property
    def is_full_edition(self):
        """Check whether the full (enterprise) edition of Scylla Doctor is actually installed.

        Returns True only when the edition is configured as ``"full"`` AND the
        full edition binary was successfully downloaded.  If the download failed
        and fell back to basic, this returns False — preventing the analysis
        phase from running with a basic edition binary that cannot produce the
        expected JSON output.
        """
        return self.configured_edition == "full" and self._full_edition_downloaded

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
        # Use a dedicated writable directory instead of relying on pwd,
        # which may point to a non-writable location in Docker containers.
        sd_dir = "/tmp/scylla_doctor"
        self.node.remoter.run(f"mkdir -p {sd_dir}", verbose=False)
        return sd_dir

    @cached_property
    def scylla_install_home(self) -> str:
        """Return the user's home directory where Scylla is installed.

        For nonroot offline installs Scylla lives under ``$HOME/scylladb``.
        This property returns ``$HOME`` so that searches for bundled binaries
        and scylla-doctor config paths point at the correct location, rather
        than the temporary working directory used for tarball extraction.
        """
        return self.node.remoter.run("echo $HOME", verbose=False).stdout.strip()

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
    def _parse_s3_url(url: str, default_bucket: str) -> tuple[str, str]:
        """Parse an S3 reference into ``(bucket_name, object_key)``.

        Supported formats:
        - ``https://s3.amazonaws.com/BUCKET/KEY`` — path-style URL.
        - ``https://s3.REGION.amazonaws.com/BUCKET/KEY`` — path-style with region.
        - ``https://BUCKET.s3.amazonaws.com/KEY`` — virtual-hosted style.
        - ``https://BUCKET.s3.REGION.amazonaws.com/KEY`` — virtual-hosted with region.
        - ``https://BUCKET/KEY`` — bucket name extracted from host,
          key from the path (legacy/simple format).
        - ``prefix/path/file.tar.gz`` (bare S3 key, no scheme) — uses
          *default_bucket*.

        Args:
            url: The URL or key path to parse.
            default_bucket: Bucket name to use when *url* is a bare key.

        Returns:
            Tuple of (bucket_name, object_key).
        """
        parsed = urlparse(url)

        # No scheme → treat as a bare S3 key
        if not parsed.scheme:
            return default_bucket, url

        host = parsed.hostname or ""
        path = parsed.path.lstrip("/")

        # Path-style: https://s3.amazonaws.com/BUCKET/KEY
        #          or https://s3.REGION.amazonaws.com/BUCKET/KEY
        if re.match(r"s3([.-][a-z0-9-]+)?\.amazonaws\.com$", host):
            # First path segment is the bucket, rest is the key
            parts = path.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
            return bucket, key

        # Virtual-hosted style: https://BUCKET.s3.amazonaws.com/KEY
        #                     or https://BUCKET.s3.REGION.amazonaws.com/KEY
        match = re.match(r"(.+)\.s3([.-][a-z0-9-]+)?\.amazonaws\.com$", host)
        if match:
            return match.group(1), path

        # Fallback: treat host as bucket name, path as key
        return host, path

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
            LOGGER.info("Extracted region from bucket name suffix")
            return region

        try:
            s3 = boto3.client("s3")
            location = s3.get_bucket_location(Bucket=bucket_name)
            # get_bucket_location returns None for us-east-1
            return location.get("LocationConstraint") or "us-east-1"
        except Exception:  # noqa: BLE001
            LOGGER.warning("Could not determine region for bucket, defaulting to us-east-1")
            return "us-east-1"

    def _download_and_extract_tarball(self, url: str, description: str = "scylla-doctor"):
        """Download a tarball from *url* and extract it on the remote node.

        Downloads to a temporary file first so that HTTP errors (e.g. S3
        returning a short XML error page) are detected before ``tar`` runs.

        Extraction always targets ``self.current_dir`` via ``-C`` so that
        it works even when the remoter's working directory is not writable
        (common in Docker containers).

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
        # --no-same-owner: avoid chown failures in Docker containers running as non-root
        # --no-same-permissions: avoid permission restore issues in restricted environments
        extract_result = self.node.remoter.run(
            f"tar -xzf {tmp_tarball} -C {self.current_dir} --no-same-owner --no-same-permissions 2>&1",
            ignore_status=True,
        )
        if extract_result.ok:
            self.node.remoter.run(f"rm -f {tmp_tarball}", verbose=False, ignore_status=True)
        else:
            stderr_output = extract_result.stdout.strip() or extract_result.stderr.strip()
            self.node.remoter.run(f"rm -f {tmp_tarball}", verbose=False, ignore_status=True)
            raise ScyllaDoctorException(
                f"Failed to extract {description} tarball (exit code {extract_result.exited}): {stderr_output}"
            )

    def download_full_scylla_doctor(self):
        """Download the full scylla-doctor edition using an S3 pre-signed URL.

        Generates a pre-signed URL on the SCT runner side (which has AWS credentials)
        and passes it to curl on the remote node, avoiding the need for AWS credentials on the node.

        If ``scylla_doctor_full_tarball_url`` is configured, it is used directly
        instead of locating the package in S3.
        """
        if self.node.remoter.run("curl --version", ignore_status=True).ok:
            LOGGER.info("curl already installed, proceeding...")
        else:
            self.node.install_package("curl")

        full_tarball_url = self.test_config.tester_obj().params.get("scylla_doctor_full_tarball_url")

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

        if full_tarball_url:
            custom_bucket, custom_key = self._parse_s3_url(full_tarball_url, default_bucket=bucket_name)
            if custom_bucket != bucket_name:
                bucket_name = custom_bucket
                LOGGER.info("Overriding bucket from full_tarball_url")
            package["Key"] = custom_key
            LOGGER.info("Using custom full SD tarball from configured URL")

        package_key = package["Key"]
        package_filename = package_key.split("/")[-1]
        LOGGER.info("Downloading full scylla-doctor package...")

        # Generate a short-lived pre-signed URL (300s) so the remote node can download without AWS creds.
        # IMPORTANT: The S3 client MUST be created with the bucket's actual region.
        # Pre-signed URLs are signed with a specific region; if the signing region
        # doesn't match the bucket's region, S3 returns a small XML error response
        # (e.g. SignatureDoesNotMatch / AccessDenied) instead of the file.
        bucket_region = self._get_bucket_region(bucket_name)
        LOGGER.info("Resolved bucket region")
        s3 = boto3.client("s3", region_name=bucket_region)
        download_url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket_name, "Key": package_key},
            ExpiresIn=300,
        )

        self._download_and_extract_tarball(download_url, description=f"full scylla-doctor ({package_filename})")
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
            # Only override DefaultPaths for nonroot installs where Scylla
            # lives under $HOME/scylladb instead of system paths.
            if self.node.is_nonroot_install:
                default_paths = dedent(f"""
                    [DefaultPaths]
                    scylla_directory = {prefix}/scylladb
                    scylla_directory_config = {prefix}/scylladb/etc/scylla
                    scylla_directory_configs = {prefix}/scylladb/etc/scylla.d
                    scylla_directory_var = {prefix}/scylladb
                """)
            else:
                default_paths = ""

            config = f"{default_paths}\n{additional_config}\n"
            LOGGER.info("Updating scylla-doctor-config file...\n%s", config)

            f.seek(0)
            f.truncate()
            f.write(config)
        self.scylla_doctor_exec += f" -cf {self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_CONF} "

    def install_scylla_doctor(self):
        if self.node.parent_cluster.cluster_backend == "docker":
            self.node.install_package("ethtool")
            self.node.install_package("tar")
            self.node.install_package("gzip")
            self.node.install_package("file")

        # Always download from S3 — package repos are not updated at the same
        # time as S3 releases, and specific versions may not be available via repo.
        self.download_scylla_doctor()
        self.python3_path = self.find_scylla_bundled_python3(self.scylla_install_home)

        additional_config = ""
        if self.node.is_nonroot_install:
            additional_config += self.SCYLLA_DOCTOR_DISABLED_OFFLINE_COLLECTORS
        if self.is_full_edition:
            additional_config += self.SCYLLA_DOCTOR_ANALYZER_CONFIG

        if additional_config:
            self.update_scylla_doctor_config(self.scylla_install_home, additional_config=additional_config)

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

    def find_scylla_bundled_python3(self, user_home: str) -> str:
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
            Path to the bundled python3 binary.

        Raises:
            ScyllaDoctorException: If Scylla-bundled python3 is not found.
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
            # Root install: broad filesystem search under standard Scylla install locations
            python3_path = self.node.remoter.sudo(
                "find /opt/scylladb /usr/lib/scylladb 2>/dev/null"
                " -path '*/bin/python3' \\( -type f -o -type l \\) | head -1",
                verbose=False,
                ignore_status=True,
            ).stdout.strip()
            LOGGER.debug("Root bundled python3 search result: %s", python3_path or "(not found)")
        if python3_path:
            LOGGER.info("Using Scylla-bundled python3 at %s", python3_path)
            return python3_path
        raise ScyllaDoctorException(
            f"No Scylla-bundled python3 found on {self.node.name}. "
            f"Scylla doctor requires the python3 executable bundled with the Scylla installation."
        )

    def _ensure_lspci(self):
        """Install pciutils (provides lspci) if not already present."""
        result = self.node.remoter.sudo("which lspci", ignore_status=True, verbose=False)
        if result.ok:
            LOGGER.info("lspci already installed, proceeding...")
            return
        LOGGER.info("lspci not found, installing pciutils...")
        self.node.install_package("pciutils")

    def _ensure_iptables(self):
        """Install and start iptables service before SD Analyzer run."""
        self.node.install_package("iptables")
        self.node.start_service("iptables", ignore_status=True)

    def run_scylla_doctor_and_collect_results(self):
        self._ensure_lspci()
        self._ensure_iptables()

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

    def run_analysis_phase(self):
        """Run the analysis phase of Scylla Doctor (full edition only).

        Loads previously collected vitals and runs analyzers to produce
        findings and recommendations about the cluster health.

        Uses ``--output json`` to get the analysis report in JSON format
        from stdout, then saves it to a file for later verification.
        """
        if not self.is_full_edition:
            LOGGER.info("Skipping analysis phase — only available for the full edition")
            return

        if not self.json_result_file:
            LOGGER.warning("Cannot run analysis phase: no vitals file collected")
            return

        json_report_name = f"{self.node.public_dns_name}.analysis.json"
        LOGGER.info("Running Scylla Doctor analysis phase (full edition)...")

        sd_cmd = f"{self.scylla_doctor_exec} --load-vitals {self.json_result_file} --output json"
        LOGGER.info("Run scylla-doctor command: %s", sd_cmd)
        stdout_content = self.run(sd_command=sd_cmd)

        # Save the JSON output from stdout to a file
        if not stdout_content:
            raise ScyllaDoctorException(f"Analysis phase produced no output. Scylla doctor version: {self.version}")
        # Use base64 to safely write JSON that may contain special characters
        encoded = base64.b64encode(stdout_content.encode()).decode()
        self.node.remoter.sudo(f"bash -c 'echo {encoded} | base64 -d > {json_report_name}'")

        # Verify the JSON report was created
        verify_result = self.node.remoter.run(
            f"test -s {json_report_name} && echo {json_report_name}", verbose=False, ignore_status=True
        )
        self.analysis_report_file = verify_result.stdout.strip() if verify_result.ok else ""
        if not self.analysis_report_file:
            raise ScyllaDoctorException(
                f"Analysis report file {json_report_name} has not been created. Scylla doctor version: {self.version}"
            )
        LOGGER.info("Analysis JSON report saved to: %s", self.analysis_report_file)

    @staticmethod
    def _extract_json_from_output(content: str) -> dict:
        """Extract a JSON object from content that may contain non-JSON text.

        Scylla-doctor may prepend human-readable text (ASCII banner, progress
        markers) before the actual JSON output.  This method first tries direct
        parsing; if that fails it locates the first ``{`` and uses
        ``raw_decode`` to parse the JSON object starting there.

        Args:
            content: Raw output that should contain a JSON object.

        Returns:
            Parsed JSON as a dict.

        Raises:
            ScyllaDoctorException: If no valid JSON object can be found.
        """
        content = content.strip()
        if not content:
            raise ScyllaDoctorException("Analysis output is empty — nothing to parse")

        # Fast path: content is pure JSON
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            pass

        # Slow path: look for JSON embedded in non-JSON output
        idx = content.find("{")
        if idx == -1:
            raise ScyllaDoctorException(f"No JSON object found in analysis output (first 500 chars): {content[:500]}")

        decoder = json.JSONDecoder()
        try:
            obj, _ = decoder.raw_decode(content, idx)
            return obj
        except json.JSONDecodeError as exc:
            raise ScyllaDoctorException(
                f"Failed to parse JSON from analysis output at position {idx} (first 500 chars): {content[:500]}"
            ) from exc

    def analyze_vitals_report_known_issues(self, analyzer, value):
        known_issue = None
        if (
            analyzer == "DriverVersionAnalyzer"
            and "API call error occurred to retrieve the minimum or latest driver version" in value["message"]
        ):
            known_issue = "https://scylladb.atlassian.net/browse/SCT-420"

        elif (
            analyzer == "NodeInstanceTypeAnalyzer"
            and "'n2-standard-2' instance type is not listed as recommended" in value["message"]
        ):
            known_issue = "https://scylladb.atlassian.net/browse/SCT-415"

        elif (
            analyzer == "NICsAnalyzer"
            and "EC2 instance class does not support enhanced networking." in value["message"]
        ):
            known_issue = "https://scylladb.atlassian.net/browse/SCT-422"

        elif (
            analyzer == "NodeInstanceTypeAnalyzer"
            and "'im4gn.xlarge' instance type is not listed as recommended" in value["message"]
        ):
            known_issue = "https://scylladb.atlassian.net/browse/SCT-421"

        elif analyzer == "ScyllaSSTablesAnalyzer" and "SSTable format is not 'me'" in value["message"]:
            known_issue = "https://scylladb.atlassian.net/browse/SCT-455"

        return known_issue

    def analyze_and_verify_analysis_results(self):
        """Parse the JSON analysis report and verify that no analyzer failed.

        The analysis report is a JSON file produced by ``run_analysis_phase()``
        (parsed from scylla-doctor's human-readable output).  Each top-level
        key is an analyzer name mapped to a dict with at least a ``status``
            PASSED = 0
            SKIPPED = 1
            WARNING = 2
            FAILED = 3
        """
        if not self.analysis_report_file:
            LOGGER.warning("No analysis report file to verify")
            return

        raw_content = self.node.remoter.sudo(f"cat {self.analysis_report_file}").stdout.strip()
        analysis_result = self._extract_json_from_output(raw_content)
        LOGGER.info("Loaded %d analyzer results from JSON report", len(analysis_result))

        LOGGER.debug("Scylla-doctor analysis output: %s", pprint.pformat(analysis_result))

        failed_analyzers = {}
        for analyzer, value in analysis_result.items():
            LOGGER.info("Analyzer %s status: %s (%s)", analyzer, value.get("status"), value)
            if isinstance(value, dict) and value.get("status") not in [0, 1]:  # PASSED, SKIPPED
                # TODO: temporary workaround. Will be fixed in SD and then this if block should be removed.
                #  See https://scylladb.atlassian.net/browse/SCT-115
                if known_issue := self.analyze_vitals_report_known_issues(analyzer, value):
                    value["known issue"] = known_issue
                failed_analyzers[analyzer] = value

        LOGGER.info("Failed analyzers: %s", pprint.pformat(failed_analyzers))

        assert not failed_analyzers, (
            f"Failed analyzers: {pprint.pformat(failed_analyzers)}. Scylla doctor version: {self.version}"
        )

    def filter_out_failed_collectors(self, collector):
        # FirewallRulesCollector return empty result - https://github.com/scylladb/field-engineering/issues/2248
        if collector == "FirewallRulesCollector":
            return True

        # https://github.com/scylladb/field-engineering/issues/2288
        # Docker containers lack many OS-level utilities (ip, iptables, ss, dmesg, timedatectl)
        # and don't have access to /proc/interrupts or cloud metadata endpoints.
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
