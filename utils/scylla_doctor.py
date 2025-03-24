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
            result = self.node.remoter.run(f"{self.python3_path} {sd_command}", verbose=False)
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

    def locate_newest_scylla_doctor_package(self):
        s3 = boto3.client("s3")
        packages = s3.list_objects(Bucket=self.SCYLLA_DOCTOR_OFFLINE_BUCKET_NAME,
                                   Prefix=self.SCYLLA_DOCTOR_OFFLINE_BUCKET_PREFIX,
                                   MaxKeys=5000)
        latest = next(
            iter(sorted(packages["Contents"], key=lambda package: package["LastModified"], reverse=True)), None)
        return latest

    def download_scylla_doctor(self):
        if self.node.remoter.run("curl --version", ignore_status=True).ok:
            LOGGER.info("curl already installed, proceeding...")
        else:
            self.node.install_package('curl')
        latest_package = self.locate_newest_scylla_doctor_package()
        if not latest_package:
            raise ScyllaDoctorException("Unable to find latest scylla-doctor package for offline install")

        package_path = latest_package["Key"]
        package_filename = package_path.split("/")[-1]
        LOGGER.info("Downloading %s...", package_filename)
        self.node.remoter.run(f"curl -JL {self.SCYLLA_DOCTOR_OFFLINE_DOWNLOAD_URI}{package_path} | tar -xvz")
        self.scylla_doctor_exec = f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_BIN}"

    def update_scylla_doctor_config(self, prefix: str):
        with remote_file(self.node.remoter, f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_CONF}") as f:
            config = dedent(f"""
                [DefaultPaths]
                scylla_directory = {prefix}/scylladb
                scylla_directory_config = {prefix}/scylladb/etc/scylla
                scylla_directory_configs = {prefix}/scylladb/etc/scylla.d
                scylla_directory_var = {prefix}/scylladb
            """)
            LOGGER.info("Updating scylla-doctor-config file...\n%s", config)

            f.seek(0)
            f.truncate()
            f.write(config)
        self.scylla_doctor_exec += f" -cf {self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_CONF} "

    def install_scylla_doctor(self):
        if self.node.parent_cluster.cluster_backend == "docker":
            self.node.install_package('ethtool')
            self.node.install_package('tar')

        if self.offline_install or self.node.parent_cluster.cluster_backend == "docker":
            self.download_scylla_doctor()
            if self.node.is_nonroot_install:
                self.python3_path = self.find_local_python3_binary(self.current_dir)
                self.update_scylla_doctor_config(self.current_dir)
        else:
            self.node.install_package('scylla-doctor')

    def argus_collect_sd_package(self):
        try:
            sd_package = Package(name="scylla-doctor", date="", version=self.version, revision_id="", build_id="")
            LOGGER.info("Saving Scylla doctor package in Argus...")
            self.test_config.argus_client().submit_packages([sd_package])
        except Exception:  # pylint: disable=broad-except
            LOGGER.error("Unable to collect Scylla Doctor package version for Argus - skipping...", exc_info=True)

    def find_local_python3_binary(self, user_home: str):
        if not (python3_path := self.node.remoter.run(f"ls {user_home}/scylladb/python3/bin/python3", verbose=False).stdout.strip()):
            python3_path = self.node.remoter.run(
                "for i in `find scylladb -name python3`;do [ -f $i ] && echo $i;done").stdout.strip()
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
        assert self.json_result_file, (f"Vitals result json file {json_name} has not been created. "
                                       f"Scylla doctor version: {self.version}")

        # Search for created scylla-logs tar.gz
        # Scylla Docker does not collect Scylla cluster logs - https://github.com/scylladb/field-engineering/issues/2288
        if self.node.parent_cluster.cluster_backend != "docker":
            result = self.node.remoter.run("ls scylla_logs_*.tar.gz", verbose=False)
            self.scylla_logs_file = result.stdout.strip()
            assert self.scylla_logs_file, (f"Scylla log archive {self.scylla_logs_file} has not been created. "
                                           f"Scylla doctor version: {self.version}")

    def analyze_vitals(self):
        LOGGER.info("Analyze vitals")
        result = self.run(sd_command=f"{self.scylla_doctor_exec} --load-vitals {self.json_result_file} --verbose")
        LOGGER.debug(pprint.pformat(result))

    def filter_out_failed_collectors(self, collector):
        # FirewallRulesCollector return empty result - https://github.com/scylladb/field-engineering/issues/2248
        if collector == "FirewallRulesCollector":
            return True

        # https://github.com/scylladb/field-engineering/issues/2288
        if (self.node.parent_cluster.cluster_backend == "docker" and
                collector in ["StorageConfigurationCollector", "PerftuneSystemConfigurationCollector"]):
            return True

        # https://github.com/scylladb/scylladb/issues/18631
        if self.node.distro.is_amazon2 and collector in ["CPUSetCollector", "PerftuneSystemConfigurationCollector"]:
            return True

        return False

    def analyze_and_verify_results(self):
        scylla_doctor_result = json.loads(self.node.remoter.run(f"cat {self.json_result_file}").stdout.strip())

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
