#!/usr/bin/env python3
import logging
import os
import re
import sys

from sdcm.utils.parallel_object import ParallelObjectException
from sdcm.utils.session import create_retry_session
from sdcm.utils.version_utils import (
    ComparableScyllaVersion,
    get_all_versions,
    get_branch_version,
    get_s3_scylla_repos_mapping,
)

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


LOGGER = logging.getLogger(__name__)


# If new support distro shared repo with others, we need to assign the start support versions. eg: centos8
start_support_versions = {
    "centos-8": "2021.1",
    "centos-9": "2024.1",
    "rocky-10": "2025.3",
}
start_support_backend = {
    "azure": "2023.1",
    # first release with an OCI image
    "oci": "2026.2",
}

# list of versions that are available, but aren't supported, and we shouldn't test upgrades from
unsupported_versions = [
    "5.3",
    # images of this release have a baked-in unstable repo snapshot which was already pruned from S3,
    # so the rollback step of the rolling upgrade test can't reinstall it back (SCYLLADB-3508)
    "2025.2",
]


class UpgradeBaseVersion:
    start_support_version = None

    def __init__(
        self,
        scylla_repo: str,
        linux_distro: str,
        scylla_version: str = None,
        base_version_all_sts_versions: bool = False,
    ):
        if len(linux_distro.split("-")) > 1:
            self.dist_type, *_, self.dist_version = linux_distro.split("-")
        else:
            self.dist_type = linux_distro.split("-", maxsplit=1)[0]
            self.dist_version = None
        self.scylla_repo = scylla_repo
        self.linux_distro = linux_distro
        self.scylla_version = self.get_version(scylla_version)
        self.repo_maps = get_s3_scylla_repos_mapping(self.dist_type, self.dist_version)
        self.base_version_all_sts_versions = base_version_all_sts_versions

    def get_version(self, scylla_version: str = None) -> str:
        """
        return scylla product name and major version. if scylla_version isn't assigned,
        we will try to get the major version from the scylla_repo url.
        """
        LOGGER.info("Getting scylla product and major version for upgrade versions listing...")
        if scylla_version is None:
            if "unstable/" in self.scylla_repo:
                version_part = self.scylla_repo.split("unstable/")[1].split("/")[1]
                scylla_version = version_part.replace("branch-", "").replace("enterprise-", "")
            else:
                scylla_version = get_branch_version(self.scylla_repo)
        LOGGER.info("Scylla major version used for upgrade versions listing: %s", scylla_version)
        return scylla_version

    def set_start_support_version(self, backend: str = None) -> None:
        """
        We can't detect the support versions for this distro, which shares the repo with others, eg: centos8
        so we need to assign the start support versions for it.
        """
        LOGGER.info("Setting start support version...")
        if self.dist_type not in ["debian", "ubuntu"] and self.linux_distro != "centos":
            self.start_support_version = start_support_versions.get(self.linux_distro, None)
            if self.start_support_version is None:
                raise Exception(
                    f"We can't detect the support versions for this distro, which shares the repo with {self.dist_type}."
                )
        self.start_support_version = start_support_backend.get(backend, self.start_support_version)
        LOGGER.info("Support start version set: %s", self.start_support_version)

    def get_supported_scylla_base_versions(self, supported_versions: list[str]) -> list:  # noqa: PLR0912
        """
        We have special base versions list for each release.
        This function is used to get the base versions list which will be used in the upgrade test.

        :param supported_versions: all scylla release versions, the base versions will be filtered out from the supported_version
        """
        LOGGER.info("Getting supported scylla base versions...")
        source_available_base_version = []

        # The major version of unstable scylla, eg: 2021.2.dev, 2021.1
        version = self.scylla_version

        if version in supported_versions:
            idx = supported_versions.index(version)
            lts_version = re.compile(r"\d{4}\.1")  # lts = long term support
            sts_version = re.compile(r"\d{4}\.[2345]")  # sts = short term support

            # Find last LTS version before current version
            last_lts_idx = None
            for i in range(idx - 1, -1, -1):
                if lts_version.search(supported_versions[i]):
                    last_lts_idx = i
                    break

            if sts_version.search(version):
                # For STS version, support upgrade from STS versions since last LTS and the last LTS version
                # Current STS version should not be counted as a base version candidate
                if last_lts_idx is not None:
                    sts_range = supported_versions[last_lts_idx + 1 : idx]  # exclude current STS (idx)
                    if self.base_version_all_sts_versions:
                        # Add all preceding STS versions between last LTS (exclusive) and current STS (exclusive)
                        for v in sts_range:
                            if sts_version.search(v):
                                source_available_base_version.append(v)
                    else:
                        # Add only the latest preceding STS version (if any)
                        for v in reversed(sts_range):
                            if sts_version.search(v):
                                source_available_base_version.append(v)
                                break
                    # Always add the last LTS version itself
                    source_available_base_version.append(supported_versions[last_lts_idx])
                # If no previous LTS, fallback to previous logic (previous version only if exists)
                elif idx > 0:
                    # previous version could be STS or LTS; treat as single baseline
                    prev_version = supported_versions[idx - 1]
                    if sts_version.search(prev_version) or lts_version.search(prev_version):
                        source_available_base_version.append(prev_version)
                else:
                    LOGGER.warning("This is the first supported version, no upgrade source available.")

            elif lts_version.search(version):
                # For LTS version, support upgrade from STS versions since last LTS and the last LTS version
                if last_lts_idx is not None:
                    sts_range = supported_versions[last_lts_idx + 1 : idx]
                    if self.base_version_all_sts_versions:
                        # Add all STS versions between last LTS (exclusive) and current LTS (exclusive)
                        for v in sts_range:
                            if sts_version.search(v):
                                source_available_base_version.append(v)
                    else:
                        # Add only the latest STS version (if any) between last LTS and current LTS
                        for v in reversed(sts_range):
                            if sts_version.search(v):
                                source_available_base_version.append(v)
                                break
                    # Add the previous LTS version itself
                    source_available_base_version.append(supported_versions[last_lts_idx])
                elif idx > 0:
                    # If no previous LTS was found (shouldn't happen for pattern), fallback to previous version
                    source_available_base_version.append(supported_versions[idx - 1])
                else:
                    LOGGER.warning("This is the first supported version, no upgrade source available.")
            else:
                LOGGER.warning("version format not the default - %s", version)

            source_available_base_version.append(version)
        elif version == "master":
            # For master branch:
            # - Default: upgrade only from last LTS version
            # - If base_version_all_sts_versions flag is set: include last LTS and latest STS version as sources
            lts_pattern = re.compile(r"\d{4}\.1")
            sts_pattern = re.compile(r"\d{4}\.[2345]")
            last_lts = None
            last_sts = None
            for v in reversed(supported_versions):
                if last_lts is None and lts_pattern.search(v) and self.filter_rc_only_version([v]):
                    last_lts = v
                if last_sts is None and sts_pattern.search(v) and self.filter_rc_only_version([v]):
                    last_sts = v
                if last_lts and last_sts:
                    break
            if self.base_version_all_sts_versions:
                # Prefer to return both if they are different; if they are the same (unlikely), deduplicate later
                if last_lts:
                    source_available_base_version.append(last_lts)
                if last_sts and last_sts != last_lts:
                    source_available_base_version.append(last_sts)
            elif last_lts or last_sts:
                # a backend whose start support version is past the last LTS has no LTS to upgrade from yet
                source_available_base_version.append(last_lts or last_sts)
        elif re.match(r"\d+.\d+", version):
            relevant_versions = [v for v in supported_versions if ComparableScyllaVersion(v) < version]
            # If dest version is smaller than the first supported release,
            # it might be an invalid dest version
            if relevant_versions:
                source_available_base_version.append(relevant_versions[-1])

        # Filter out unsupported version, or Nones
        source_available_base_version = [v for v in source_available_base_version if v in supported_versions]

        # if there's only release candidates in those repos, skip this version
        source_available_base_version = self.filter_rc_only_version(source_available_base_version)
        LOGGER.info("Supported scylla base versions for: source_available=%s", source_available_base_version)
        return list(set(source_available_base_version))

    def filter_rc_only_version(self, base_version_list: list[str]) -> list[str]:
        """
        Filter out the base versions that only have release candidates (rc) versions.

        if it's master branch, we also limit the base version to the latest one,
        which isn't release candidates (rc) only release.

        :param base_version_list: list of base versions to filter
        """
        LOGGER.info("Filtering rc versions from base version list...")
        base_version_list = sorted(list(set(base_version_list)), key=ComparableScyllaVersion)
        try:
            filter_rc = [v for v in get_all_versions(self.repo_maps[base_version_list[-1]]) if "rc" not in v]
        except ValueError, ParallelObjectException:
            # Repository for this version doesn't exist yet (e.g. new release announced
            # but no rc0 packages published). Treat it as unavailable and skip it.
            LOGGER.warning("Skipping version %s: repository doesn't fully exist yet", base_version_list[-1])
            filter_rc = []
        if not filter_rc:
            # if the release only has rc versions, we don't want to test it as a base version
            base_version_list = base_version_list[:-1]
        if self.scylla_version in ("master",) and not self.base_version_all_sts_versions:
            # for master branch, we only want the latest version
            # we don't test all the options, just the latest one
            base_version_list = base_version_list[-1:]

        return base_version_list

    def get_version_list(self) -> tuple[list, list]:
        """
        return all supported releases versions, and the supported base versions of upgrade test.
        """
        LOGGER.info("Getting the supported releases version list...")
        supported_versions = []

        # Filter out the unsupported versions
        for version_prefix, _ in self.repo_maps.items():
            # skip non release versions, and the old open source ones (e.g. 6.2)
            if not re.match(r"20\d{2}\.\d+", version_prefix):
                continue
            if self.start_support_version and ComparableScyllaVersion(version_prefix) < self.start_support_version:
                continue
            if version_prefix in unsupported_versions:
                continue
            supported_versions.append(version_prefix)
        version_list = self.get_supported_scylla_base_versions(supported_versions)
        LOGGER.info("Got supported releases version list: %s", version_list)
        return supported_versions, version_list


SUPPORTED_VERSIONS_URL = (
    "https://raw.githubusercontent.com/scylladb/scylladb-docs-homepage/main/docs/_static/data/supported_versions.json"
)


def fetch_official_supported_versions(url: str = SUPPORTED_VERSIONS_URL) -> list[str]:
    """Fetch officially supported ScyllaDB versions from the scylladb-docs-homepage repository.

    Reads the supported_versions.json file, filters entries with status "Supported",
    and extracts the version number (e.g. "ScyllaDB 2026.1" -> "2026.1").

    Args:
        url: URL to the supported_versions.json file.

    Returns:
        List of supported version strings, e.g. ["2026.1", "2025.4", "2025.1"].
    """
    LOGGER.info("Fetching official supported versions from %s", url)
    session = create_retry_session()
    response = session.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    supported = []
    for entry in data.get("data", []):
        status = entry.get("status", "").strip()
        if status != "Supported":
            continue
        version_str = entry.get("version", "")
        # Extract version number from strings like "ScyllaDB 2026.1" or "ScyllaDB 2025.1 (LTS)"
        match = re.search(r"(\d{4}\.\d+)", version_str)
        if match:
            supported.append(match.group(1))

    LOGGER.info("Official supported versions: %s", supported)
    return supported
