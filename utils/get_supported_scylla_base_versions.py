#!/usr/bin/env python3
import logging
import sys
import re
import os

from sdcm.utils.version_utils import (
    is_enterprise,
    get_all_versions,
    get_branch_version,
    ComparableScyllaVersion,
    get_s3_scylla_repos_mapping,
)

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


LOGGER = logging.getLogger(__name__)


# We add here special versions that are supported, but don't follow the default upgrade support matrix
extra_supported_versions = {
    "2021.1": ["4.3"],
    "2022.1": ["5.0"],
    "2022.2": ["5.1"],
    "2023.1": ["5.2"],
    "2024.1": ["5.4"],
    "2024.2": ["6.0"],
    "2025.1": ["6.2"],
}
# If new support distro shared repo with others, we need to assign the start support versions. eg: centos8
start_support_versions = {
    "centos-8": {"scylla": "4.1", "enterprise": "2021.1"},
    "centos-9": {"scylla": "5.4", "enterprise": "2024.1"},
    # oss isn't really supported on rocky10, but we add it here sinc code can't get None value
    "rocky-10": {"scylla": None, "enterprise": "2025.3"},
}
start_support_backend = {"azure": {"scylla": "5.2", "enterprise": "2023.1"}}

# list of version that are available, but aren't supported, and we should test upgrades from
unsupported_versions = [
    "5.3",
]


class UpgradeBaseVersion:
    oss_start_support_version = None
    ent_start_support_version = None

    def __init__(
        self,
        scylla_repo: str,
        linux_distro: str,
        scylla_version: str = None,
        base_version_all_sts_versions: bool = False,
        official_supported_versions: bool = False,
    ):
        if len(linux_distro.split("-")) > 1:
            self.dist_type, *_, self.dist_version = linux_distro.split("-")
        else:
            self.dist_type = linux_distro.split("-")[0]
            self.dist_version = None
        self.scylla_repo = scylla_repo
        self.linux_distro = linux_distro
        self.scylla_version = self.get_version(scylla_version)
        self.repo_maps = get_s3_scylla_repos_mapping(self.dist_type, self.dist_version)
        self.official_supported_versions = official_supported_versions
        self.base_version_all_sts_versions = base_version_all_sts_versions
        # Official supported versions are 2 LTS, and 2 latests released.
        self.num_base_versions = 1 if not official_supported_versions else 2

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
            versions_dict = start_support_versions.get(self.linux_distro, None)
            if versions_dict is None:
                raise Exception(
                    f"We can't detect the support versions for this distro, which shares the repo with {self.dist_type}."
                )
            self.oss_start_support_version = versions_dict["scylla"]
            self.ent_start_support_version = versions_dict["enterprise"]
        backend_dict = start_support_backend.get(backend, None)
        if backend_dict:
            self.oss_start_support_version = backend_dict["scylla"]
            self.ent_start_support_version = backend_dict["enterprise"]
        LOGGER.info(
            "Support start versions set: oss=%s enterprise=%s",
            self.oss_start_support_version,
            self.ent_start_support_version,
        )

    def _find_previous_lts_indices(self, release_list: list[str], idx: int) -> list[int]:
        """Find indices of LTS versions before the given index (up to num_base_versions)."""
        lts_pattern = re.compile(r"\d{4}\.1")
        lts_indices = []
        for i in range(idx - 1, -1, -1):
            if lts_pattern.search(release_list[i]):
                lts_indices.append(i)
                if len(lts_indices) >= self.num_base_versions:
                    break
        return lts_indices

    def _collect_sts_versions_from_range(self, sts_range: list[str], sts_pattern: re.Pattern) -> list[str]:
        """Collect STS versions from a range, respecting base_version_all_sts_versions and num_base_versions."""
        if self.base_version_all_sts_versions:
            return [v for v in sts_range if sts_pattern.search(v)]
        # Return up to num_base_versions from the end (most recent first)
        result = []
        for v in reversed(sts_range):
            if sts_pattern.search(v):
                result.append(v)
                if len(result) >= self.num_base_versions:
                    break
        return result

    def _get_base_versions_for_release(self, release_list: list[str], idx: int) -> list[str]:
        """Get base versions when current version is a known release (LTS or STS)."""
        lts_pattern = re.compile(r"\d{4}\.1")
        sts_pattern = re.compile(r"\d{4}\.[2345]")
        version = release_list[idx]
        result = []

        is_sts = sts_pattern.search(version) or ComparableScyllaVersion(version) < "2023.1"
        is_lts = lts_pattern.search(version)

        if not is_sts and not is_lts:
            LOGGER.warning("version format not the default - %s", version)
            result.append(version)
            return result

        lts_indices = self._find_previous_lts_indices(release_list, idx)
        last_lts_idx = lts_indices[0] if lts_indices else None

        if last_lts_idx is not None:
            sts_range = release_list[last_lts_idx + 1 : idx]
            result.extend(self._collect_sts_versions_from_range(sts_range, sts_pattern))
            # Add LTS versions
            for lts_idx in lts_indices:
                result.append(release_list[lts_idx])
        elif idx > 0:
            prev_version = release_list[idx - 1]
            if sts_pattern.search(prev_version) or lts_pattern.search(prev_version):
                result.append(prev_version)
        else:
            LOGGER.warning("This is the first supported version, no upgrade source available.")

        result.append(version)
        return result

    def _get_base_versions_for_master(self, release_list: list[str]) -> list[str]:
        """Get base versions when target is the master branch."""
        lts_pattern = re.compile(r"\d{4}\.1")
        sts_pattern = re.compile(r"\d{4}\.[2345]")
        lts_found = []
        sts_found = []

        for v in reversed(release_list):
            try:
                if (
                    len(lts_found) < self.num_base_versions
                    and lts_pattern.search(v)
                    and self.filter_rc_only_version([v])
                ):
                    lts_found.append(v)
                if (
                    len(sts_found) < self.num_base_versions
                    and sts_pattern.search(v)
                    and self.filter_rc_only_version([v])
                ):
                    sts_found.append(v)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping version %s — failed to check rc status: %s", v, exc)
                continue
            if len(lts_found) >= self.num_base_versions and len(sts_found) >= self.num_base_versions:
                break

        result = list(lts_found)

        if self.base_version_all_sts_versions:
            latest_lts = lts_found[0] if lts_found else None
            has_sts_after_latest_lts = latest_lts and any(
                ComparableScyllaVersion(s) > ComparableScyllaVersion(latest_lts) for s in sts_found
            )
            if has_sts_after_latest_lts:
                for v in sts_found:
                    if v not in result:
                        result.append(v)
            elif sts_found:
                if sts_found[0] not in result:
                    result.append(sts_found[0])

        return result

    def _get_base_versions_for_unreleased(self, release_list: list[str], version: str) -> list[str]:
        """Get base versions for an unreleased numeric version (not in supported list, not master)."""
        relevant_versions = [v for v in release_list if ComparableScyllaVersion(v) < version]
        if relevant_versions:
            return [relevant_versions[-1]]
        return []

    def get_supported_scylla_base_versions(self, supported_versions: list[str]) -> list:
        """
        Get the base versions list which will be used in the upgrade test.

        We have special base versions list for each release, and we don't support upgrading
        from enterprise to opensource.

        :param supported_versions: all scylla release versions, the base versions will be filtered from this list
        """
        LOGGER.info("Getting supported scylla base versions...")

        source_available_release_list = [v for v in supported_versions if is_enterprise(v)]
        version = self.scylla_version

        # Start with extra supported versions that don't follow the default upgrade support matrix
        base_versions = list(extra_supported_versions.get(version, []))

        if version in supported_versions:
            idx = source_available_release_list.index(version)
            base_versions.extend(self._get_base_versions_for_release(source_available_release_list, idx))
        elif version == "master":
            base_versions.extend(self._get_base_versions_for_master(source_available_release_list))
        elif re.match(r"\d+.\d+", version):
            base_versions.extend(self._get_base_versions_for_unreleased(source_available_release_list, version))

        # Filter out unsupported versions
        base_versions = [v for v in base_versions if v in supported_versions]

        # Filter out versions that only have release candidates
        base_versions = self.filter_rc_only_version(base_versions)
        LOGGER.info("Supported scylla base versions for: source_available=%s", base_versions)
        return list(set(base_versions))

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
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch versions for %s: %s — keeping it in the list", base_version_list[-1], exc)
            filter_rc = True  # assume non-rc versions exist
        if not filter_rc:
            # if the release only has rc versions, we don't want to test it as a base version
            base_version_list = base_version_list[:-1]

        return base_version_list

    def get_version_list(self) -> tuple[list, list]:
        """
        return all supported releases versions, and the supported base versions of upgrade test.
        """
        LOGGER.info("Getting the supported releases version list...")
        supported_versions = []

        # Filter out the unsupported versions
        for version_prefix, _ in self.repo_maps.items():
            # can't find the major version from the version_prefix string
            if not re.match(r"\d+.\d+", version_prefix):
                continue
            # OSS: the major version is smaller than the start support version
            if (
                self.oss_start_support_version
                and not is_enterprise(version_prefix)
                and ComparableScyllaVersion(version_prefix) < self.oss_start_support_version
            ):
                continue
            # Enterprise: the major version is smaller than the start support version
            if (
                self.ent_start_support_version
                and is_enterprise(version_prefix)
                and ComparableScyllaVersion(version_prefix) < self.ent_start_support_version
            ):
                continue
            if version_prefix in unsupported_versions:
                continue
            supported_versions.append(version_prefix)
        version_list = self.get_supported_scylla_base_versions(supported_versions)
        LOGGER.info("Got supported releases version list: %s", version_list)
        return supported_versions, version_list
