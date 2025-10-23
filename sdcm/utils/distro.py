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

import enum
import logging
from sdcm.utils.decorators import static_init

LOGGER = logging.getLogger(__name__)


class DistroError(Exception):
    pass


@enum.unique
class DistroBase(enum.Enum):
    RHEL = enum.auto()
    DEBIAN = enum.auto()
    UNKNOWN = enum.auto()


# A tuple of tuples of the form:
# (enum_prefix, os name, versions list (format is either "major.minor" or "major"), boolean - debian_like == True else rhel_like)
KNOWN_OS = (
    ("CENTOS", "centos", ["7", "8", "9"], DistroBase.RHEL),
    ("RHEL", "rhel", ["7", "8", "9", "10"], DistroBase.RHEL),
    ("OEL", "ol", ["7", "8", "9"], DistroBase.RHEL),
    ("AMAZON", "amzn", ["2023"], DistroBase.RHEL),
    ("ROCKY", "rocky", ["8", "9", "10"], DistroBase.RHEL),
    ("DEBIAN", "debian", ["11", "12", "13"], DistroBase.DEBIAN),
    ("UBUNTU", "ubuntu", ["20.04", "21.04", "21.10", "22.04", "24.04"], DistroBase.DEBIAN),
    ("SLES", "sles", ["15"], DistroBase.UNKNOWN),
    ("FEDORA", "fedora", ["34", "35", "36"], DistroBase.RHEL),
    ("MINT", "linuxmint", ["20", "21", "22"], DistroBase.DEBIAN),

)


enum_data = {}

DistroBase_mapping = {}

for enum_prefix, os, versions, DistroBase in KNOWN_OS:
    if DistroBase not in DistroBase_mapping:
        DistroBase_mapping[DistroBase] = []
    DistroBase_mapping[DistroBase].append(os)
    versions_by_major = {}
    for version in versions:
        version_major = version.split(".", maxsplit=1)[0]
        if version_major not in versions_by_major:
            versions_by_major[version_major] = []
        versions_by_major[version_major].append(version)
    for version_major, versions_with_same_major in versions_by_major.items():
        versions_with_same_major.sort()
        enum_data[enum_prefix + version_major] = (os, versions_with_same_major[0])
        for version in versions_with_same_major[1:]:
            version_for_member_names = version.replace(".", "_")
            enum_data[enum_prefix + version_for_member_names] = (os, version)


@static_init
class EnumFunctionalMixin:
    @classmethod
    def static_init(cls):
        def get_os_name_for_prop(name):
            p_name = next(filter(lambda x: x[1] == name, KNOWN_OS), None)
            if p_name is not None:
                return p_name[0].lower()
            else:
                return name
        properties_mapping = {}
        for enum_name, data in enum_data.items():
            version_for_prop = data[1].replace(".", "_")
            version_prefix = version_for_prop.split('_')[0]
            os_name_for_prop = get_os_name_for_prop(data[0])
            prop_name = f'is_{os_name_for_prop}'
            prop_name_with_version = f'{prop_name}{version_prefix}'
            if prop_name not in properties_mapping:
                properties_mapping[prop_name] = []
            if prop_name_with_version not in properties_mapping:
                properties_mapping[prop_name_with_version] = []
            properties_mapping[prop_name].append(enum_name)
            properties_mapping[prop_name_with_version].append(enum_name)
            if version_for_prop != version_prefix:
                prop_name_with_version = f'{prop_name}{version_prefix}'
                if prop_name_with_version not in properties_mapping:
                    properties_mapping[prop_name_with_version] = []
                properties_mapping[prop_name_with_version].append(enum_name)
        setattr(cls, 'properties_mapping', properties_mapping)
        for prop_name, enum_names in properties_mapping.items():
            setattr(cls, prop_name, property(lambda self, enum_names=enum_names: self.name in enum_names))
        enum_data['UNKNOWN'] = (None, None)

    @property
    def is_unknown(self):
        return self.name == 'UNKNOWN'

    @property
    def is_debian_like(self):
        return self.value[0] in DistroBase_mapping[DistroBase.DEBIAN]

    @property
    def is_rhel_like(self):
        return self.value[0] in DistroBase_mapping[DistroBase.RHEL]

    @classmethod
    def _missing_(cls, value):
        LOGGER.error("%s: missed key for %s", cls.__name__, value)
        return cls.UNKNOWN

    @classmethod
    def from_os_release(cls, os_release: str):
        """Parse /etc/os-release file and return Distro instance.

        Example of /etc/os-release (Debian 9):
          PRETTY_NAME="Debian GNU/Linux 9 (stretch)"
          NAME="Debian GNU/Linux"
          VERSION_ID="9"
          VERSION="9 (stretch)"
          VERSION_CODENAME=stretch
          ID=debian
          HOME_URL="https://www.debian.org/"
          SUPPORT_URL="https://www.debian.org/support"
          BUG_REPORT_URL="https://bugs.debian.org/"
        """
        parsed = {}
        for line in os_release.splitlines():
            if not line.strip():  # skip empty lines
                continue
            try:
                key, value = line.split("=", 1)
            except ValueError as err:
                raise DistroError(f"Unable to parse /etc/os-release line: `{line}'") from err
            value = value.strip('"')
            parsed[key] = value
        distro_id = parsed.get("ID")
        distro_version_id = parsed.get("VERSION_ID")

        # Don't remove minor version for Ubuntu (e.g., keep `16.04' and `18.04' as is.)
        if distro_id != "ubuntu" and distro_version_id is not None:
            distro_version_id = distro_version_id.split(".", 1)[0]

        # Use a nice feature of Enum here:
        #   https://docs.python.org/3/library/enum.html#programmatic-access-to-enumeration-members-and-their-attributes
        distro = cls((distro_id, distro_version_id))

        if distro == cls.UNKNOWN:
            LOGGER.error("Unable to detect Linux distribution name")
        else:
            LOGGER.debug("Detected Linux distribution: %s", distro.name)

        return distro

    @property
    def uses_systemd(self):
        # Debian uses systemd as a default init system since 8.0, Ubuntu since 15.04, and RedHat-based since 7.0
        return self not in (self.UNKNOWN, )


Distro: enum.Enum = enum.Enum('Distro', enum_data, module=__name__, type=EnumFunctionalMixin)
