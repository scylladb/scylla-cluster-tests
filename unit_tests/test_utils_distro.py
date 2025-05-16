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

from __future__ import absolute_import

import unittest

from sdcm.utils.distro import Distro, DistroError


DISTROS_OS_RELEASE = {
    "Debian 12": """\
PRETTY_NAME="Debian GNU/Linux 12 (bookworm)"
NAME="Debian GNU/Linux"
VERSION_ID="12"
VERSION="12 (bookworm)"
VERSION_CODENAME=bookworm
ID=debian
HOME_URL="https://www.debian.org/"
SUPPORT_URL="https://www.debian.org/support"
BUG_REPORT_URL="https://bugs.debian.org/"
""",

    "Debian 11": """\
PRETTY_NAME="Debian GNU/Linux 11 (bullseye)"
NAME="Debian GNU/Linux"
VERSION_ID="11"
VERSION="11 (bullseye)"
VERSION_CODENAME=bullseye
ID=debian
HOME_URL="https://www.debian.org/"
SUPPORT_URL="https://www.debian.org/support"
BUG_REPORT_URL="https://bugs.debian.org/"
""",

    "CentOS 7": """\
NAME="CentOS Linux"
VERSION="7 (Core)"
ID="centos"
ID_LIKE="rhel fedora"
VERSION_ID="7"
PRETTY_NAME="CentOS Linux 7 (Core)"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:centos:centos:7"
HOME_URL="https://www.centos.org/"
BUG_REPORT_URL="https://bugs.centos.org/"

CENTOS_MANTISBT_PROJECT="CentOS-7"
CENTOS_MANTISBT_PROJECT_VERSION="7"
REDHAT_SUPPORT_PRODUCT="centos"
REDHAT_SUPPORT_PRODUCT_VERSION="7"
""",

    "CentOS 8": """\
NAME="CentOS Linux"
VERSION="8 (Core)"
ID="centos"
ID_LIKE="rhel fedora"
VERSION_ID="8"
PLATFORM_ID="platform:el8"
PRETTY_NAME="CentOS Linux 8 (Core)"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:centos:centos:8"
HOME_URL="https://www.centos.org/"
BUG_REPORT_URL="https://bugs.centos.org/"

CENTOS_MANTISBT_PROJECT="CentOS-8"
CENTOS_MANTISBT_PROJECT_VERSION="8"
REDHAT_SUPPORT_PRODUCT="centos"
REDHAT_SUPPORT_PRODUCT_VERSION="8"
""",

    "RHEL 7": """\
NAME="Red Hat Enterprise Linux Server"
VERSION="7.7 (Maipo)"
ID="rhel"
ID_LIKE="fedora"
VARIANT="Server"
VARIANT_ID="server"
VERSION_ID="7.7"
PRETTY_NAME="Red Hat Enterprise Linux Server 7.7 (Maipo)"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:redhat:enterprise_linux:7.7:GA:server"
HOME_URL="https://www.redhat.com/"
BUG_REPORT_URL="https://bugzilla.redhat.com/"

REDHAT_BUGZILLA_PRODUCT="Red Hat Enterprise Linux 7"
REDHAT_BUGZILLA_PRODUCT_VERSION=7.7
REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux"
REDHAT_SUPPORT_PRODUCT_VERSION="7.7"
""",

    "RHEL 8": """\
NAME="Red Hat Enterprise Linux"
VERSION="8.1 (Ootpa)"
ID="rhel"
ID_LIKE="fedora"
VERSION_ID="8.1"
PLATFORM_ID="platform:el8"
PRETTY_NAME="Red Hat Enterprise Linux 8.1 (Ootpa)"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:redhat:enterprise_linux:8.1:GA"
HOME_URL="https://www.redhat.com/"
BUG_REPORT_URL="https://bugzilla.redhat.com/"

REDHAT_BUGZILLA_PRODUCT="Red Hat Enterprise Linux 8"
REDHAT_BUGZILLA_PRODUCT_VERSION=8.1
REDHAT_SUPPORT_PRODUCT="Red Hat Enterprise Linux"
REDHAT_SUPPORT_PRODUCT_VERSION="8.1"
""",

    "OEL 7.3": """\
NAME="Oracle Linux Server"
VERSION="7.3"
ID="ol"
VERSION_ID="7.3"
PRETTY_NAME="Oracle Linux Server 7.3"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:oracle:linux:7:3:server"
HOME_URL="https://linux.oracle.com/"
BUG_REPORT_URL="https://bugzilla.oracle.com/"

ORACLE_BUGZILLA_PRODUCT="Oracle Linux 7"
ORACLE_BUGZILLA_PRODUCT_VERSION=7.3
ORACLE_SUPPORT_PRODUCT="Oracle Linux"
ORACLE_SUPPORT_PRODUCT_VERSION=7.3
""",

    "OEL 8.1": """\
NAME="Oracle Linux Server"
VERSION="8.1"
ID="ol"
ID_LIKE="fedora"
VARIANT="Server"
VARIANT_ID="server"
VERSION_ID="8.1"
PLATFORM_ID="platform:el8"
PRETTY_NAME="Oracle Linux Server 8.1"
ANSI_COLOR="0;31"
CPE_NAME="cpe:/o:oracle:linux:8:1:server"
HOME_URL="https://linux.oracle.com/"
BUG_REPORT_URL="https://bugzilla.oracle.com/"

ORACLE_BUGZILLA_PRODUCT="Oracle Linux 8"
ORACLE_BUGZILLA_PRODUCT_VERSION=8.1
ORACLE_SUPPORT_PRODUCT="Oracle Linux"
ORACLE_SUPPORT_PRODUCT_VERSION=8.1
""",

    "SLES 15": """\
NAME="SLES"
VERSION="15-SP3"
VERSION_ID="15.3"
PRETTY_NAME="SUSE Linux Enterprise Server 15 SP3"
ID="sles"
ID_LIKE="suse"
ANSI_COLOR="0;32"
CPE_NAME="cpe:/o:suse:sles:15:sp3"
DOCUMENTATION_URL="https://documentation.suse.com/"
""",

    "Ubuntu 20.04": """\
NAME="Ubuntu"
VERSION="20.04 LTS (Focal Fossa)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 20.04 LTS"
VERSION_ID="20.04"
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
VERSION_CODENAME=focal
UBUNTU_CODENAME=focal
""",

    "Ubuntu 22.04": """\
NAME="Ubuntu"
VERSION="22.04 LTS (Jammy Jellyfish)"
ID=ubuntu
ID_LIKE=debian
PRETTY_NAME="Ubuntu 22.04 LTS"
VERSION_ID="22.04"
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
VERSION_CODENAME=jammy
UBUNTU_CODENAME=jammy
    """,

    "Amazon Linux 2": """\
NAME="Amazon Linux"
VERSION="2"
ID="amzn"
ID_LIKE="centos rhel fedora"
VERSION_ID="2"
PRETTY_NAME="Amazon Linux 2"
ANSI_COLOR="0;33"
CPE_NAME="cpe:2.3:o:amazon:amazon_linux:2"
HOME_URL="https://amazonlinux.com/"
""",

    "Amazon Linux 2023": """\
NAME="Amazon Linux"
VERSION="2023"
ID="amzn"
ID_LIKE="fedora"
VERSION_ID="2023"
PLATFORM_ID="platform:al2023"
PRETTY_NAME="Amazon Linux 2023.4.20240513"
ANSI_COLOR="0;33"
CPE_NAME="cpe:2.3:o:amazon:amazon_linux:2023"
HOME_URL="https://aws.amazon.com/linux/amazon-linux-2023/"
DOCUMENTATION_URL="https://docs.aws.amazon.com/linux/"
SUPPORT_URL="https://aws.amazon.com/premiumsupport/"
BUG_REPORT_URL="https://github.com/amazonlinux/amazon-linux-2023"
VENDOR_NAME="AWS"
VENDOR_URL="https://aws.amazon.com/"
SUPPORT_END="2028-03-15"
""",

    "Rocky Linux 8": """\
NAME="Rocky Linux"
VERSION="8.5 (Green Obsidian)"
ID="rocky"
ID_LIKE="rhel centos fedora"
VERSION_ID="8.5"
PLATFORM_ID="platform:el8"
PRETTY_NAME="Rocky Linux 8.5 (Green Obsidian)"
ANSI_COLOR="0;32"
CPE_NAME="cpe:/o:rocky:rocky:8:GA"
HOME_URL="https://rockylinux.org/"
BUG_REPORT_URL="https://bugs.rockylinux.org/"
ROCKY_SUPPORT_PRODUCT="Rocky Linux"
ROCKY_SUPPORT_PRODUCT_VERSION="8"
""",
    "Rocky Linux 9": """\
NAME="Rocky Linux"
VERSION="9.0 (Blue Onyx)"
ID="rocky"
ID_LIKE="rhel centos fedora"
VERSION_ID="9.0"
PLATFORM_ID="platform:el9"
PRETTY_NAME="Rocky Linux 9.0 (Blue Onyx)"
ANSI_COLOR="0;32"
LOGO="fedora-logo-icon"
CPE_NAME="cpe:/o:rocky:rocky:9::baseos"
HOME_URL="https://rockylinux.org/"
BUG_REPORT_URL="https://bugs.rockylinux.org/"
ROCKY_SUPPORT_PRODUCT="Rocky-Linux-9"
ROCKY_SUPPORT_PRODUCT_VERSION="9.0"
REDHAT_SUPPORT_PRODUCT="Rocky Linux"
REDHAT_SUPPORT_PRODUCT_VERSION="9.0"
""",

    "Unknown": """\
ID=sillylinux
VERSION_ID=666
""",

    "Garbage": """\
ID ubuntu
VERSION_ID 18.04
""",
}


class TestDistro(unittest.TestCase):
    def test_unknown(self):
        self.assertTrue(Distro.UNKNOWN.is_unknown)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Unknown"])
        self.assertTrue(distro.is_unknown)

    def test_debian11(self):
        self.assertTrue(Distro.DEBIAN11.is_debian11)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Debian 11"])
        self.assertTrue(distro.is_debian11)
        self.assertTrue(distro.is_debian)

    def test_debian12(self):
        self.assertTrue(Distro.DEBIAN12.is_debian12)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Debian 12"])
        self.assertTrue(distro.is_debian12)
        self.assertTrue(distro.is_debian)

    def test_centos7(self):
        self.assertTrue(Distro.CENTOS7.is_centos7)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["CentOS 7"])
        self.assertTrue(distro.is_centos7)
        self.assertTrue(distro.is_rhel_like)

    def test_centos8(self):
        self.assertTrue(Distro.CENTOS8.is_centos8)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["CentOS 8"])
        self.assertTrue(distro.is_centos8)
        self.assertTrue(distro.is_rhel_like)

    def test_rhel7(self):
        self.assertTrue(Distro.RHEL7.is_rhel7)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["RHEL 7"])
        self.assertTrue(distro.is_rhel7)
        self.assertTrue(distro.is_rhel_like)

    def test_rhel8(self):
        self.assertTrue(Distro.RHEL8.is_rhel8)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["RHEL 8"])
        self.assertTrue(distro.is_rhel8)
        self.assertTrue(distro.is_rhel_like)

    def test_oel7(self):
        self.assertTrue(Distro.OEL7.is_oel7)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["OEL 7.3"])
        self.assertTrue(distro.is_oel7)
        self.assertTrue(distro.is_rhel_like)

    def test_oel8(self):
        self.assertTrue(Distro.OEL8.is_oel8)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["OEL 8.1"])
        self.assertTrue(distro.is_oel8)
        self.assertTrue(distro.is_rhel_like)

    def test_sles15(self):
        self.assertTrue(Distro.SLES15.is_sles15)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["SLES 15"])
        self.assertTrue(distro.is_sles15)
        self.assertTrue(distro.is_sles)

    def test_ubuntu20(self):
        self.assertTrue(Distro.UBUNTU20.is_ubuntu20)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Ubuntu 20.04"])
        self.assertTrue(distro.is_ubuntu20)
        self.assertTrue(distro.is_ubuntu)

    def test_ubuntu22(self):
        self.assertTrue(Distro.UBUNTU22.is_ubuntu22)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Ubuntu 22.04"])
        self.assertTrue(distro.is_ubuntu22)
        self.assertTrue(distro.is_ubuntu)

    def test_amazon2(self):
        self.assertTrue(Distro.AMAZON2.is_amazon2)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Amazon Linux 2"])
        self.assertTrue(distro.is_amazon2)
        self.assertTrue(distro.is_rhel_like)

    def test_amazon2023(self):
        self.assertTrue(Distro.AMAZON2023.is_amazon2023)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Amazon Linux 2023"])
        self.assertTrue(distro.is_amazon2023)
        self.assertTrue(distro.is_rhel_like)

    def test_rocky8(self):
        self.assertTrue(Distro.ROCKY8.is_rocky8)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Rocky Linux 8"])
        self.assertTrue(distro.is_rocky8)
        self.assertTrue(distro.is_rhel_like)

    def test_rocky9(self):
        self.assertTrue(Distro.ROCKY9.is_rocky9)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Rocky Linux 9"])
        self.assertTrue(distro.is_rocky9)
        self.assertTrue(distro.is_rhel_like)

    def test_parsing_error(self):
        self.assertRaises(DistroError, Distro.from_os_release, DISTROS_OS_RELEASE["Garbage"])

    def test_debian_like(self):
        self.assertTrue(Distro.DEBIAN11.is_debian11)
        distro = Distro.from_os_release(DISTROS_OS_RELEASE["Debian 11"])
        invalid_distro = Distro.from_os_release(DISTROS_OS_RELEASE["Amazon Linux 2"])
        self.assertTrue(distro.is_debian_like)
        self.assertFalse(invalid_distro.is_debian_like)
