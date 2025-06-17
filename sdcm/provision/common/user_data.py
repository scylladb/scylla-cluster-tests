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
# Copyright (c) 2021 ScyllaDB

import abc
from enum import Enum
from typing import Literal

from sdcm.provision.common.builders import AttrBuilder


DataDeviceType = Literal['attached', 'instance_store']


class RaidLevelType(int, Enum):
    RAID0 = 0
    RAID5 = 5


class UserDataBuilderBase(AttrBuilder, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def to_string(self) -> str:
        pass


class ScyllaUserDataBuilderBase(UserDataBuilderBase, metaclass=abc.ABCMeta):
    cluster_name: str

    @property
    @abc.abstractmethod
    def scylla_yaml(self):
        pass

    @property
    @abc.abstractmethod
    def start_scylla_on_first_boot(self):
        return False

    @property
    @abc.abstractmethod
    def data_device(self) -> DataDeviceType:
        """
        Tell scylla setup to target non-nvme volumes for data devices if it is configured so
         and use nvme disks otherwise
        """

    @property
    @abc.abstractmethod
    def post_configuration_script(self) -> str:
        """
        Script that is ran right after node is initialized
        """

    @abc.abstractmethod
    def to_string(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def raid_level(self) -> RaidLevelType:
        """
        Tell scylla setup which raid to build RAID0 or RAID5
        """
