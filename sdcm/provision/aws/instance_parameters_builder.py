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
from typing import List, Optional


from sdcm.provision.aws.instance_parameters import AWSPlacementInfo, AWSDiskMapping
from sdcm.provision.common.builders import AttrBuilder


class AWSInstanceParamsBuilderBase(AttrBuilder, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def BlockDeviceMappings(self) -> List[AWSDiskMapping]:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def ImageId(self) -> Optional[str]:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def KeyName(self) -> str:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def NetworkInterfaces(self) -> List[dict]:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def IamInstanceProfile(self):  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def InstanceType(self) -> str:  # pylint: disable=invalid-name
        pass

    @property
    @abc.abstractmethod
    def Placement(self) -> Optional[AWSPlacementInfo]:  # pylint: disable=invalid-name
        pass
