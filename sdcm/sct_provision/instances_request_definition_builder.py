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
# Copyright (c) 2022 ScyllaDB

from dataclasses import dataclass
from typing import Callable, List

from sdcm.provision.provisioner import InstanceDefinition
from sdcm.sct_config import SCTConfiguration


@dataclass
class InstancesRequest:
    """List of InstancesDefinitions and Provisioner creation attributes.

    Contains complete information needed to create instances for given region and test id"""
    backend: str
    test_id: str
    region: str
    definitions: List[InstanceDefinition]


class InstancesRequestBuilder:
    """Entry point for creation all needed information to create Provisioners and instances, based on SCT Configuration.

    Each backend must register own callable (e.g. function) which will be used when given backend is used."""

    def __init__(self) -> None:
        self._functions = {}

    def register_builder(self, backend: str, builder_function: Callable) -> None:
        """Registers builder for given backend

        Must be used before calling InstancesRequestBuilder for given backend."""
        self._functions[backend] = builder_function

    def build(self, sct_config: SCTConfiguration) -> List[InstancesRequest]:
        """Creates InstancesRequest for each region based on SCTConfiguration.

        Prior use, must register builder for given backend."""
        backend = sct_config.get("cluster_backend")
        return self._functions[backend](sct_config)
