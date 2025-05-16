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

import importlib

from sdcm.sct_config import SCTConfiguration, init_and_verify_sct_config
from sdcm.test_config import TestConfig


class SCTProvisionLayout:
    _cluster_backend_mapping = {}

    def __init_subclass__(cls, cluster_backend: str = None):
        if cluster_backend is None:
            raise RuntimeError('Please init class with `cluster_backend` property')
        cls._cluster_backend_mapping[cluster_backend] = cls

    def __new__(cls, params: SCTConfiguration):
        backend = params.get('cluster_backend')
        try:
            importlib.import_module(cls.__module__.replace('.common.', f'.{backend}.'), package=None)
        except ModuleNotFoundError:
            pass
        target_class = cls._cluster_backend_mapping.get(backend, SCTProvisionLayout)
        return object.__new__(target_class)

    def __init__(self, params: SCTConfiguration):
        self._params = params

    @property
    def _provision_another_scylla_cluster(self):
        return self._params.get('db_type') == 'mixed_scylla'

    @property
    def db_cluster(self):
        return None

    @property
    def loader_cluster(self):
        return None

    @property
    def monitoring_cluster(self):
        return None

    @property
    def cs_db_cluster(self):
        return None

    def provision(self):
        raise RuntimeError("This backend is not supported yet")


def create_sct_configuration(test_name: str):
    sct_configuration = init_and_verify_sct_config()
    TestConfig().set_test_name(test_name)
    TestConfig().set_test_id_only(sct_configuration.get('test_id'))
    return sct_configuration
