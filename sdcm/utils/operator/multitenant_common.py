#!/usr/bin/env python

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

import copy
import logging
import time

from sdcm.utils.common import ParallelObject
from sdcm.tester import silence
from sdcm.utils.database_query_utils import PartitionsValidationAttributes

LOGGER = logging.getLogger(__name__)


class TenantMixin:  # pylint: disable=too-many-instance-attributes
    _testMethodName = "runTest"

    # pylint: disable=too-many-instance-attributes
    def __init__(self, db_cluster, loaders, monitors,  # pylint: disable=too-many-arguments
                 prometheus_db, params, test_config, cluster_index):
        self.db_cluster = db_cluster
        self.loaders = loaders
        self.monitors = monitors
        self.prometheus_db = prometheus_db
        self.params = copy.deepcopy(params)
        self.kafka_cluster = None
        self.log = logging.getLogger(self.__class__.__name__)
        self._stats = self._init_stats()
        self.test_config = test_config
        self._init_test_duration()
        self.create_stats = self.params.get(key='store_perf_results')
        self.partitions_attrs: PartitionsValidationAttributes | None = self._init_data_validation()
        self.status = "RUNNING"
        self.cluster_index = str(cluster_index)
        self._test_id = self.test_config.test_id() + f"--{cluster_index}"
        self._test_index = self.get_str_index()
        self.create_test_stats()
        self.start_time = self.get_test_start_time() or time.time()
        self.timeout_thread = self._init_test_timeout_thread()
        self.test_config.reuse_cluster(False)
        self.validate_large_collections = self.params.get('validate_large_collections')

    def get_str_index(self):
        return f"{self.get_str_index_prefix}-{self.db_cluster.k8s_clusters[0].tenants_number}-tenants"

    def id(self):  # pylint: disable=invalid-name
        if "Longevity" in self.__class__.__name__:
            return f"{self.test_config.test_id()}-{self._test_index}"
        # NOTE: performance results should not have unique IDs to be able to be used in comparisons
        return f"id--{self._test_index}"

    def __str__(self) -> str:
        return self._test_index + f"--{self.cluster_index}"

    def __repr__(self) -> str:
        return self.__str__()


def get_tenants(test_class_instance):  # pylint: disable=too-many-branches,too-many-locals
    parent_test_class = None
    for base in test_class_instance.__class__.__bases__:
        if base.__name__.endswith("Test"):
            parent_test_class = base
            break
    else:
        test_class_instance.log.warning(
            "Could not find parent test class. Tenant classes won't inherit anything.")
    tenant_class_name, get_str_index_prefix = f"TenantFor{parent_test_class.__name__}", "k8s"
    if "Performance" in tenant_class_name:
        get_str_index_prefix += "-perf"
    elif "Longevity" in tenant_class_name:
        get_str_index_prefix += "-longevity"

    # Init tenants
    tenants = []
    for i, db_cluster in enumerate(test_class_instance.db_clusters_multitenant):
        current_cluster_index = i + 1
        current_tenant_class = type(
            f"{tenant_class_name}-{current_cluster_index}", (TenantMixin, parent_test_class), {
                "get_str_index_prefix": get_str_index_prefix,
            }
        )
        tenants.append(current_tenant_class(
            db_cluster=db_cluster,
            loaders=test_class_instance.loaders_multitenant[i],
            monitors=test_class_instance.monitors_multitenant[i],
            prometheus_db=test_class_instance.prometheus_db_multitenant[i],
            params=test_class_instance.params,
            test_config=test_class_instance.test_config,
            cluster_index=current_cluster_index,
        ))

    # Process multitenant parameters if present
    for param_dict in test_class_instance.params.config_options:
        param_name = param_dict['name']
        param_value = test_class_instance.params.get(param_name)
        if not (param_dict.get("is_k8s_multitenant_value") and isinstance(param_value, list)):
            continue
        LOGGER.debug(
            "Process multitenant option '%s'. It's value: %s",
            param_name, param_value)
        for i, _ in enumerate(tenants):
            current_param_value = param_value[i]
            if param_name.startswith("stress_") and isinstance(
                    current_param_value, list) and len(current_param_value) == 1:
                # NOTE: performance tests expect only single string values
                #       for the main stress commands. So, transform list of a single str to str.
                current_param_value = current_param_value[0]
            LOGGER.debug(
                "Setting '%s' option of the '%s' tenant with the following value: %s",
                param_name, str(tenants[i]), current_param_value)
            tenants[i].params[param_name] = tenants[i].db_cluster.params[
                param_name] = current_param_value

    return tenants


# TODO: support here other attrs/methods from 'Tester' class:
#       - create_stats
#       - update_test_with_errors
#       - save_email_data
#       - _check_if_db_log_time_consistency_looks_good
class MultiTenantTestMixin:
    tenants = None

    def setUp(self):  # pylint: disable=invalid-name
        super().setUp()
        self.tenants = get_tenants(self)

    @silence()
    def stop_resources(self):
        if not self.tenants:
            self.tenants = get_tenants(self)

        def _stop_resources(tenant):
            tenant.stop_resources()

        self.log.info("Running 'stop_resources' in parallel for all the tenants")
        object_set = ParallelObject(
            timeout=2100,
            objects=[[tenant] for tenant in self.tenants],
            num_workers=len(self.tenants),
        )
        object_set.run(func=_stop_resources, unpack_objects=True, ignore_exceptions=False)
