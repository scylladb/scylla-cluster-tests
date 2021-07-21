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
# Copyright (c) 2021 ScyllaDB
from sdcm.cluster_k8s import SCYLLA_NAMESPACE


def get_orphaned_services(db_cluster):
    pod_names = scylla_pod_names(db_cluster)
    services_names = scylla_services_names(db_cluster)
    return list(set(services_names) - set(pod_names))


def scylla_pod_names(db_cluster):
    pods = db_cluster.k8s_cluster.kubectl(f"get pods -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                          f"-o=custom-columns='NAME:.metadata.name'")

    return [name for name in pods.stdout.split() if name != 'NAME']


def scylla_services_names(db_cluster):
    services = db_cluster.k8s_cluster.kubectl(f"get svc -n {SCYLLA_NAMESPACE} -l scylla/cluster=sct-cluster "
                                              f"-o=custom-columns='NAME:.metadata.name'")
    return [name for name in services.stdout.split() if name not in ('NAME', 'sct-cluster-client')]
