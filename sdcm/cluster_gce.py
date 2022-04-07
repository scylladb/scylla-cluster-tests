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

import os
import json
import time
import logging
from typing import Dict, Any, ParamSpec, TypeVar
from textwrap import dedent
from functools import cached_property
from collections.abc import Callable

import tenacity
from libcloud.common.google import GoogleBaseError, ResourceNotFoundError, InvalidRequestError

from sdcm import cluster
from sdcm.wait import exponential_retry
from sdcm.keystore import pub_key_from_private_key_file
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.utils.common import list_instances_gce, gce_meta_to_dict
from sdcm.utils.decorators import retrying


SPOT_TERMINATION_CHECK_DELAY = 1
SPOT_TERMINATION_METADATA_CHECK_TIMEOUT = 15

LOGGER = logging.getLogger(__name__)

P = ParamSpec("P")  # pylint: disable=invalid-name
R = TypeVar("R")  # pylint: disable=invalid-name


class CreateGCENodeError(Exception):
    pass


class GCENode(cluster.BaseNode):

    """
    Wraps GCE instances, so that we can also control the instance through SSH.
    """

    log = LOGGER

    def __init__(self, gce_instance, gce_service, credentials, parent_cluster,  # pylint: disable=too-many-arguments
                 node_prefix='node', node_index=1, gce_image_username='root',
                 base_logdir=None, dc_idx=0):
        name = f"{node_prefix}-{dc_idx}-{node_index}".lower()
        self.node_index = node_index
        self._instance = gce_instance
        self._gce_service = gce_service

        ssh_login_info = {'hostname': None,
                          'user': gce_image_username,
                          'key_file': credentials.key_file,
                          'extra_ssh_options': '-tt'}
        self._preempted_last_state = False
        super().__init__(name=name,
                         parent_cluster=parent_cluster,
                         ssh_login_info=ssh_login_info,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix,
                         dc_idx=dc_idx)

    @staticmethod
    def is_gce() -> bool:
        return True

    def init(self):
        self._wait_public_ip()

        # sleep 10 seconds for waiting users are added to system
        # related issue: https://github.com/scylladb/scylla-cluster-tests/issues/1121
        time.sleep(10)

        super().init()

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _set_keep_alive(self) -> bool:
        return self._instance_wait_safe(self._gce_service.ex_set_node_labels, self._instance, {"keep": "alive"}) and \
            super()._set_keep_alive()

    def _instance_wait_safe(self, instance_method: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return exponential_retry(func=lambda: instance_method(*args, **kwargs), logger=self.log)
        except tenacity.RetryError:
            raise cluster.NodeError(
                f"Timeout while running '{instance_method.__name__}' method on GCE instance '{self._instance.id}'"
            ) from None

    def _refresh_instance_state(self):
        node_name = self._instance.name
        instance = self._instance_wait_safe(self._gce_service.ex_get_node, node_name)
        self._instance = instance
        ip_tuple = (instance.public_ips, instance.private_ips)
        return ip_tuple

    @property
    def region(self):
        return self._gce_service.region.name

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.extra['scheduling']['preemptible']

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.

        There are few different methods how to detect this event in GCE:

            https://cloud.google.com/compute/docs/instances/create-start-preemptible-instance#detecting_if_an_instance_was_preempted

        but we use internal metadata because the getting of zone operations is not implemented in Apache Libcloud yet.
        """
        try:
            result = self.remoter.run(
                'curl "http://metadata.google.internal/computeMetadata/v1/instance/preempted'
                '?wait_for_change=true&timeout_sec=%d" -H "Metadata-Flavor: Google"'
                % SPOT_TERMINATION_METADATA_CHECK_TIMEOUT, verbose=False)
            status = result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self.log.warning('Error during getting spot termination notification %s', details)
            return 0
        preempted = status.lower() == 'true'
        if preempted and not self._preempted_last_state:
            self.log.warning('Got spot termination notification from GCE')
            SpotTerminationEvent(node=self, message='Instance was preempted.').publish()
        self._preempted_last_state = preempted
        return SPOT_TERMINATION_CHECK_DELAY

    def restart(self):
        # When using local_ssd disks in GCE, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance_wait_safe(self._instance.reboot)

    def hard_reboot(self):
        self._instance_wait_safe(self._instance.reboot)

    def _safe_destroy(self):
        try:
            self._gce_service.ex_get_node(self.instance_name)
            self._instance.destroy()
        except ResourceNotFoundError:
            self.log.exception("Instance doesn't exist, skip destroy")

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance_wait_safe(self._safe_destroy)
        super().destroy()

    def get_console_output(self):
        # TODO adding console output from instance on GCE
        self.log.warning('Method is not implemented for GCENode')
        return ''

    def get_console_screenshot(self):
        # TODO adding console output from instance on GCE
        self.log.warning('Method is not implemented for GCENode')
        return b''

    def _get_ipv6_ip_address(self):
        self.log.warning('On GCE, VPC networks only support IPv4 unicast traffic. '
                         'They do not support IPv6 traffic within the network.')
        return ""

    @property
    def image(self):
        return self._instance.image


class GCECluster(cluster.BaseCluster):  # pylint: disable=too-many-instance-attributes,abstract-method

    """
    Cluster of Node objects, started on GCE (Google Compute Engine).
    """

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,  # pylint: disable=too-many-arguments
                 cluster_uuid=None, gce_instance_type='n1-standard-1', gce_region_names=None,
                 gce_n_local_ssd=1, gce_image_username='root', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=3, add_disks=None, params=None, node_type=None, service_accounts=None):

        # pylint: disable=too-many-locals
        self._gce_image = gce_image
        self._gce_image_type = gce_image_type
        self._gce_image_size = gce_image_size
        self._gce_network = gce_network
        self._gce_services = services
        self._credentials = credentials
        self._gce_instance_type = gce_instance_type
        self._gce_image_username = gce_image_username
        self._gce_region_names = gce_region_names
        self._gce_n_local_ssd = int(gce_n_local_ssd) if gce_n_local_ssd else 0
        self._add_disks = add_disks
        self._service_accounts = service_accounts
        # the full node prefix will contain unique uuid, so use this for search of existing nodes
        self._node_prefix = node_prefix
        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         # services=services,
                         region_names=gce_region_names,
                         node_type=node_type)
        self.log.debug("GCECluster constructor")

    def __str__(self):
        identifier = 'GCE Cluster %s | ' % self.name
        identifier += 'Image: %s | ' % os.path.basename(self._gce_image)
        identifier += 'Root Disk: %s %s GB | ' % (self._gce_image_type, self._gce_image_size)
        if self._gce_n_local_ssd:
            identifier += 'Local SSD: %s | ' % self._gce_n_local_ssd
        if self._add_disks:
            for disk_type, disk_size in self._add_disks.items():
                if int(disk_size):
                    identifier += '%s: %s | ' % (disk_type, disk_size)
        identifier += 'Type: %s' % self._gce_instance_type
        return identifier

    def _get_disk_url(self, disk_type='pd-standard', dc_idx=0):
        project = self._gce_services[dc_idx].ex_get_project()
        return "projects/%s/zones/%s/diskTypes/%s" % (project.name, self._gce_region_names[dc_idx], disk_type)

    def _get_root_disk_struct(self, name, disk_type='pd-standard', dc_idx=0):
        device_name = '%s-root-%s' % (name, disk_type)
        return {"type": "PERSISTENT",
                "deviceName": device_name,
                "initializeParams": {
                    # diskName parameter has a limit of 62 chars, comment it to use system allocated name
                    # "diskName": device_name,
                    "diskType": self._get_disk_url(disk_type, dc_idx=dc_idx),
                    "diskSizeGb": self._gce_image_size,
                    "sourceImage": self._gce_image
                },
                "boot": True,
                "autoDelete": True}

    def _get_local_ssd_disk_struct(self, name, index, interface='NVME', dc_idx=0):
        device_name = '%s-data-local-ssd-%s' % (name, index)
        return {"type": "SCRATCH",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url('local-ssd', dc_idx=dc_idx),
                },
                "interface": interface,
                "autoDelete": True}

    def _get_persistent_disk_struct(self, name, disk_size, disk_type='pd-ssd', dc_idx=0):
        device_name = '%s-data-%s' % (name, disk_type)
        return {"type": "PERSISTENT",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url(disk_type, dc_idx=dc_idx),
                    "diskSizeGb": disk_size,
                },
                "autoDelete": True}

    def _get_disks_struct(self, name, dc_idx):
        gce_disk_struct = [self._get_root_disk_struct(name=name,
                                                      disk_type=self._gce_image_type,
                                                      dc_idx=dc_idx)]
        for i in range(self._gce_n_local_ssd):
            gce_disk_struct.append(self._get_local_ssd_disk_struct(name=name, index=i, dc_idx=dc_idx))
        if self._add_disks:
            for disk_type, disk_size in self._add_disks.items():
                disk_size = int(disk_size)
                if disk_size:
                    gce_disk_struct.append(self._get_persistent_disk_struct(name=name, disk_size=disk_size,
                                                                            disk_type=disk_type, dc_idx=dc_idx))
        self.log.debug(gce_disk_struct)
        return gce_disk_struct

    def _create_instance(self, node_index, dc_idx, spot=False):
        def set_tags_as_labels(_instance):
            self.log.debug(f"Expected tags are {self.tags}")
            lower_tags = dict((k.lower(), v.lower().replace('.', '-')) for k, v in self.tags.items())
            self.log.debug(f"Lower tags are {lower_tags}")
            self._gce_services[dc_idx].ex_set_node_labels(_instance, lower_tags)

        # if size of disk is larget than 80G, then
        # change the timeout of job completion to default * 3.

        gce_job_default_timeout = None
        if self._gce_image_size and int(self._gce_image_size) > 80:
            gce_job_default_timeout = self._gce_services[dc_idx].connection.timeout
            self._gce_services[dc_idx].connection.timeout = gce_job_default_timeout * 3
            self.log.info("Job complete timeout is set to %ss" %
                          self._gce_services[dc_idx].connection.timeout)
        name = f"{self.node_prefix}-{dc_idx}-{node_index}".lower()
        gce_disk_struct = self._get_disks_struct(name=name, dc_idx=dc_idx)
        # Name must start with a lowercase letter followed by up to 63
        # lowercase letters, numbers, or hyphens, and cannot end with a hyphen
        assert len(name) <= 63, "Max length of instance name is 63"
        startup_script = self.test_config.get_startup_script()

        if self.params.get("scylla_linux_distro") in ("ubuntu-bionic", "ubuntu-xenial", "ubuntu-focal",):
            # we need to disable sshguard to prevent blocking connections from the builder
            startup_script += dedent("""
                sudo systemctl disable sshguard
                sudo systemctl stop sshguard
            """)
        username = self.params.get("gce_image_username")
        public_key = pub_key_from_private_key_file(self.params.get("user_credentials_path"))
        raid_level = self.params.get("raid_level")
        create_node_params = dict(name=name,
                                  size=self._gce_instance_type,
                                  image=self._gce_image,
                                  ex_network=self._gce_network,
                                  ex_disks_gce_struct=gce_disk_struct,
                                  ex_metadata={**self.tags,
                                               "Name": name,
                                               "NodeIndex": node_index,
                                               "startup-script": startup_script,
                                               "user-data": json.dumps(dict(scylla_yaml=dict(cluster_name=self.name),
                                                                            start_scylla_on_first_boot=False,
                                                                            raid_level=raid_level)),
                                               "block-project-ssh-keys": "true",
                                               "ssh-keys": f"{username}:ssh-rsa {public_key}", },
                                  ex_service_accounts=self._service_accounts,
                                  ex_preemptible=spot)

        instance = self._create_node_with_retries(name=name,
                                                  dc_idx=dc_idx,
                                                  create_node_params=create_node_params,
                                                  spot=spot)

        self.log.info('Created %s instance %s', 'spot' if spot else 'on-demand', instance)
        try:
            set_tags_as_labels(instance)
        except InvalidRequestError as exc:
            self.log.warning(f"Unable to set tags as labels due to {exc}")

        if gce_job_default_timeout:
            self.log.info('Restore default job timeout %s' % gce_job_default_timeout)
            self._gce_services[dc_idx].connection.timeout = gce_job_default_timeout
        return instance

    @retrying(n=3,
              sleep_time=900,
              allowed_exceptions=(GoogleBaseError, ResourceNotFoundError),
              message="Retrying to create a GCE node...",
              raise_on_exceeded=True)
    def _create_node_with_retries(self,
                                  name: str,
                                  dc_idx: int,
                                  create_node_params: dict[str, Any],
                                  spot: bool = False) -> GCENode:
        try:
            return self._gce_services[dc_idx].create_node(**create_node_params)
        except GoogleBaseError as gbe:
            if not spot:
                raise

            #  attempt to destroy if node did not start due to preemption
            if "Instance failed to start due to preemption" in str(gbe):
                try:
                    self._destroy_instance(name=name, dc_idx=dc_idx)
                except ResourceNotFoundError:
                    LOGGER.warning("Attempted to destroy node: {name: %s, idx:%s}, but could not find it. "
                                   "Possibly the node was destroyed already.", name, dc_idx)

            create_node_params['ex_preemptible'] = spot = False

            raise gbe

    def _create_instances(self, count, dc_idx=0):
        spot = 'spot' in self.instance_provision
        instances = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            instances.append(self._create_instance(node_index, dc_idx, spot))
        return instances

    def _destroy_instance(self, name: str, dc_idx: int) -> bool:
        target_node = self._get_instances_by_name(dc_idx=dc_idx, name=name)

        return self._gce_services[dc_idx].destroy_node(node=target_node, destroy_boot_disk=True)

    def _get_instances_by_prefix(self, dc_idx: int = 0):
        instances_by_zone = self._gce_services[dc_idx].list_nodes(ex_zone=self._gce_region_names[dc_idx])
        return [node for node in instances_by_zone if node.name.startswith(self._node_prefix)]

    def _get_instances_by_name(self, name: str, dc_idx: int = 0):
        instances_by_zone = self._gce_services[dc_idx].list_nodes(ex_zone=self._gce_region_names[dc_idx])
        found = [node for node in instances_by_zone if node.name == name]
        return found[0] if found else None

    def _get_instances(self, dc_idx):
        test_id = self.test_config.test_id()
        if not test_id:
            raise ValueError("test_id should be configured for using reuse_cluster")
        instances_by_nodetype = list_instances_gce(tags_dict={'TestId': test_id, 'NodeType': self.node_type})
        instances_by_zone = self._get_instances_by_prefix(dc_idx)
        instances = []
        attr_name = 'public_ips' if self._node_public_ips else 'private_ips'
        for node_zone in instances_by_zone:
            # Filter nodes by zone and by ip addresses
            if not getattr(node_zone, attr_name):
                continue
            for node_nodetype in instances_by_nodetype:
                if node_zone.uuid == node_nodetype.uuid:
                    instances.append(node_zone)

        def sort_by_index(node):
            metadata = gce_meta_to_dict(node.extra['metadata'])
            return metadata.get('NodeIndex', 0)

        instances = sorted(instances, key=sort_by_index)
        return instances

    def _create_node(self, instance, node_index, dc_idx):
        try:
            node = GCENode(gce_instance=instance,
                           gce_service=self._gce_services[dc_idx],
                           credentials=self._credentials[0],
                           parent_cluster=self,
                           gce_image_username=self._gce_image_username,
                           node_prefix=self.node_prefix,
                           node_index=node_index,
                           base_logdir=self.logdir,
                           dc_idx=dc_idx)
            node.init()
            return node
        except Exception as ex:
            raise CreateGCENodeError('Failed to create node: %s' % ex) from ex

    # pylint: disable=too-many-arguments
    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False):
        if count <= 0:
            return []
        self.log.info("Adding nodes to cluster")
        nodes = []
        if self.test_config.REUSE_CLUSTER:
            instances = self._get_instances(dc_idx)
            if not instances:
                raise RuntimeError("No nodes found for testId %s " % (self.test_config.test_id(),))
        else:
            instances = self._create_instances(count, dc_idx)

        self.log.debug('instances: %s', instances)
        if instances:
            self.log.debug('GCE instance extra info: %s', instances[0].extra)
        for node_index, instance in enumerate(instances, start=self._node_index + 1):
            node = self._create_node(instance, node_index, dc_idx)
            nodes.append(node)
            self.nodes.append(node)
            self.log.info("Added node: %s", node.name)
            node.enable_auto_bootstrap = enable_auto_bootstrap

        self._node_index += count
        self.log.info('added nodes: %s', nodes)
        return nodes


class ScyllaGCECluster(cluster.BaseScyllaCluster, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos',
                 user_prefix=None, n_nodes=3, add_disks=None, params=None, gce_datacenter=None, service_accounts=None):
        # pylint: disable=too-many-locals
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        super().__init__(
            gce_image=gce_image,
            gce_image_type=gce_image_type,
            gce_image_size=gce_image_size,
            gce_n_local_ssd=gce_n_local_ssd,
            gce_network=gce_network,
            gce_instance_type=gce_instance_type,
            gce_image_username=gce_image_username,
            services=services,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            add_disks=add_disks,
            params=params,
            gce_region_names=gce_datacenter,
            node_type='scylla-db',
            service_accounts=service_accounts,
        )
        self.version = '2.1'

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()


class LoaderSetGCE(cluster.BaseLoaderSet, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos',
                 user_prefix=None, n_nodes=10, add_disks=None, params=None, gce_datacenter=None):
        # pylint: disable=too-many-locals
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')
        cluster.BaseLoaderSet.__init__(self,
                                       params=params)
        GCECluster.__init__(self,
                            gce_image=gce_image,
                            gce_network=gce_network,
                            gce_image_type=gce_image_type,
                            gce_image_size=gce_image_size,
                            gce_n_local_ssd=gce_n_local_ssd,
                            gce_instance_type=gce_instance_type,
                            gce_image_username=gce_image_username,
                            services=service,
                            credentials=credentials,
                            cluster_prefix=cluster_prefix,
                            node_prefix=node_prefix,
                            n_nodes=n_nodes,
                            add_disks=add_disks,
                            params=params,
                            node_type='loader',
                            gce_region_names=gce_datacenter
                            )


class MonitorSetGCE(cluster.BaseMonitorSet, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,  # pylint: disable=too-many-arguments
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos', user_prefix=None, n_nodes=1,
                 targets=None, add_disks=None, params=None, gce_datacenter=None):
        # pylint: disable=too-many-locals
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')

        targets = targets if targets else {}
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        GCECluster.__init__(self,
                            gce_image=gce_image,
                            gce_image_type=gce_image_type,
                            gce_image_size=gce_image_size,
                            gce_n_local_ssd=gce_n_local_ssd,
                            gce_network=gce_network,
                            gce_instance_type=gce_instance_type,
                            gce_image_username=gce_image_username,
                            services=service,
                            credentials=credentials,
                            cluster_prefix=cluster_prefix,
                            node_prefix=node_prefix,
                            n_nodes=n_nodes,
                            add_disks=add_disks,
                            params=params,
                            node_type='monitor',
                            gce_region_names=gce_datacenter
                            )
