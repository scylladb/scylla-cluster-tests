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
import base64
import os
import logging
import time
from textwrap import dedent
from typing import List, Dict, Optional, Union, Literal, ParamSpec, TypeVar
from functools import cached_property
from collections.abc import Callable

import boto3
import tenacity
from mypy_boto3_ec2.type_defs import LaunchTemplateBlockDeviceMappingRequestTypeDef, \
    LaunchTemplateEbsBlockDeviceRequestTypeDef, RequestLaunchTemplateDataTypeDef, \
    LaunchTemplateTagSpecificationRequestTypeDef

from sdcm import sct_abs_path, cluster
from sdcm.cluster_aws import MonitorSetAWS
from sdcm.utils.aws_utils import tags_as_ec2_tags, EksClusterCleanupMixin
from sdcm.utils.k8s import TokenUpdateThread
from sdcm.wait import wait_for, exponential_retry
from sdcm.cluster_k8s import (
    BaseScyllaPodContainer,
    CloudK8sNodePool,
    KubernetesCluster,
    ScyllaPodCluster,
)
from sdcm.cluster_k8s.iptables import IptablesPodIpRedirectMixin, IptablesClusterOpsMixin
from sdcm.remote import LOCALRUNNER


LOGGER = logging.getLogger(__name__)

P = ParamSpec("P")  # pylint: disable=invalid-name
R = TypeVar("R")  # pylint: disable=invalid-name


# pylint: disable=too-many-instance-attributes
class EksNodePool(CloudK8sNodePool):
    k8s_cluster: 'EksCluster'
    disk_type: Literal["standard", "io1", "io2", "gp2", "sc1", "st1"]

    # pylint: disable=too-many-arguments,too-many-locals
    def __init__(
            self,
            k8s_cluster: 'EksCluster',
            name: str,
            num_nodes: int,
            disk_size: int,
            instance_type: str,
            role_arn: str,
            labels: dict = None,
            security_group_ids: List[str] = None,
            ec2_subnet_ids: List[str] = None,
            ssh_key_pair_name: str = None,
            provision_type: Literal['ON_DEMAND', 'SPOT'] = 'ON_DEMAND',
            launch_template: str = None,
            image_type: Literal['AL2_x86_64', 'AL2_x86_64_GPU', 'AL2_ARM_64'] = 'AL2_x86_64',
            disk_type: Literal["standard", "io1", "io2", "gp2", "sc1", "st1"] = None,
            k8s_version: str = None,
            is_deployed: bool = False,
            user_data: str = None,
    ):
        super().__init__(
            k8s_cluster=k8s_cluster,
            name=name,
            num_nodes=num_nodes,
            instance_type=instance_type,
            image_type=image_type,
            disk_size=disk_size,
            disk_type=disk_type,
            labels=labels,
            is_deployed=is_deployed,
        )
        self.role_arn = role_arn
        self.ec2_subnet_ids = self.k8s_cluster.ec2_subnet_ids if ec2_subnet_ids is None else ec2_subnet_ids
        self.ssh_key_pair_name = os.path.basename(
            self.k8s_cluster.credentials[0].key_pair_name) if ssh_key_pair_name is None else ssh_key_pair_name
        self.security_group_ids = \
            self.k8s_cluster.ec2_security_group_ids[0] if security_group_ids is None else security_group_ids
        self.provision_type = provision_type
        self.launch_template = launch_template
        self.k8s_version = self.k8s_cluster.eks_cluster_version if k8s_version is None else k8s_version
        self.user_data = user_data

    @property
    def launch_template_name(self) -> str:
        return f'sct-{self.k8s_cluster.short_cluster_name}-{self.name}'

    @property
    def is_launch_template_required(self) -> bool:
        return bool(self.user_data)

    @property
    def _launch_template_cfg(self) -> dict:
        block_devices = []
        if self.disk_size:
            root_disk_def = LaunchTemplateBlockDeviceMappingRequestTypeDef(
                DeviceName='/dev/xvda',
                Ebs=LaunchTemplateEbsBlockDeviceRequestTypeDef(
                    DeleteOnTermination=True,
                    Encrypted=False,
                    VolumeSize=self.disk_size,
                    VolumeType=self.disk_type
                ))
            block_devices.append(root_disk_def)
        launch_template = RequestLaunchTemplateDataTypeDef(
            KeyName=self.ssh_key_pair_name,
            EbsOptimized=False,
            BlockDeviceMappings=block_devices,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "SubnetId": self.ec2_subnet_ids[0],
            }],
        )
        if self.user_data:
            launch_template['UserData'] = base64.b64encode(self.user_data.encode('utf-8')).decode("ascii")
        if self.tags:
            launch_template['TagSpecifications'] = [LaunchTemplateTagSpecificationRequestTypeDef(
                ResourceType="instance",
                Tags=tags_as_ec2_tags(self.tags)
            )]
        return launch_template

    @property
    def _node_group_cfg(self) -> dict:
        labels = {} if self.labels is None else self.labels
        tags = {} if self.tags is None else self.tags
        node_labels = labels.copy()
        node_labels['node-pool'] = self.name
        node_pool_config = {
            'clusterName': self.k8s_cluster.short_cluster_name,
            'nodegroupName': self.name,
            'scalingConfig': {
                'minSize': self.num_nodes,
                'maxSize': self.num_nodes,
                'desiredSize': self.num_nodes
            },
            # subnets controls AZ placement, if you specify subnets from multiple AZ
            # nodes will be spread across these AZ evenly, which can lead to failures and/or slowdowns
            'subnets': [self.ec2_subnet_ids[0]],
            'instanceTypes': [self.instance_type],
            'amiType': self.image_type,
            'nodeRole': self.role_arn,
            'labels': labels,
            'tags': tags,
            'capacityType': self.provision_type.upper(),
            'version': self.k8s_version
        }
        if self.is_launch_template_required:
            node_pool_config['launchTemplate'] = {'name': self.launch_template_name}
        else:
            node_pool_config['diskSize'] = self.disk_size
            node_pool_config['remoteAccess'] = {
                'ec2SshKey': self.ssh_key_pair_name,
            }
        return node_pool_config

    def deploy(self) -> None:
        LOGGER.info("Deploy %s node pool with %d node(s)", self.name, self.num_nodes)
        if self.is_launch_template_required:
            LOGGER.info("Deploy launch template %s", self.launch_template_name)
            self.k8s_cluster.ec2_client.create_launch_template(
                LaunchTemplateName=self.launch_template_name,
                LaunchTemplateData=self._launch_template_cfg,
            )
        self.k8s_cluster.eks_client.create_nodegroup(**self._node_group_cfg)
        self.is_deployed = True

    def resize(self, num_nodes):
        LOGGER.info("Resize %s pool to %d node(s)", self.name, num_nodes)
        self.k8s_cluster.eks_client.update_nodegroup_config(
            clusterName=self.k8s_cluster.short_cluster_name,
            nodegroupName=self.name,
            scalingConfig={
                'minSize': num_nodes,
                'maxSize': num_nodes,
                'desiredSize': num_nodes
            },
        )
        self.num_nodes = num_nodes

    def undeploy(self):
        try:
            self.k8s_cluster.eks_client.delete_nodegroup(
                clusterName=self.k8s_cluster.short_cluster_name,
                nodegroupName=self.name)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.debug("Failed to delete nodegroup %s/%s, due to the following error:\n%s",
                         self.k8s_cluster.short_cluster_name, self.name, exc)


class EksTokenUpdateThread(TokenUpdateThread):
    update_period = 300

    def __init__(self, aws_cmd: str, kubectl_token_path: str):
        self._aws_cmd = aws_cmd
        super().__init__(kubectl_token_path=kubectl_token_path)

    def get_token(self) -> str:
        return LOCALRUNNER.run(self._aws_cmd).stdout


# pylint: disable=too-many-instance-attributes
class EksCluster(KubernetesCluster, EksClusterCleanupMixin):
    POOL_LABEL_NAME = 'eks.amazonaws.com/nodegroup'
    IS_NODE_TUNING_SUPPORTED = True
    pools: Dict[str, EksNodePool]
    short_cluster_name: str

    # pylint: disable=too-many-arguments
    def __init__(self,
                 eks_cluster_version,
                 ec2_security_group_ids,
                 ec2_subnet_ids,
                 ec2_role_arn,
                 credentials,
                 user_prefix,
                 service_ipv4_cidr,
                 vpc_cni_version,
                 nodegroup_role_arn,
                 params=None,
                 cluster_uuid=None,
                 region_name=None
                 ):
        super().__init__(user_prefix=user_prefix, cluster_uuid=cluster_uuid, region_name=region_name, params=params)
        self.credentials = credentials
        self.eks_cluster_version = eks_cluster_version
        self.ec2_role_arn = ec2_role_arn
        self.nodegroup_role_arn = nodegroup_role_arn
        self.ec2_security_group_ids = ec2_security_group_ids
        self.ec2_subnet_ids = ec2_subnet_ids
        self.service_ipv4_cidr = service_ipv4_cidr
        self.vpc_cni_version = vpc_cni_version

    @cached_property
    def allowed_labels_on_scylla_node(self) -> list:
        allowed_labels_on_scylla_node = [
            ('name', 'node-setup'),
            ('name', 'cpu-policy'),
            ('k8s-app', 'aws-node'),
            ('app', 'local-volume-provisioner'),
            ('k8s-app', 'kube-proxy'),
            ('scylla/cluster', self.k8s_scylla_cluster_name),
        ]
        if self.is_performance_tuning_enabled:
            # NOTE: add performance tuning related pods only if we expect it to be.
            #       When we have tuning disabled it must not exist.
            allowed_labels_on_scylla_node.extend(self.perf_pods_labels)
        return allowed_labels_on_scylla_node

    def create_eks_cluster(self, wait_till_functional=True):
        self.eks_client.create_cluster(
            name=self.short_cluster_name,
            version=self.eks_cluster_version,
            roleArn=self.ec2_role_arn,
            resourcesVpcConfig={
                'securityGroupIds': self.ec2_security_group_ids[0],
                'subnetIds': self.ec2_subnet_ids,
                'endpointPublicAccess': True,
                'endpointPrivateAccess': True,
                'publicAccessCidrs': [
                    '0.0.0.0/0',
                ]
            },
            kubernetesNetworkConfig={
                'serviceIpv4Cidr': self.service_ipv4_cidr
            },
            logging={
                'clusterLogging': [
                    {
                        'types': [
                            'api',
                            'audit',
                            'authenticator',
                            'controllerManager',
                            'scheduler'
                        ],
                        'enabled': True
                    },
                ]
            },
            tags=self.tags,
        )
        self.eks_client.create_addon(
            clusterName=self.short_cluster_name,
            addonName='vpc-cni',
            addonVersion=self.vpc_cni_version
        )
        if wait_till_functional:
            wait_for(lambda: self.cluster_status == 'ACTIVE', step=60, throw_exc=True, timeout=1200,
                     text=f'Waiting till EKS cluster {self.short_cluster_name} become operational')

    @property
    def cluster_info(self) -> dict:
        return self.eks_client.describe_cluster(name=self.short_cluster_name)['cluster']

    @property
    def cluster_status(self) -> str:
        return self.cluster_info['status']

    def __str__(self):
        return f"{type(self).__name__} {self.name} | Version: {self.eks_cluster_version}"

    def create_token_update_thread(self):
        return EksTokenUpdateThread(
            aws_cmd=f'aws eks --region {self.region_name} get-token --cluster-name {self.short_cluster_name}',
            kubectl_token_path=self.kubectl_token_path
        )

    def create_kubectl_config(self):
        LOCALRUNNER.run(f'aws eks --region {self.region_name} update-kubeconfig --name {self.short_cluster_name}')

    def deploy(self):
        LOGGER.info("Create EKS cluster `%s'", self.short_cluster_name)
        self.create_eks_cluster()
        LOGGER.info("Patch kubectl config")
        self.patch_kubectl_config()

    def tune_network(self):
        """Tune networking on all nodes of an EKS cluster to reduce number of reserved IPs.

        Set following special EKS-specific env vars to 'aws-node' daemonset:

            WARM_ENI_TARGET (default is 1) - number of 'ready-to-use' additional
                network interfaces which in our case almost always stay idle.
                Setting this one to 0 means we have 1 network interface at start,
                and new ones will be added (expensive API calls) only when needed automatically.
            MINIMUM_IP_TARGET (no default) - minimum number of IP addresses that must be
                dedicated to an EC2 instance. If not set then number of reserved IPs equal
                to number of IP capacity of a network interface.
            WARM_IP_TARGET (no default) - how many unused IP must be kept as 'ready-to-use'.
                if not set then depend on the available IPs of ENIs.

        General idea behind it: reduce number of 'reserved' IP addresses by each of
        EC2 instances used in an EKS cluster.
        Without such tweaking one cluster we create uses more than 300 IP addresses
        and having /22 subnet (<1024 IP addresses) we can run just 3 EKS clusters at once.

        Env vars details:
            https://github.com/aws/amazon-vpc-cni-k8s/blob/master/docs/eni-and-ip-target.md
        IPs per network interface per node type details:
            https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html
        """
        LOGGER.info("Tune network of the EKS cluster")
        env_vars = (
            "WARM_ENI_TARGET=0",
            "MINIMUM_IP_TARGET=8",
            "WARM_IP_TARGET=2",
        )
        self.kubectl(f"set env daemonset aws-node {' '.join(env_vars)}", namespace="kube-system")

    def deploy_node_pool(self, pool: EksNodePool, wait_till_ready=True) -> None:
        self._add_pool(pool)
        if pool.is_deployed:
            return
        if wait_till_ready:
            pool.deploy_and_wait_till_ready()
        else:
            pool.deploy()

    def resize_node_pool(self, name: str, num_nodes: int, wait_till_ready=True) -> None:
        self.pools[name].resize(num_nodes)
        if wait_till_ready:
            self.pools[name].wait_for_nodes_readiness()

    def destroy(self):
        EksClusterCleanupMixin.destroy(self)
        self.stop_token_update_thread()

    def get_ec2_instance_by_id(self, instance_id):
        return boto3.resource('ec2', region_name=self.region_name).Instance(id=instance_id)

    def deploy_scylla_manager(self, pool_name: str = None) -> None:
        self.deploy_minio_s3_backend()
        super().deploy_scylla_manager(pool_name=pool_name)

    def _get_all_instance_ids(self):
        cmd = "get node --no-headers -o custom-columns=:.spec.providerID"
        return [name.split("/")[-1] for name in self.kubectl(cmd).stdout.split()]

    def set_tags(self, instance_ids):
        if isinstance(instance_ids, str):
            instance_ids = [instance_ids]
        boto3.client('ec2', region_name=self.region_name).create_tags(
            Resources=instance_ids,
            Tags=[{"Key": key, "Value": value} for key, value in self.tags.items()],
        )

    def set_tags_on_all_instances(self):
        # NOTE: EKS doesn't apply nodeGroup's tags to nodes.
        # So, we add it for each node explicitly.
        self.set_tags(self._get_all_instance_ids())

    def set_security_groups(self, instance):
        for network_interface in instance.network_interfaces:
            security_groups = [g["GroupId"] for g in network_interface.groups]
            # NOTE: Make API call only if it is needed
            if self.ec2_security_group_ids[0][0] not in security_groups:
                security_groups.append(self.ec2_security_group_ids[0][0])
                network_interface.modify_attribute(Groups=security_groups)

    def set_security_groups_on_all_instances(self):
        # NOTE: EKS doesn't apply nodeGroup's security groups to nodes
        # So, we add it for each network interface of a node explicitly.
        for instance_id in self._get_all_instance_ids():
            self.set_security_groups(self.get_ec2_instance_by_id(instance_id))

    def deploy_scylla_cluster(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        super().deploy_scylla_cluster(*args, **kwargs)
        self.set_security_groups_on_all_instances()
        self.set_tags_on_all_instances()

    def deploy_monitoring_cluster(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        super().deploy_monitoring_cluster(*args, **kwargs)
        self.set_security_groups_on_all_instances()
        self.set_tags_on_all_instances()

    def upgrade_kubernetes_platform(self) -> str:
        upgrade_version = f"1.{int(self.eks_cluster_version.split('.')[1]) + 1}"

        # Upgrade control plane (API, scheduler, manager and so on ...)
        LOGGER.info("Upgrading K8S control plane to the '%s' version", upgrade_version)
        self.eks_client.update_cluster_version(
            name=self.short_cluster_name,
            version=upgrade_version,
        )
        # NOTE: sleep for some small period of time to make sure that cluster's status changes
        #       before we poll for it's readiness.
        #       Upgrade takes dozens of minutes, so, 'sleep'ing for some time won't cause
        #       time waste by calling 'time.sleep' function.
        #       5sec is not enough in some cases. 20sec must be sufficient
        time.sleep(20)
        self.eks_client.get_waiter('cluster_active').wait(
            name=self.short_cluster_name,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 120},
        )

        # Upgrade scylla-related node pools
        for node_pool in (self.AUXILIARY_POOL_NAME, self.SCYLLA_POOL_NAME):
            LOGGER.info("Upgrading '%s' node pool to the '%s' version", node_pool, upgrade_version)
            self.eks_client.update_nodegroup_version(
                clusterName=self.short_cluster_name,
                nodegroupName=node_pool,
                version=upgrade_version,
            )
            time.sleep(20)
            self.eks_client.get_waiter('nodegroup_active').wait(
                clusterName=self.short_cluster_name,
                nodegroupName=node_pool,
                WaiterConfig={
                    # NOTE: one Scylla K8S node upgrade takes about 10 minutes
                    #       So, wait timeout will be different for different number of DB nodes
                    #       Set it bigger than the expected value to avoid possible fluctuations
                    'Delay': 30,
                    'MaxAttempts': self.params.get("n_db_nodes") * 30,
                },
            )
        return upgrade_version


class EksScyllaPodContainer(BaseScyllaPodContainer, IptablesPodIpRedirectMixin):
    parent_cluster: 'EksScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @cached_property
    def hydra_dest_ip(self) -> str:
        return self.public_ip_address

    @cached_property
    def nodes_dest_ip(self) -> str:
        return self.public_ip_address

    @property
    def ec2_host(self):
        return self.parent_cluster.k8s_cluster.get_ec2_instance_by_id(self.ec2_instance_id)

    @property
    def ec2_instance_id(self):
        return self._node.spec.provider_id.split('/')[-1]

    def terminate_k8s_host(self):
        self.log.info('terminate_k8s_host: EC2 instance of kubernetes node will be terminated, '
                      'the following is affected :\n' + dedent('''
            EC2 instance  X  <-
            K8s node      X
            Scylla Pod    X
            Scylla node   X
            '''))
        self._instance_wait_safe(self.ec2_instance_destroy)
        self.wait_for_k8s_node_readiness()
        self.parent_cluster.k8s_cluster.set_security_groups_on_all_instances()
        self.parent_cluster.k8s_cluster.set_tags_on_all_instances()

    def ec2_instance_destroy(self, ec2_host=None):
        ec2_host = ec2_host or self.ec2_host
        if ec2_host:
            ec2_host.terminate()

    def _instance_wait_safe(self, instance_method: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return exponential_retry(func=lambda: instance_method(*args, **kwargs), logger=self.log)
        except tenacity.RetryError:
            raise cluster.NodeError(
                f"Timeout while running '{instance_method.__name__}' method on AWS instance '{self.ec2_host.id}'"
            ) from None

    def terminate_k8s_node(self):
        """
        Delete kubernetes node, which will terminate scylla node that is running on it
        """
        self.log.info('terminate_k8s_node: kubernetes node will be deleted, the following is affected :\n' + dedent('''
            EC2 instance    X  <-
            K8s node        X  <-
            Scylla Pod      X
            Scylla node     X
            '''))
        ec2_host = self.ec2_host
        super().terminate_k8s_node()

        # EKS does not clean up instance automatically, in order to let pool to recover it self
        # instance that holds node should be terminated
        self.ec2_instance_destroy(ec2_host=ec2_host)


class EksScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    NODE_PREPARE_FILE = sct_abs_path("sdcm/k8s_configs/eks/scylla-node-prepare.yaml")
    node_terminate_methods = [
        'drain_k8s_node',
        # NOTE: uncomment below when following scylla-operator bug is fixed:
        #       https://github.com/scylladb/scylla-operator/issues/643
        # 'terminate_k8s_host',
        # 'terminate_k8s_node',
    ]
    k8s_cluster: 'EksCluster'
    PodContainerClass = EksScyllaPodContainer
    nodes: List[EksScyllaPodContainer]

    # pylint: disable=too-many-arguments
    def __init__(self,
                 k8s_cluster: KubernetesCluster,
                 scylla_cluster_name: Optional[str] = None,
                 user_prefix: Optional[str] = None,
                 n_nodes: Union[list, int] = 3,
                 params: Optional[dict] = None,
                 node_pool: EksNodePool = None,
                 ) -> None:
        super().__init__(k8s_cluster=k8s_cluster, scylla_cluster_name=scylla_cluster_name, user_prefix=user_prefix,
                         n_nodes=n_nodes, params=params, node_pool=node_pool)
    # pylint: disable=too-many-arguments

    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = 0,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False) -> List[EksScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap)
        for node in new_nodes:
            self.k8s_cluster.set_security_groups(node.ec2_host)
            self.k8s_cluster.set_tags(node.ec2_host.id)
        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes, loaders=False)
        return new_nodes


class MonitorSetEKS(MonitorSetAWS):
    # On EKS nodes you can't communicate to cluster nodes outside of it, so we have to enforce using public ip
    DB_NODES_IP_ADDRESS = 'public_ip_address'

    def install_scylla_manager(self, node):
        pass
