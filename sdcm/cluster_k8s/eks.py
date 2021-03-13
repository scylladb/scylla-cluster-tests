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
from typing import List, Dict, Optional, Union, Literal
from functools import cached_property, partial

import boto3
from mypy_boto3_ec2.type_defs import LaunchTemplateBlockDeviceMappingRequestTypeDef, \
    LaunchTemplateEbsBlockDeviceRequestTypeDef, RequestLaunchTemplateDataTypeDef, \
    LaunchTemplateTagSpecificationRequestTypeDef

from sdcm import sct_abs_path, cluster
from sdcm.cluster_aws import AWSCluster, MonitorSetAWS
from sdcm.localhost import LocalHost
from sdcm.utils.aws_utils import tags_as_ec2_tags, EksClusterCleanupMixin
from sdcm.utils.k8s import TokenUpdateThread
from sdcm.wait import wait_for
from sdcm.cluster_k8s import KubernetesCluster, ScyllaPodCluster, BaseScyllaPodContainer, CloudK8sNodePool, \
    SCYLLA_NAMESPACE
from sdcm.cluster_k8s.iptables import IptablesPodIpRedirectMixin, IptablesClusterOpsMixin
from sdcm.remote import LOCALRUNNER


LOGGER = logging.getLogger(__name__)


class EksNodePool(CloudK8sNodePool):
    k8s_cluster: 'EksCluster'
    disk_type: Literal["standard", "io1", "io2", "gp2", "sc1", "st1"]

    def __init__(
            self,
            k8s_cluster: 'EksCluster',
            name: str,
            num_nodes: int,
            disk_size: int,
            instance_type: str,
            role_arn: str,
            labels: dict = None,
            tags: dict = None,
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
            disk_size=disk_size,
            disk_type=disk_type,
            image_type=image_type,
            instance_type=instance_type,
            labels=labels,
            tags=tags,
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
        except Exception as exc:
            LOGGER.debug("Failed to delete nodegroup %s/%s, due to the following error:\n%s",
                         self.k8s_cluster.short_cluster_name, self.name, exc)


class EksTokenUpdateThread(TokenUpdateThread):
    update_period = 300

    def __init__(self, aws_cmd: str, kubectl_token_path: str):
        self._aws_cmd = aws_cmd
        super().__init__(kubectl_token_path=kubectl_token_path)

    def get_token(self) -> str:
        return LOCALRUNNER.run(self._aws_cmd).stdout


class EksCluster(KubernetesCluster, EksClusterCleanupMixin):
    POOL_LABEL_NAME = 'eks.amazonaws.com/nodegroup'
    USE_MONITORING_EXPOSE_SERVICE = True
    USE_POD_RESOLVER = True
    pools: Dict[str, EksNodePool]
    short_cluster_name: str

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

    def aws_cli(self, cmd) -> str:
        return LOCALRUNNER.run(cmd).stdout

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

    def create_scylla_manager_agent_config(self):
        self.update_secret_from_data('scylla-agent-config', SCYLLA_NAMESPACE, {
            'scylla-manager-agent.yaml': {
                's3': {
                    'region': self.region_name
                }
            }
        })


class EksScyllaPodContainer(BaseScyllaPodContainer, IptablesPodIpRedirectMixin):
    parent_cluster: 'EksScyllaPodCluster'
    public_ip_via_service = True

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

    def ec2_instance_destroy(self):
        if self.ec2_host:
            self.ec2_host.terminate()

    def _instance_wait_safe(self, instance_method, *args, **kwargs):
        """
        Wrapper around GCE instance methods that is safer to use.

        Let's try a method, and if it fails, let's retry using an exponential
        backoff algorithm, similar to what Amazon recommends for it's own
        service [1].

        :see: [1] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        threshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                return instance_method(*args, **kwargs)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error('Call to method %s (retries: %s) failed: %s',
                               instance_method, retries, details)
                time.sleep(min((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise cluster.NodeError('GCE instance %s method call error after '
                                    'exponential backoff wait' % self.ec2_host.id)

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
        super().terminate_k8s_node()

        # EKS does not clean up instance automatically, in order to let pool to recover it self
        # instance that holds node should be terminated

        self.ec2_instance_destroy()
        self.wait_for_k8s_node_readiness()
        self.wait_for_pod_readiness()


class EksScyllaPodCluster(ScyllaPodCluster, IptablesClusterOpsMixin):
    NODE_PREPARE_FILE = sct_abs_path("sdcm/k8s_configs/eks/scylla-node-prepare.yaml")

    k8s_cluster: 'EksCluster'
    PodContainerClass = EksScyllaPodContainer
    nodes: List[EksScyllaPodContainer]

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
        self.add_hydra_iptables_rules(nodes=new_nodes)
        self.update_nodes_iptables_redirect_rules(nodes=new_nodes, loaders=False)
        return new_nodes


class MonitorSetEKS(MonitorSetAWS):
    # On EKS nodes you can't communicate to cluster nodes outside of it, so we have to enforce using public ip
    DB_NODES_IP_ADDRESS = 'public_ip_address'

    def install_scylla_manager(self, node, auth_token):
        pass
