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
import time
import pprint
from textwrap import dedent
from threading import Lock
from typing import List, Dict, Literal, ParamSpec, TypeVar
from functools import cached_property
from collections.abc import Callable

import boto3
import tenacity
from mypy_boto3_ec2.type_defs import LaunchTemplateBlockDeviceMappingRequestTypeDef, \
    LaunchTemplateEbsBlockDeviceRequestTypeDef, RequestLaunchTemplateDataTypeDef, \
    LaunchTemplateTagSpecificationRequestTypeDef
from botocore.exceptions import ClientError

from sdcm import sct_abs_path, cluster
from sdcm.cluster_aws import MonitorSetAWS
from sdcm import ec2_client
from sdcm.utils.ci_tools import get_test_name
from sdcm.utils.common import list_instances_aws
from sdcm.utils.k8s import TokenUpdateThread
from sdcm.wait import wait_for, exponential_retry
from sdcm.cluster_k8s import (
    BaseScyllaPodContainer,
    CloudK8sNodePool,
    KubernetesCluster,
    ScyllaPodCluster,
    SCYLLA_AGENT_CONFIG_NAME,
    SCYLLA_NAMESPACE,
)
from sdcm.remote import LOCALRUNNER
from sdcm.utils.aws_utils import (
    get_arch_from_instance_type,
    get_ec2_network_configuration,
    tags_as_ec2_tags,
    EksClusterCleanupMixin,
)
from sdcm.utils.nemesis_utils.node_allocator import mark_new_nodes_as_running_nemesis


P = ParamSpec("P")
R = TypeVar("R")

# we didn't add configuration for all the rest 'io1', 'io2', 'gp2', 'sc1', 'st1'
SUPPORTED_EBS_STORAGE_CLASSES = ['gp3', ]

EC2_INSTANCE_UPDATE_LOCK = Lock()

ARCH_TO_IMAGE_TYPE_MAPPING = {'arm64': 'AL2_ARM_64', 'x86_64': 'AL2_x86_64'}


def init_k8s_eks_cluster(region_name: str, availability_zone: str, params: dict,
                         credentials: List[cluster.UserRemoteCredentials],
                         cluster_uuid: str = None):
    """Dedicated for the usage by the 'Tester' class which orchestrates all the resources creation.

    Return the 'k8s_cluster' object back to the 'Tester' as soon as possible to be able to trigger
    logs gathering operations in case resources provisioning and/or setup fail.
    The 'Tester' should then call the 'deploy_k8s_eks_cluster' function
    providing initialized 'k8s_cluster' object.
    """
    second_availability_zone = "b" if availability_zone == "a" else "a"
    availability_zones = [availability_zone, second_availability_zone]
    ec2_security_group_ids, ec2_subnet_ids = get_ec2_network_configuration(
        regions=[region_name], availability_zones=availability_zones, params=params)
    return EksCluster(
        eks_cluster_version=params.get("eks_cluster_version"),
        ec2_security_group_ids=ec2_security_group_ids,
        ec2_subnet_ids=ec2_subnet_ids[0],
        credentials=credentials,
        ec2_role_arn=params.get("eks_role_arn"),
        nodegroup_role_arn=params.get("eks_nodegroup_role_arn"),
        service_ipv4_cidr=params.get("eks_service_ipv4_cidr"),
        vpc_cni_version=params.get("eks_vpc_cni_version"),
        user_prefix=params.get("user_prefix"),
        cluster_uuid=cluster_uuid,
        params=params,
        region_name=region_name)


def deploy_k8s_eks_cluster(k8s_cluster) -> None:
    """Dedicated for the usage by the 'Tester' class which orchestrates all the resources creation.

    This function accepts the initialized 'k8s_cluster' object
    and then creates all the needed node pools and other ecosystem workloads.
    """
    params = k8s_cluster.params
    k8s_cluster.deploy()
    k8s_cluster.tune_network()
    k8s_cluster.set_nodeselector_for_deployments(
        pool_name=k8s_cluster.AUXILIARY_POOL_NAME, namespace="kube-system")

    k8s_cluster.deploy_node_pool(wait_till_ready=False, pool=EksNodePool(
        name=k8s_cluster.AUXILIARY_POOL_NAME,
        # NOTE: It should have at least 5 vCPU to be able to hold all the pods
        num_nodes=params.get('k8s_n_auxiliary_nodes'),
        instance_type=params.get('k8s_instance_type_auxiliary'),
        disk_size=40,
        role_arn=k8s_cluster.nodegroup_role_arn,
        k8s_cluster=k8s_cluster))

    # TODO: add support for different DB nodes amount in different K8S clusters
    scylla_pool = EksNodePool(
        name=k8s_cluster.SCYLLA_POOL_NAME,
        num_nodes=params.get("n_db_nodes"),
        instance_type=params.get('instance_type_db'),
        role_arn=params.get('eks_nodegroup_role_arn'),
        disk_size=params.get('root_disk_size_db'),
        k8s_cluster=k8s_cluster)
    k8s_cluster.deploy_node_pool(scylla_pool, wait_till_ready=False)

    # TODO: add support for different loaders amount in different K8S clusters
    if params.get("n_loaders"):
        k8s_cluster.deploy_node_pool(wait_till_ready=False, pool=EksNodePool(
            name=k8s_cluster.LOADER_POOL_NAME,
            num_nodes=params.get("n_loaders"),
            instance_type=params.get("instance_type_loader"),
            role_arn=params.get('eks_nodegroup_role_arn'),
            disk_size=params.get('root_disk_size_monitor'),
            k8s_cluster=k8s_cluster))

    if params.get('k8s_deploy_monitoring'):
        k8s_cluster.deploy_node_pool(wait_till_ready=False, pool=EksNodePool(
            name=k8s_cluster.MONITORING_POOL_NAME,
            num_nodes=params.get("k8s_n_monitor_nodes") or params.get("n_monitor_nodes"),
            instance_type=params.get("k8s_instance_type_monitor") or params.get("instance_type_monitor"),
            role_arn=params.get('eks_nodegroup_role_arn'),
            disk_size=params.get('root_disk_size_monitor'),
            k8s_cluster=k8s_cluster))
    k8s_cluster.wait_all_node_pools_to_be_ready()
    k8s_cluster.configure_ebs_csi_driver()

    k8s_cluster.deploy_cert_manager(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)
    if params.get("k8s_enable_sni"):
        k8s_cluster.deploy_ingress_controller(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)
    k8s_cluster.deploy_scylla_operator()
    if params.get("k8s_use_chaos_mesh"):
        k8s_cluster.chaos_mesh.initialize()
    k8s_cluster.prepare_k8s_scylla_nodes(node_pools=scylla_pool)
    if params.get('use_mgmt'):
        # NOTE: deploy scylla-manager only in the first region. It will then be used by each of the regions
        if params.region_names.index(k8s_cluster.region_name) == 0:
            k8s_cluster.deploy_scylla_manager(pool_name=k8s_cluster.AUXILIARY_POOL_NAME)
    return k8s_cluster


class EksNodePool(CloudK8sNodePool):
    k8s_cluster: 'EksCluster'
    disk_type: Literal["standard", "io1", "io2", "gp2", "gp3", "sc1", "st1"]

    def __init__(  # noqa: PLR0913
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
            image_type: Literal['AL2_x86_64', 'AL2_x86_64_GPU', 'AL2_ARM_64'] = None,
            disk_type: Literal["standard", "io1", "io2", "gp2", "gp3", "sc1", "st1"] = "gp3",
            k8s_version: str = None,
            is_deployed: bool = False,
            user_data: str = None,
    ):
        if not image_type:
            current_arch = get_arch_from_instance_type(
                instance_type=instance_type, region_name=k8s_cluster.region_name)
            image_type = ARCH_TO_IMAGE_TYPE_MAPPING.get(current_arch, "AL2_x86_64")
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
        # NOTE: use launch template to be able to specify the `gp3` disk type by default
        return True

    @property
    def _launch_template_cfg(self) -> dict:
        root_ebs_block_device_params = {
            "DeleteOnTermination": True,
            "Encrypted": False,
            "VolumeType": self.disk_type,
        }
        if self.disk_size:
            root_ebs_block_device_params["VolumeSize"] = self.disk_size
        root_disk_def = LaunchTemplateBlockDeviceMappingRequestTypeDef(
            DeviceName="/dev/xvda",
            Ebs=LaunchTemplateEbsBlockDeviceRequestTypeDef(**root_ebs_block_device_params))
        launch_template = RequestLaunchTemplateDataTypeDef(
            KeyName=self.ssh_key_pair_name,
            EbsOptimized=False,
            BlockDeviceMappings=[root_disk_def],
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
        tags = {'Owner': 'SCT', 'Name': 'SCT', **(self.tags or {})}
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
        self.k8s_cluster.log.info("Deploy %s node pool with %d node(s)", self.name, self.num_nodes)
        if self.is_launch_template_required:
            self.k8s_cluster.log.info("Deploy launch template %s", self.launch_template_name)
            create_launch_template_args = {
                "LaunchTemplateName": self.launch_template_name,
                "LaunchTemplateData": self._launch_template_cfg,
            }
            if self.tags:
                create_launch_template_args["TagSpecifications"] = [LaunchTemplateTagSpecificationRequestTypeDef(
                    ResourceType="launch-template",
                    Tags=tags_as_ec2_tags(self.tags),
                )]
            self.k8s_cluster.ec2_client.create_launch_template(**create_launch_template_args)
        self.k8s_cluster.eks_client.create_nodegroup(**self._node_group_cfg)
        self.is_deployed = True

    def resize(self, num_nodes):
        self.k8s_cluster.log.info("Resize %s pool to %d node(s)", self.name, num_nodes)
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
        except Exception as exc:  # noqa: BLE001
            self.k8s_cluster.log.debug(
                "Failed to delete nodegroup %s/%s, due to the following error:\n%s",
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
    IS_NODE_TUNING_SUPPORTED = True
    NODE_PREPARE_FILE = sct_abs_path("sdcm/k8s_configs/eks/scylla-node-prepare.yaml")
    NODE_CONFIG_CRD_FILE = sct_abs_path("sdcm/k8s_configs/eks/node-config-crd.yaml")
    STORAGE_CLASS_FILE = sct_abs_path("sdcm/k8s_configs/eks/storageclass_gp3.yaml")
    pools: Dict[str, EksNodePool]
    short_cluster_name: str

    def __init__(self,  # noqa: PLR0913
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
            ('app', 'static-local-volume-provisioner'),
            ('k8s-app', 'kube-proxy'),
            ('app.kubernetes.io/name', 'aws-ebs-csi-driver'),
        ]
        if self.tenants_number > 1:
            allowed_labels_on_scylla_node.append(('app', 'scylla'))
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/name', 'scylla'))
        else:
            allowed_labels_on_scylla_node.append(('scylla/cluster', self.k8s_scylla_cluster_name))
        if self.is_performance_tuning_enabled:
            # NOTE: add performance tuning related pods only if we expect it to be.
            #       When we have tuning disabled it must not exist.
            allowed_labels_on_scylla_node.extend(self.perf_pods_labels)
        if self.params.get('k8s_use_chaos_mesh'):
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/component', 'chaos-daemon'))
        if self.params.get("k8s_local_volume_provisioner_type") != 'static':
            allowed_labels_on_scylla_node.append(('app', 'node-pkg-installer'))
            allowed_labels_on_scylla_node.append(('app.kubernetes.io/name', 'local-csi-driver'))
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
            accessConfig={
                'authenticationMode': 'API_AND_CONFIG_MAP'
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
                        'enabled': self.params.get("k8s_log_api_calls"),
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
        # TODO: think if we need to pin version, or select base on k8s version
        self.eks_client.create_addon(
            clusterName=self.short_cluster_name,
            addonName='aws-ebs-csi-driver',
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

    def add_k8s_admin_principal(self):
        eks = boto3.client('eks', region_name=self.region_name)
        admin_principals = self.params.get('eks_admin_arn')

        try:
            for principal in admin_principals:
                eks.create_access_entry(
                    clusterName=self.short_cluster_name,
                    principalArn=principal,
                    type='STANDARD'
                )
                eks.associate_access_policy(
                    clusterName=self.short_cluster_name,
                    principalArn=principal,
                    policyArn="arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy",
                    accessScope={'type': 'cluster'}
                )

        except ClientError as e:
            err_code = e.response["Error"]["Code"]
            err_message = e.response["Error"]["Message"]
            self.log.error(f"Error while adding cluster admin principals: [{err_code}] {err_message}")

    def create_token_update_thread(self):
        return EksTokenUpdateThread(
            aws_cmd=f'aws eks --region {self.region_name} get-token --cluster-name {self.short_cluster_name}',
            kubectl_token_path=self.kubectl_token_path
        )

    def create_kubectl_config(self) -> None:
        LOCALRUNNER.run(
            f"aws eks --region {self.region_name} update-kubeconfig"
            f" --name {self.short_cluster_name} --kubeconfig={self.kube_config_path}")

    def deploy(self):
        self.log.info("Create EKS cluster `%s'", self.short_cluster_name)
        self.create_eks_cluster()
        self.log.info("Patch kubectl config")
        self.patch_kubectl_config()
        self.log.info("Allow SCT roles to connect and manage cluster")
        self.add_k8s_admin_principal()
        self.log.info("Create storage class")
        self.create_ebs_storge_class()

    def create_ebs_storge_class(self, storage_class_backend="gp3"):
        if storage_class_backend in SUPPORTED_EBS_STORAGE_CLASSES:
            tags_specification = "\n".join(
                [f'  tagSpecification_{i}: "{k}={v}"' for i, (k, v) in enumerate(self.tags.items(), start=1)])
            self.apply_file(self.STORAGE_CLASS_FILE, environ=dict(EXTRA_TAG_SPEC=tags_specification))
        else:
            raise NotImplementedError(f"'{storage_class_backend}' storage class backend is not supported")

    @property
    def ebs_csi_driver_info(self) -> dict:
        addon_info = self.eks_client.describe_addon(
            clusterName=self.short_cluster_name,
            addonName='aws-ebs-csi-driver',
        )
        self.log.debug(pprint.pformat(addon_info))
        return addon_info['addon']

    @property
    def ebs_csi_driver_status(self) -> str:
        return self.ebs_csi_driver_info['status']

    def configure_ebs_csi_driver(self):
        tags = ",".join([f"{key}={value}" for key, value in self.tags.items()])

        wait_for(lambda: self.ebs_csi_driver_status == 'ACTIVE', step=60, throw_exc=True, timeout=600,
                 text='Waiting till aws-ebs-csi-driver become operational')
        LOCALRUNNER.run(
            f'eksctl utils associate-iam-oidc-provider --region={self.region_name} --cluster={self.short_cluster_name} --approve')
        LOCALRUNNER.run(f'eksctl create iamserviceaccount --name ebs-csi-controller-sa --namespace kube-system '
                        f'--cluster {self.short_cluster_name} '
                        f'--attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy '
                        f'--approve --role-name EKS_EBS-{self.short_cluster_name} --region {self.region_name} '
                        f'--tags {tags} --override-existing-serviceaccounts')

        self.kubectl("rollout restart deployment ebs-csi-controller", namespace="kube-system")

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
        self.log.info("Tune network of the EKS cluster")
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

    def get_ec2_instance_by_id(self, instance_id):
        return boto3.resource('ec2', region_name=self.region_name).Instance(id=instance_id)

    def create_iamserviceaccount_for_s3_access(self, db_cluster_name: str, namespace: str = SCYLLA_NAMESPACE):
        tags = ",".join([f"{key}={value}" for key, value in self.tags.items()])
        LOCALRUNNER.run(
            f'eksctl create iamserviceaccount --name {db_cluster_name}-member'
            f' --namespace {namespace} --cluster {self.short_cluster_name}'
            f' --attach-policy-arn arn:aws:iam::aws:policy/AWSBackupServiceRolePolicyForS3Restore'
            f' --approve --role-name EKS_S3-{self.short_cluster_name}--{db_cluster_name}'
            f' --region {self.region_name} --tags {tags} --override-existing-serviceaccounts')

    def create_scylla_manager_agent_config(self, s3_provider_endpoint: str = None, namespace: str = SCYLLA_NAMESPACE):
        if s3_provider_endpoint:
            self.log.warning(
                "Ignoring the value ('%s') for the 's3_provider_endpoint' parameter,"
                "which is needed only using S3-compatible services like MinIO.",
                s3_provider_endpoint)
        data = {}
        if self.params.get("use_mgmt"):
            data["s3"] = {
                "provider": "AWS",
                "region": next(iter(self.params.region_names), ''),
            }
        # Create kubernetes secret that holds scylla manager agent configuration
        self.update_secret_from_data(SCYLLA_AGENT_CONFIG_NAME, namespace, {
            "scylla-manager-agent.yaml": data,
        })

    def _get_all_instance_ids(self):
        cmd = "get node --no-headers -o custom-columns=:.spec.providerID"
        return [name.split("/")[-1] for name in self.kubectl(cmd).stdout.split()]

    def set_tags(self, instance_ids, memo={}):  # noqa: B006
        if not instance_ids:
            return
        if isinstance(instance_ids, str):
            instance_ids = [instance_ids]
        with EC2_INSTANCE_UPDATE_LOCK:
            instance_ids = [instance_id for instance_id in instance_ids if instance_id not in memo]
            if not instance_ids:
                return
            self.log.debug("Going to update tags of the following instances: %s", instance_ids)
            boto3.client('ec2', region_name=self.region_name).create_tags(
                Resources=instance_ids,
                Tags=[{"Key": key, "Value": value} for key, value in self.tags.items()],
            )
            for instance_id in instance_ids:
                memo[instance_id] = 'already_updated'
            self.log.debug("Successfully updated tags for the following instances: %s", instance_ids)

    def set_tags_on_all_instances(self):
        # NOTE: EKS doesn't apply nodeGroup's tags to nodes.
        # So, we add it for each node explicitly.
        self.set_tags(self._get_all_instance_ids())

    def set_security_groups(self, instance_id, memo={}):  # noqa: B006
        if not instance_id or instance_id in memo:
            return
        with EC2_INSTANCE_UPDATE_LOCK:
            if instance_id in memo:
                return
            self.log.debug("Going to update security groups of the following instance: %s", instance_id)
            instance = self.get_ec2_instance_by_id(instance_id)
            for network_interface in instance.network_interfaces:
                security_groups = [g["GroupId"] for g in network_interface.groups]
                if self.ec2_security_group_ids[0][0] in security_groups:
                    continue
                security_groups.append(self.ec2_security_group_ids[0][0])
                network_interface.modify_attribute(Groups=security_groups)
            memo[instance_id] = 'already_updated'
            self.log.debug("Successfully updated security groups for the following instance: %s", instance_id)

    def set_security_groups_on_all_instances(self):
        # NOTE: EKS doesn't apply nodeGroup's security groups to nodes
        # So, we add it for each network interface of a node explicitly.
        for instance_id in self._get_all_instance_ids():
            self.set_security_groups(instance_id)

    def deploy_scylla_cluster(self, *args, **kwargs) -> None:
        super().deploy_scylla_cluster(*args, **kwargs)
        self.set_security_groups_on_all_instances()
        self.set_tags_on_all_instances()

    def upgrade_kubernetes_platform(self, pod_objects: list[cluster.BaseNode],
                                    use_additional_scylla_nodepool: bool) -> (str, CloudK8sNodePool):
        upgrade_version = f"1.{int(self.eks_cluster_version.split('.')[1]) + 1}"

        # Upgrade control plane (API, scheduler, manager and so on ...)
        self.log.info("Upgrading K8S control plane to the '%s' version", upgrade_version)
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
        self.eks_cluster_version = upgrade_version

        # Upgrade scylla-related node pools
        for node_pool, need_upgrade in (
                (self.AUXILIARY_POOL_NAME, True),
                (self.SCYLLA_POOL_NAME, not use_additional_scylla_nodepool)):
            if not need_upgrade:
                continue
            self.log.info("Upgrading '%s' node pool to the '%s' version", node_pool, upgrade_version)
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

        if use_additional_scylla_nodepool:
            # Create new node pool
            new_scylla_pool_name = f"{self.SCYLLA_POOL_NAME}-new"
            new_scylla_pool = EksNodePool(
                name=new_scylla_pool_name,
                num_nodes=self.params.get("n_db_nodes"),
                instance_type=self.params.get('instance_type_db'),
                role_arn=self.params.get('eks_nodegroup_role_arn'),
                disk_size=self.params.get('root_disk_size_db'),
                k8s_cluster=self)
            self.deploy_node_pool(new_scylla_pool, wait_till_ready=True)
            self.set_security_groups_on_all_instances()
            self.set_tags_on_all_instances()

            # Prepare new nodes for Scylla pods hosting
            self.prepare_k8s_scylla_nodes(
                node_pools=[self.pools[self.SCYLLA_POOL_NAME], new_scylla_pool])

            # Move Scylla pods to the new nodes
            self.move_pods_to_new_node_pool(
                pod_objects=pod_objects, node_pool_name=new_scylla_pool_name,
                pod_readiness_timeout_minutes=120)

            # Delete old node pool
            self.pools[self.SCYLLA_POOL_NAME].undeploy()

            return upgrade_version, new_scylla_pool
        else:
            return upgrade_version, self.pools[self.SCYLLA_POOL_NAME]


class EksScyllaPodContainer(BaseScyllaPodContainer):
    parent_cluster: 'EksScyllaPodCluster'

    pod_readiness_delay = 30  # seconds
    pod_readiness_timeout = 30  # minutes
    pod_terminate_timeout = 30  # minutes

    @cached_property
    def hydra_dest_ip(self) -> str:
        return self.private_ip_address

    @cached_property
    def nodes_dest_ip(self) -> str:
        return self.private_ip_address

    @property
    def ec2_host(self):
        return self.k8s_cluster.get_ec2_instance_by_id(self.ec2_instance_id)

    @property
    def ec2_instance_id(self):
        return self._node.spec.provider_id.split('/')[-1]

    def refresh_network_interfaces_info(self):
        pass

    def terminate_k8s_host(self):
        self.k8s_cluster.log.info(
            'terminate_k8s_host: EC2 instance of kubernetes node will be terminated, '
            'the following is affected :\n' + dedent('''
            EC2 instance  X  <-
            K8s node      X
            Scylla Pod    X
            Scylla node   X
            '''))
        self._instance_wait_safe(self.ec2_instance_destroy)
        self.wait_for_k8s_node_readiness()
        self.k8s_cluster.set_security_groups_on_all_instances()
        self.k8s_cluster.set_tags_on_all_instances()

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
        self.k8s_cluster.log.info(
            'terminate_k8s_node: kubernetes node will be deleted, the following is affected :\n' + dedent('''
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


class EksScyllaPodCluster(ScyllaPodCluster):
    PodContainerClass = EksScyllaPodContainer
    nodes: List[EksScyllaPodContainer]

    @mark_new_nodes_as_running_nemesis
    def add_nodes(self,
                  count: int,
                  ec2_user_data: str = "",
                  dc_idx: int = None,
                  rack: int = 0,
                  enable_auto_bootstrap: bool = False,
                  instance_type=None
                  ) -> List[EksScyllaPodContainer]:
        new_nodes = super().add_nodes(count=count,
                                      ec2_user_data=ec2_user_data,
                                      dc_idx=dc_idx,
                                      rack=rack,
                                      enable_auto_bootstrap=enable_auto_bootstrap,
                                      instance_type=instance_type)
        for node in new_nodes:
            ec2_instance_id = node.ec2_instance_id
            node.k8s_cluster.set_security_groups(ec2_instance_id)
            node.k8s_cluster.set_tags(ec2_instance_id)
        return new_nodes


class MonitorSetEKS(MonitorSetAWS):
    DB_NODES_IP_ADDRESS = 'ip_address'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json_file_params_for_replace = {
            "$test_name": f"{get_test_name()}--{self.targets['db_cluster'].scylla_cluster_name}",
        }

    def install_scylla_manager(self, node):
        pass

    # NOTE: setting and filtering of the "MonitorId" tag is needed for the multi-tenant setup.
    def _get_instances(self, dc_idx, az_idx=None):
        # az_idx is not used as we specify az's for k8s cluster while we don't create instances in multi-az.
        if not self.monitor_id:
            raise ValueError("'monitor_id' must exist")
        ec2 = ec2_client.EC2ClientWrapper(region_name=self.region_names[dc_idx])
        results = list_instances_aws(
            tags_dict={'MonitorId': self.monitor_id, 'NodeType': self.node_type},
            region_name=self.region_names[dc_idx],
            group_as_region=True,
        )
        instances = results[self.region_names[dc_idx]]

        def sort_by_index(item):
            for tag in item['Tags']:
                if tag['Key'] == 'NodeIndex':
                    return tag['Value']
            return '0'
        instances = sorted(instances, key=sort_by_index)
        return [ec2.get_instance(instance['InstanceId']) for instance in instances]

    def _create_instances(self, count, ec2_user_data='', dc_idx=0, az_idx=0, instance_type=None, is_zero_node=False):
        instances = super()._create_instances(count=count, ec2_user_data=ec2_user_data, dc_idx=dc_idx,
                                              az_idx=az_idx, instance_type=instance_type, is_zero_node=is_zero_node)
        for instance in instances:
            self._ec2_services[dc_idx].create_tags(
                Resources=[instance.id],
                Tags=[{"Key": "MonitorId", "Value": self.monitor_id}],
            )
        return instances
