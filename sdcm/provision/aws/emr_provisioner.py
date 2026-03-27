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
# Copyright (c) 2025 ScyllaDB

"""EMR cluster lifecycle management for spark-migrator testing."""

import logging

import boto3
import botocore

from sdcm.spark_migrator import _ensure_s3_bucket
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)

EMR_SERVICE_ROLE_NAME = "EMR_DefaultRole_V2"
EMR_EC2_INSTANCE_PROFILE_NAME = "EMR_EC2_DefaultRole"

EMR_READY_STATES = ("WAITING",)
EMR_PROVISIONING_STATES = ("STARTING", "BOOTSTRAPPING", "RUNNING")
EMR_TERMINAL_STATES = ("TERMINATED", "TERMINATED_WITH_ERRORS")

SPARK4_VERSION = "4.0.2"
SPARK4_INSTALL_DIR = "/opt/spark4"
BOOTSTRAP_SCRIPT_S3_KEY = "scripts/bootstrap-spark4.sh"
SPARK4_PACKAGE = f"spark-{SPARK4_VERSION}-bin-hadoop3"
SPARK4_URL = f"https://downloads.apache.org/spark/spark-{SPARK4_VERSION}/{SPARK4_PACKAGE}.tgz"
RUNNER_SCRIPT_S3_KEY = "scripts/run-migrator.sh"

BOOTSTRAP_SCRIPT_CONTENT = f"""#!/bin/bash
set -ex
cd /tmp
wget -q "{SPARK4_URL}"
tar xzf "{SPARK4_PACKAGE}.tgz"
sudo mv /tmp/{SPARK4_PACKAGE} {SPARK4_INSTALL_DIR}
sudo chown -R hadoop:hadoop {SPARK4_INSTALL_DIR}
rm -f /tmp/{SPARK4_PACKAGE}.tgz
echo "Bootstrap complete: Spark {SPARK4_VERSION} installed at {SPARK4_INSTALL_DIR}"
"""


class EmrClusterProvisioner:
    """Provisions and manages Amazon EMR clusters for spark-migrator testing.

    Follows the same tagging conventions as other SCT AWS resources for
    cleanup discovery (TestId, RunByUser, NodeType).
    """

    def __init__(self, region_name, params):
        self.region_name = region_name
        self.params = params
        self.emr_client = boto3.client("emr", region_name=region_name)
        self.s3_client = boto3.client("s3", region_name=region_name)
        self.cluster_id = None

    @property
    def s3_bucket(self):
        return f"sct-emr-spark-migrator-{self.region_name}"

    def _ensure_bootstrap_script(self):
        """upload the Spark 4.0 bootstrap script to S3."""
        _ensure_s3_bucket(self.s3_client, self.s3_bucket, self.region_name)
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=BOOTSTRAP_SCRIPT_S3_KEY,
            Body=BOOTSTRAP_SCRIPT_CONTENT.encode("utf-8"),
        )
        return f"s3://{self.s3_bucket}/{BOOTSTRAP_SCRIPT_S3_KEY}"

    def create_emr_cluster(self, test_id, user, vpc_subnet_id=None, test_name="", version=""):
        """Create an EMR cluster with configured instance groups.

        Args:
            test_id: SCT test ID for tagging.
            user: RunByUser for tagging.
            vpc_subnet_id: Optional subnet ID for the EMR cluster.
            test_name: Test name for SCP-required tagging.
            version: Scylla version string for SCP-required tagging.

        Returns:
            str: EMR cluster ID.
        """
        cluster_name = f"sct-emr-{test_id[:8]}"
        bootstrap_s3_path = self._ensure_bootstrap_script()

        # don't pass SCT security groups as EMR-managed SGs — EMR rejects SGs
        # with public ingress on non-22 ports. Let EMR create/use its own SGs.
        instances = {
            "InstanceGroups": self._build_instance_groups(),
            "KeepJobFlowAliveWhenNoSteps": bool(self.params.get("emr_keep_alive")),
            "TerminationProtected": False,
            "Ec2KeyName": "scylla_test_id_ed25519",
        }
        if vpc_subnet_id:
            instances["Ec2SubnetId"] = vpc_subnet_id

        applications = [{"Name": app} for app in (self.params.get("emr_applications") or ["Spark"])]
        cluster_config = {
            "Name": cluster_name,
            "ReleaseLabel": self.params.get("emr_release_label"),
            "Applications": applications,
            "Instances": instances,
            "Tags": [
                {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"},
                {"Key": "TestId", "Value": test_id},
                {"Key": "RunByUser", "Value": user},
                {"Key": "NodeType", "Value": "emr"},
                {"Key": "CreatedBy", "Value": "SCT"},
                {"Key": "TestName", "Value": test_name or "spark-migrator-test"},
                {"Key": "version", "Value": version or "unknown"},
                {"Key": "Name", "Value": cluster_name},
                {"Key": "keep_action", "Value": "terminate"},
            ],
            "VisibleToAllUsers": True,
            "JobFlowRole": EMR_EC2_INSTANCE_PROFILE_NAME,
            "ServiceRole": EMR_SERVICE_ROLE_NAME,
            "BootstrapActions": [
                {
                    "Name": f"Install Spark {SPARK4_VERSION}",
                    "ScriptBootstrapAction": {"Path": bootstrap_s3_path},
                },
            ],
        }

        if log_uri := self.params.get("emr_log_uri"):
            cluster_config["LogUri"] = log_uri

        LOGGER.info(
            "Creating EMR cluster with release %s in %s...", self.params.get("emr_release_label"), self.region_name
        )
        response = self.emr_client.run_job_flow(**cluster_config)
        self.cluster_id = response["JobFlowId"]
        LOGGER.info("EMR cluster created: %s", self.cluster_id)
        return self.cluster_id

    def _build_instance_groups(self):
        """Build EMR instance group configurations."""
        instance_groups = [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": self.params.get("emr_instance_type_master") or "m5.xlarge",
                "InstanceCount": 1,
            },
        ]

        core_count = self.params.get("emr_instance_count_core") or 2
        if core_count > 0:
            instance_groups.append(
                {
                    "Name": "Core",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": self.params.get("emr_instance_type_core") or "m5.xlarge",
                    "InstanceCount": core_count,
                }
            )

        task_count = self.params.get("emr_instance_count_task") or 0
        task_instance_type = self.params.get("emr_instance_type_task")
        if task_count > 0 and task_instance_type:
            bid_percentage = str(self.params.get("emr_spot_bid_percentage") or 100)
            instance_groups.append(
                {
                    "Name": "Task",
                    "Market": "SPOT",
                    "InstanceRole": "TASK",
                    "InstanceType": task_instance_type,
                    "InstanceCount": task_count,
                    "BidPrice": bid_percentage,
                }
            )

        return instance_groups

    @retrying(n=60, sleep_time=30, message="Waiting for EMR cluster to be ready...")
    def wait_for_emr_cluster_ready(self, cluster_id=None):
        """Wait until EMR cluster reaches WAITING state.

        Args:
            cluster_id: EMR cluster ID. Uses self.cluster_id if not provided.

        Returns:
            dict: Cluster description.

        Raises:
            AssertionError: If cluster enters a terminal state.
        """
        cluster_id = cluster_id or self.cluster_id
        status = self.get_emr_cluster_status(cluster_id)
        state = status["State"]

        if state in EMR_READY_STATES:
            LOGGER.info("EMR cluster %s is ready (state: %s)", cluster_id, state)
            return status

        assert state not in EMR_TERMINAL_STATES, (
            f"EMR cluster {cluster_id} entered terminal state: {state}. "
            f"Reason: {status.get('StateChangeReason', {}).get('Message', 'unknown')}"
        )
        assert state in EMR_PROVISIONING_STATES, f"EMR cluster {cluster_id} in unexpected state: {state}"
        raise RuntimeError(f"EMR cluster {cluster_id} still provisioning (state: {state})")

    def get_emr_cluster_status(self, cluster_id=None):
        """Get current EMR cluster status.

        Args:
            cluster_id: EMR cluster ID. Uses self.cluster_id if not provided.

        Returns:
            dict: Cluster status information.
        """
        cluster_id = cluster_id or self.cluster_id
        response = self.emr_client.describe_cluster(ClusterId=cluster_id)
        return response["Cluster"]["Status"]

    def get_emr_cluster_description(self, cluster_id=None):
        """Get full EMR cluster description.

        Args:
            cluster_id: EMR cluster ID. Uses self.cluster_id if not provided.

        Returns:
            dict: Full cluster description.
        """
        cluster_id = cluster_id or self.cluster_id
        response = self.emr_client.describe_cluster(ClusterId=cluster_id)
        return response["Cluster"]

    def get_emr_master_dns(self, cluster_id=None):
        """Get EMR master node public DNS name.

        Args:
            cluster_id: EMR cluster ID. Uses self.cluster_id if not provided.

        Returns:
            str: Master node public DNS, or None if not available.
        """
        cluster_id = cluster_id or self.cluster_id
        cluster = self.get_emr_cluster_description(cluster_id)
        return cluster.get("MasterPublicDnsName")

    def terminate_emr_cluster(self, cluster_id=None):
        """Terminate an EMR cluster.

        Args:
            cluster_id: EMR cluster ID. Uses self.cluster_id if not provided.
        """
        cluster_id = cluster_id or self.cluster_id
        if not cluster_id:
            LOGGER.warning("No EMR cluster ID to terminate")
            return

        LOGGER.info("Terminating EMR cluster %s...", cluster_id)
        self.emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
        LOGGER.info("EMR cluster %s termination initiated", cluster_id)

    def _upload_runner_script(self, jar_s3_path, config_s3_path):
        """Generate and upload a runner shell script that calls standalone Spark 4.0.

        Args:
            jar_s3_path: S3 path to the migrator JAR.
            config_s3_path: S3 path to the migrator config YAML.

        Returns:
            str: S3 URI of the uploaded runner script.
        """
        script_content = f"""#!/bin/bash
set -ex

# download the migrator JAR and config from S3
aws s3 cp {jar_s3_path} /tmp/scylla-migrator-assembly.jar
aws s3 cp {config_s3_path} /tmp/migrator-config.yaml

# point YARN at standalone Spark 4.0 (EMR's bundled Spark ships Scala 2.12,
# but the migrator requires Scala 2.13 which comes with Spark 4.0)
export SPARK_HOME={SPARK4_INSTALL_DIR}
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

# run on YARN in cluster mode so the driver runs on a cluster node
{SPARK4_INSTALL_DIR}/bin/spark-submit \\
  --deploy-mode cluster \\
  --master yarn \\
  --class com.scylladb.migrator.Migrator \\
  --conf "spark.scylla.config=migrator-config.yaml" \\
  --files /tmp/migrator-config.yaml \\
  /tmp/scylla-migrator-assembly.jar
"""
        s3_uri = f"s3://{self.s3_bucket}/{RUNNER_SCRIPT_S3_KEY}"
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=RUNNER_SCRIPT_S3_KEY,
            Body=script_content.encode("utf-8"),
        )
        LOGGER.info("Runner script uploaded to %s", s3_uri)
        return s3_uri

    @property
    def script_runner_jar(self):
        return f"s3://{self.region_name}.elasticmapreduce/libs/script-runner/script-runner.jar"

    def add_step(self, cluster_id, step_name, jar_s3_path, config_s3_path):
        """Add a spark-migrator step to an EMR cluster using script-runner.jar.

        Uses script-runner.jar to execute a shell script that calls the standalone
        Spark 4.0 installation (/opt/spark4/bin/spark-submit) instead of EMR's
        bundled Spark, which ships Scala 2.12 incompatible with the migrator.

        Args:
            cluster_id: EMR cluster ID.
            step_name: Name of the step.
            jar_s3_path: S3 path to the migrator JAR file.
            config_s3_path: S3 path to the migrator config YAML.

        Returns:
            str: Step ID.
        """
        cluster_id = cluster_id or self.cluster_id
        runner_script_uri = self._upload_runner_script(jar_s3_path, config_s3_path)

        response = self.emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": step_name,
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": self.script_runner_jar,
                        "Args": [runner_script_uri],
                    },
                }
            ],
        )
        step_id = response["StepIds"][0]
        LOGGER.info("Added step %s (ID: %s) to EMR cluster %s", step_name, step_id, cluster_id)
        return step_id

    def get_step_status(self, cluster_id, step_id):
        """Get the status of an EMR step.

        Args:
            cluster_id: EMR cluster ID.
            step_id: Step ID.

        Returns:
            dict: Step status information.
        """
        cluster_id = cluster_id or self.cluster_id
        response = self.emr_client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id,
        )
        return response["Step"]["Status"]

    @retrying(n=120, sleep_time=30, message="Waiting for EMR step to complete...")
    def wait_for_step_completion(self, cluster_id, step_id):
        """Wait for an EMR step to complete.

        Args:
            cluster_id: EMR cluster ID.
            step_id: Step ID.

        Returns:
            dict: Step status.

        Raises:
            AssertionError: If step fails or is cancelled.
        """
        cluster_id = cluster_id or self.cluster_id
        status = self.get_step_status(cluster_id, step_id)
        state = status["State"]

        if state == "COMPLETED":
            LOGGER.info("EMR step %s completed successfully", step_id)
            return status

        assert state not in ("FAILED", "CANCELLED", "INTERRUPTED"), (
            f"EMR step {step_id} failed with state: {state}. "
            f"Reason: {status.get('FailureDetails', {}).get('Message', 'unknown')}"
        )
        raise RuntimeError(f"EMR step {step_id} still running (state: {state})")


def list_emr_clusters(tags_dict, region_name):
    """List EMR clusters matching the given tags.

    Args:
        tags_dict: Dictionary of tags to filter by (e.g., TestId, RunByUser).
        region_name: AWS region name.

    Returns:
        list: List of matching EMR cluster summaries with cluster IDs.
    """
    emr_client = boto3.client("emr", region_name=region_name)
    matching_clusters = []

    paginator = emr_client.get_paginator("list_clusters")
    for page in paginator.paginate(ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]):
        for cluster_summary in page.get("Clusters", []):
            cluster_id = cluster_summary["Id"]
            cluster_desc = emr_client.describe_cluster(ClusterId=cluster_id)
            cluster_tags = {tag["Key"]: tag["Value"] for tag in cluster_desc["Cluster"].get("Tags", [])}

            tags_match = all(cluster_tags.get(key) == value for key, value in tags_dict.items())
            if tags_match:
                matching_clusters.append(
                    {
                        "ClusterId": cluster_id,
                        "Name": cluster_summary.get("Name", ""),
                        "State": cluster_summary["Status"]["State"],
                        "Tags": cluster_tags,
                    }
                )

    return matching_clusters


def _get_iam_role(iam_client, role_name):
    """Return the IAM role dict or None if it doesn't exist."""
    try:
        return iam_client.get_role(RoleName=role_name)["Role"]
    except botocore.exceptions.ClientError as ex:
        if "NoSuchEntity" in str(ex):
            return None
        raise


def _get_instance_profile(iam_client, profile_name):
    """Return the IAM instance profile dict or None if it doesn't exist."""
    try:
        return iam_client.get_instance_profile(InstanceProfileName=profile_name)["InstanceProfile"]
    except botocore.exceptions.ClientError as ex:
        if "NoSuchEntity" in str(ex):
            return None
        raise


def ensure_emr_roles(region_name):
    """Create default EMR IAM roles if they don't exist.

    Creates EMR_DefaultRole (service role) and EMR_EC2_DefaultRole (instance profile)
    with the standard AWS-managed policies required for EMR cluster operation.

    Args:
        region_name: AWS region name (IAM is global, but the client needs a region).
    """
    iam_client = boto3.client("iam", region_name=region_name)

    if not _get_iam_role(iam_client, EMR_SERVICE_ROLE_NAME):
        LOGGER.info("Creating EMR service role '%s'...", EMR_SERVICE_ROLE_NAME)
        emr_trust_policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"Service":"elasticmapreduce.amazonaws.com"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        iam_client.create_role(
            RoleName=EMR_SERVICE_ROLE_NAME,
            AssumeRolePolicyDocument=emr_trust_policy,
            Description="Default role for EMR service",
        )
        iam_client.attach_role_policy(
            RoleName=EMR_SERVICE_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2",
        )
        LOGGER.info("EMR service role created.")
    else:
        LOGGER.debug("EMR service role '%s' already exists.", EMR_SERVICE_ROLE_NAME)

    if not _get_instance_profile(iam_client, EMR_EC2_INSTANCE_PROFILE_NAME):
        LOGGER.info("Creating EMR EC2 instance profile '%s'...", EMR_EC2_INSTANCE_PROFILE_NAME)
        ec2_trust_policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"Service":"ec2.amazonaws.com"},'
            '"Action":"sts:AssumeRole"}]}'
        )
        iam_client.create_role(
            RoleName=EMR_EC2_INSTANCE_PROFILE_NAME,
            AssumeRolePolicyDocument=ec2_trust_policy,
            Description="Default role for EMR EC2 instances",
        )
        iam_client.attach_role_policy(
            RoleName=EMR_EC2_INSTANCE_PROFILE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role",
        )
        iam_client.create_instance_profile(InstanceProfileName=EMR_EC2_INSTANCE_PROFILE_NAME)
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=EMR_EC2_INSTANCE_PROFILE_NAME,
            RoleName=EMR_EC2_INSTANCE_PROFILE_NAME,
        )
        LOGGER.info("EMR EC2 instance profile created.")
    else:
        LOGGER.debug("EMR EC2 instance profile '%s' already exists.", EMR_EC2_INSTANCE_PROFILE_NAME)
