#!/usr/bin/env bash
# AWS provision test with a standalone Cassandra cluster.
# Usage: ./run-aws-cassandra-test.sh [cassandra_version]
#
# Provisions a single Cassandra node on AWS via apt install,
# then runs cassandra-stress against it.
#
# Prerequisites:
#   - AWS credentials configured (OKTA or aws configure)
#   - SCT_AMI_ID_DB_CASSANDRA set to a Ubuntu 22.04 AMI in the target region
#     (e.g., SCT_AMI_ID_DB_CASSANDRA=ami-0c7217cdde317cfec for us-east-1)
#   - SCT_REGION_NAME set (e.g., us-east-1)
#   - For local dev: export SCT_IP_SSH_CONNECTIONS=public
#
# Examples:
#   ./run-aws-cassandra-test.sh          # default: Cassandra 4.1
#   ./run-aws-cassandra-test.sh 5.0      # Cassandra 5.0

set -euo pipefail

CASSANDRA_VERSION="${1:-4.1}"

export SCT_CASSANDRA_VERSION="${CASSANDRA_VERSION}"
export SCT_USE_MGMT=false
export SCT_IP_SSH_CONNECTIONS=public
export SCT_INSTANCE_PROVISION=on_demand
export SCT_AMI_ID_DB_CASSANDRA='resolve:ssm:/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id'

echo "=== Running Cassandra ${CASSANDRA_VERSION} AWS provision test ==="
echo "    AMI: ${SCT_AMI_ID_DB_CASSANDRA}"
echo "    Region: ${SCT_REGION_NAME:-us-east-1}"

uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend aws \
    --config test-cases/cassandra-aws-provision-test.yaml \
    --config configurations/network_config/test_communication_public.yaml
