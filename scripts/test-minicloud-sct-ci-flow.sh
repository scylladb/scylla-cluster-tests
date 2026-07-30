#!/bin/bash
# Reproducer #2: Full SCT CI flow simulation against minicloud
# Starts minicloud, then runs: provision infra → launch instances → discover by tags → cleanup
#
# Run: bash scripts/test-minicloud-sct-ci-flow.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${SCRIPT_DIR}/start-minicloud.sh"

export AWS_ENDPOINT_URL="http://localhost:${MINICLOUD_PORT:-5000}"
export AWS_DEFAULT_REGION=us-east-1

TEST_ID="ci-flow-$(date +%s)"
RUN_BY_USER="repro-test"
PASS=0
FAIL=0

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        PASS=$((PASS + 1))
        echo "    ✅ $desc"
    else
        FAIL=$((FAIL + 1))
        echo "    ❌ $desc (expected='$expected', got='$actual')"
    fi
}

check_not_empty() {
    local desc="$1" actual="$2"
    if [ -n "$actual" ] && [ "$actual" != "null" ] && [ "$actual" != "None" ]; then
        PASS=$((PASS + 1))
        echo "    ✅ $desc ($actual)"
    else
        FAIL=$((FAIL + 1))
        echo "    ❌ $desc (got empty/null)"
    fi
}

echo "=== SCT CI Flow Simulation ==="
echo "Endpoint: $AWS_ENDPOINT_URL"
echo "TestId: $TEST_ID"
echo ""

echo "[Phase 1] Provision Infrastructure"
echo ""

echo "  Creating VPC..."
VPC_ID=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=sct-vpc-$TEST_ID},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER}]" \
    --query 'Vpc.VpcId' --output text)
echo "    VPC_ID=$VPC_ID"

echo "  Creating Subnet..."
SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 \
    --availability-zone us-east-1a \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=sct-subnet-us-east-1a-$TEST_ID},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER}]" \
    --query 'Subnet.SubnetId' --output text)
echo "    SUBNET_ID=$SUBNET_ID"

echo "  Creating Security Group..."
SG_ID=$(aws ec2 create-security-group --group-name "sct-sg-$TEST_ID" --description "SCT SG" \
    --vpc-id "$VPC_ID" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=sct-security-group-$TEST_ID},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER}]" \
    --query 'GroupId' --output text)
echo "    SG_ID=$SG_ID"

echo "  Creating Internet Gateway..."
IGW_ID=$(aws ec2 create-internet-gateway \
    --tag-specifications "ResourceType=internet-gateway,Tags=[{Key=Name,Value=sct-igw-$TEST_ID},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER}]" \
    --query 'InternetGateway.InternetGatewayId' --output text)
echo "    IGW_ID=$IGW_ID"

aws ec2 attach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" 2>/dev/null || true

echo "  Creating Route Table..."
RTB_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" \
    --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=sct-rtb-$TEST_ID},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER}]" \
    --query 'RouteTable.RouteTableId' --output text)
echo "    RTB_ID=$RTB_ID"

echo ""
echo "[Phase 2] Launch Instances (3 DB nodes)"
echo ""

INSTANCE_IDS=""
for i in 1 2 3; do
    INST_ID=$(aws ec2 run-instances \
        --image-id ami-0ec6833605400f8d5 \
        --instance-type i4i.large \
        --network-interfaces "DeviceIndex=0,SubnetId=$SUBNET_ID,Groups=$SG_ID,AssociatePublicIpAddress=true" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=sct-db-node-$TEST_ID-$i},{Key=TestId,Value=$TEST_ID},{Key=RunByUser,Value=$RUN_BY_USER},{Key=NodeType,Value=scylla-db}]" \
        --query 'Instances[0].InstanceId' --output text)
    echo "    Instance $i: $INST_ID"
    INSTANCE_IDS="$INSTANCE_IDS $INST_ID"
done

echo ""
echo "  Adding post-launch tags via CreateTags..."
# shellcheck disable=SC2086
aws ec2 create-tags --resources $INSTANCE_IDS \
    --tags "Key=keep,Value=0" "Key=keep_action,Value=terminate"
echo "    Done"
echo ""

echo "[Phase 3] Discover Resources by Tags"
echo ""

echo "  3a. Discover VPC by tag:Name..."
FOUND_VPC=$(aws ec2 describe-vpcs \
    --filters "Name=tag:Name,Values=sct-vpc-$TEST_ID" \
    --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo "None")
check "VPC found by Name tag" "$VPC_ID" "$FOUND_VPC"

echo "  3b. Discover Subnet by tag:Name..."
FOUND_SUBNET=$(aws ec2 describe-subnets \
    --filters "Name=tag:Name,Values=sct-subnet-us-east-1a-$TEST_ID" \
    --query 'Subnets[0].SubnetId' --output text 2>/dev/null || echo "None")
check "Subnet found by Name tag" "$SUBNET_ID" "$FOUND_SUBNET"

echo "  3c. Discover Security Group by tag:Name..."
FOUND_SG=$(aws ec2 describe-security-groups \
    --filters "Name=tag:Name,Values=sct-security-group-$TEST_ID" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
check "SG found by Name tag" "$SG_ID" "$FOUND_SG"

echo "  3d. Discover Internet Gateway by tag:Name..."
FOUND_IGW=$(aws ec2 describe-internet-gateways \
    --filters "Name=tag:Name,Values=sct-igw-$TEST_ID" \
    --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null || echo "None")
check "IGW found by Name tag" "$IGW_ID" "$FOUND_IGW"

echo "  3e. Discover Route Table by tag:Name..."
FOUND_RTB=$(aws ec2 describe-route-tables \
    --filters "Name=tag:Name,Values=sct-rtb-$TEST_ID" \
    --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || echo "None")
check "RTB found by Name tag" "$RTB_ID" "$FOUND_RTB"

echo ""
echo "  3f. Discover Instances by tag:TestId + state=running..."
FOUND_INSTANCES=$(aws ec2 describe-instances \
    --filters "Name=tag:TestId,Values=$TEST_ID" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "None")
FOUND_COUNT=$(echo "$FOUND_INSTANCES" | wc -w)
check "3 instances found by TestId+running" "3" "$FOUND_COUNT"

echo "  3g. Discover Instances by tag:NodeType=scylla-db..."
FOUND_DB=$(aws ec2 describe-instances \
    --filters "Name=tag:NodeType,Values=scylla-db" "Name=tag:TestId,Values=$TEST_ID" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "None")
FOUND_DB_COUNT=$(echo "$FOUND_DB" | wc -w)
check "3 DB instances found by NodeType tag" "3" "$FOUND_DB_COUNT"

echo "  3h. Verify instance state is 'running'..."
FIRST_INST=$(echo "$INSTANCE_IDS" | awk '{print $1}')
STATE=$(aws ec2 describe-instances --instance-ids "$FIRST_INST" \
    --query 'Reservations[0].Instances[0].State.Name' --output text 2>/dev/null || echo "None")
check "instance state is running" "running" "$STATE"

echo "  3i. Verify instance private IP assigned..."
PRIV_IP=$(aws ec2 describe-instances --instance-ids "$FIRST_INST" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null || echo "None")
check_not_empty "private IP assigned" "$PRIV_IP"

echo "  3j. Verify instance public IP assigned..."
PUB_IP=$(aws ec2 describe-instances --instance-ids "$FIRST_INST" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null || echo "None")
check_not_empty "public IP assigned" "$PUB_IP"

echo "  3k. Verify availability zone matches placement..."
AZ=$(aws ec2 describe-instances --instance-ids "$FIRST_INST" \
    --query 'Reservations[0].Instances[0].Placement.AvailabilityZone' --output text 2>/dev/null || echo "None")
check "AZ is us-east-1a" "us-east-1a" "$AZ"

echo ""
echo "[Phase 4] Cleanup by Tags"
echo ""

echo "  4a. Terminate instances found by TestId tag..."
CLEANUP_INSTANCES=$(aws ec2 describe-instances \
    --filters "Name=tag:TestId,Values=$TEST_ID" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "")
if [ -n "$CLEANUP_INSTANCES" ]; then
    # shellcheck disable=SC2086
    aws ec2 terminate-instances --instance-ids $CLEANUP_INSTANCES >/dev/null 2>&1 || true
    TERM_COUNT=$(echo "$CLEANUP_INSTANCES" | wc -w)
    check "terminated instances" "3" "$TERM_COUNT"
else
    FAIL=$((FAIL + 1))
    echo "    ❌ no instances found for cleanup"
fi

echo "  4b. Verify instances terminated..."
sleep 1
REMAINING=$(aws ec2 describe-instances \
    --filters "Name=tag:TestId,Values=$TEST_ID" "Name=instance-state-name,Values=running" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null || echo "")
REMAINING_COUNT=$(echo "$REMAINING" | tr -s ' ' | sed 's/^ *//;s/ *$//' | wc -w)
if [ "$REMAINING_COUNT" -eq 0 ] || [ -z "$(echo "$REMAINING" | tr -d ' ')" ]; then
    PASS=$((PASS + 1))
    echo "    ✅ no running instances remain"
else
    FAIL=$((FAIL + 1))
    echo "    ❌ $REMAINING_COUNT instances still running: $REMAINING"
fi

echo "  4c. Delete Security Group..."
aws ec2 delete-security-group --group-id "$SG_ID" 2>/dev/null && \
    { PASS=$((PASS + 1)); echo "    ✅ SG deleted"; } || \
    { FAIL=$((FAIL + 1)); echo "    ❌ SG delete failed"; }

echo "  4d. Detach + Delete Internet Gateway..."
aws ec2 detach-internet-gateway --internet-gateway-id "$IGW_ID" --vpc-id "$VPC_ID" 2>/dev/null || true
aws ec2 delete-internet-gateway --internet-gateway-id "$IGW_ID" 2>/dev/null && \
    { PASS=$((PASS + 1)); echo "    ✅ IGW deleted"; } || \
    { FAIL=$((FAIL + 1)); echo "    ❌ IGW delete failed"; }

echo "  4e. Delete Route Table..."
aws ec2 delete-route-table --route-table-id "$RTB_ID" 2>/dev/null && \
    { PASS=$((PASS + 1)); echo "    ✅ RTB deleted"; } || \
    { FAIL=$((FAIL + 1)); echo "    ❌ RTB delete failed"; }

echo "  4f. Delete Subnet..."
aws ec2 delete-subnet --subnet-id "$SUBNET_ID" 2>/dev/null && \
    { PASS=$((PASS + 1)); echo "    ✅ Subnet deleted"; } || \
    { FAIL=$((FAIL + 1)); echo "    ❌ Subnet delete failed"; }

echo "  4g. Delete VPC..."
aws ec2 delete-vpc --vpc-id "$VPC_ID" 2>/dev/null && \
    { PASS=$((PASS + 1)); echo "    ✅ VPC deleted"; } || \
    { FAIL=$((FAIL + 1)); echo "    ❌ VPC delete failed"; }

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ $FAIL -gt 0 ]; then
    exit 1
fi
