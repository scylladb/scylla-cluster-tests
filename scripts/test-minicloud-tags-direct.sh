#!/bin/bash
# Reproducer #1: Direct tag API validation against minicloud
# Starts minicloud, then tests each Describe* handler for tag filter support.
#
# Run: bash scripts/test-minicloud-tags-direct.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${SCRIPT_DIR}/start-minicloud.sh"

export AWS_ENDPOINT_URL="http://localhost:${MINICLOUD_PORT:-5000}"
export AWS_DEFAULT_REGION=us-east-1

PASS=0
FAIL=0
ERRORS=""

check() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$actual" = "$expected" ]; then
        PASS=$((PASS + 1))
        echo "  ✅ $desc"
    else
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  ❌ $desc (expected='$expected', got='$actual')"
        echo "  ❌ $desc (expected='$expected', got='$actual')"
    fi
}

check_not_empty() {
    local desc="$1" actual="$2"
    if [ -n "$actual" ] && [ "$actual" != "null" ] && [ "$actual" != "None" ] && [ "$actual" != "[]" ]; then
        PASS=$((PASS + 1))
        echo "  ✅ $desc"
    else
        FAIL=$((FAIL + 1))
        ERRORS="${ERRORS}\n  ❌ $desc (got empty/null: '$actual')"
        echo "  ❌ $desc (got empty/null: '$actual')"
    fi
}

echo "=== Minicloud Tag Direct API Test ==="
echo "Endpoint: $AWS_ENDPOINT_URL"
echo ""

echo "[Setup] Creating test infrastructure..."

VPC_ID=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 \
    --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=sct-vpc},{Key=TestId,Value=tag-test-001},{Key=RunByUser,Value=repro}]' \
    --query 'Vpc.VpcId' --output text 2>&1)
echo "  VPC: $VPC_ID"

SUBNET_ID=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 \
    --availability-zone us-east-1a \
    --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=sct-subnet-us-east-1a},{Key=TestId,Value=tag-test-001}]' \
    --query 'Subnet.SubnetId' --output text 2>&1)
echo "  Subnet: $SUBNET_ID"

SG_ID=$(aws ec2 create-security-group --group-name sct-sg --description "SCT Security Group" \
    --vpc-id "$VPC_ID" \
    --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=sct-sg},{Key=TestId,Value=tag-test-001}]' \
    --query 'GroupId' --output text 2>&1)
echo "  SG: $SG_ID"

IGW_ID=$(aws ec2 create-internet-gateway \
    --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=sct-igw},{Key=TestId,Value=tag-test-001}]' \
    --query 'InternetGateway.InternetGatewayId' --output text 2>&1)
echo "  IGW: $IGW_ID"

RTB_ID=$(aws ec2 create-route-table --vpc-id "$VPC_ID" \
    --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=sct-rtb},{Key=TestId,Value=tag-test-001}]' \
    --query 'RouteTable.RouteTableId' --output text 2>&1)
echo "  RTB: $RTB_ID"

echo ""
echo "[Setup] Adding post-creation tags via CreateTags..."
aws ec2 create-tags --resources "$VPC_ID" "$SUBNET_ID" "$SG_ID" "$IGW_ID" "$RTB_ID" \
    --tags Key=CreatedVia,Value=CreateTags Key=NodeType,Value=scylla-db 2>&1
echo "  Done"
echo ""

echo "[Test Group 1] Tags present in Describe* responses"
echo ""

echo "  DescribeVpcs:"
VPC_TAGS=$(aws ec2 describe-vpcs --vpc-ids "$VPC_ID" --query 'Vpcs[0].Tags' --output json 2>/dev/null || echo "null")
check_not_empty "tags returned" "$VPC_TAGS"
EXTRA=$(echo "$VPC_TAGS" | python3 -c "import sys,json; tags={t['Key']:t['Value'] for t in json.load(sys.stdin) or []}; print(tags.get('CreatedVia',''))" 2>/dev/null || echo "")
check "CreateTags tag visible" "CreateTags" "$EXTRA"

echo "  DescribeSubnets:"
SUBNET_TAGS=$(aws ec2 describe-subnets --subnet-ids "$SUBNET_ID" --query 'Subnets[0].Tags' --output json 2>/dev/null || echo "null")
check_not_empty "tags returned" "$SUBNET_TAGS"
EXTRA=$(echo "$SUBNET_TAGS" | python3 -c "import sys,json; tags={t['Key']:t['Value'] for t in json.load(sys.stdin) or []}; print(tags.get('CreatedVia',''))" 2>/dev/null || echo "")
check "CreateTags tag visible" "CreateTags" "$EXTRA"

echo "  DescribeSecurityGroups:"
SG_TAGS=$(aws ec2 describe-security-groups --group-ids "$SG_ID" --query 'SecurityGroups[0].Tags' --output json 2>/dev/null || echo "null")
check_not_empty "tags returned" "$SG_TAGS"
EXTRA=$(echo "$SG_TAGS" | python3 -c "import sys,json; tags={t['Key']:t['Value'] for t in json.load(sys.stdin) or []}; print(tags.get('CreatedVia',''))" 2>/dev/null || echo "")
check "CreateTags tag visible" "CreateTags" "$EXTRA"

echo "  DescribeInternetGateways:"
IGW_RESP=$(aws ec2 describe-internet-gateways --output json 2>/dev/null || echo "{}")
IGW_TAGS=$(echo "$IGW_RESP" | python3 -c "
import sys,json
data=json.load(sys.stdin)
for igw in data.get('InternetGateways',[]):
    if igw['InternetGatewayId']=='$IGW_ID':
        print(json.dumps(igw.get('Tags',[])))
        break
else:
    print('null')
" 2>/dev/null || echo "null")
check_not_empty "tags returned" "$IGW_TAGS"
EXTRA=$(echo "$IGW_TAGS" | python3 -c "import sys,json; tags={t['Key']:t['Value'] for t in json.load(sys.stdin) or []}; print(tags.get('CreatedVia',''))" 2>/dev/null || echo "")
check "CreateTags tag visible" "CreateTags" "$EXTRA"

echo "  DescribeRouteTables:"
RTB_TAGS=$(aws ec2 describe-route-tables --route-table-ids "$RTB_ID" --query 'RouteTables[0].Tags' --output json 2>/dev/null || echo "null")
check_not_empty "tags returned" "$RTB_TAGS"
EXTRA=$(echo "$RTB_TAGS" | python3 -c "import sys,json; tags={t['Key']:t['Value'] for t in json.load(sys.stdin) or []}; print(tags.get('CreatedVia',''))" 2>/dev/null || echo "")
check "CreateTags tag visible" "CreateTags" "$EXTRA"

echo ""
echo "[Test Group 2] Tag filter support (tag:Name=value)"
echo ""

echo "  DescribeVpcs filter tag:Name=sct-vpc:"
RESULT=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=sct-vpc" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo "None")
check "found by tag" "$VPC_ID" "$RESULT"

echo "  DescribeVpcs filter tag:TestId=tag-test-001:"
RESULT=$(aws ec2 describe-vpcs --filters "Name=tag:TestId,Values=tag-test-001" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo "None")
check "found by TestId tag" "$VPC_ID" "$RESULT"

echo "  DescribeSubnets filter tag:Name=sct-subnet-us-east-1a:"
RESULT=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=sct-subnet-us-east-1a" --query 'Subnets[0].SubnetId' --output text 2>/dev/null || echo "None")
check "found by tag" "$SUBNET_ID" "$RESULT"

echo "  DescribeSecurityGroups filter tag:Name=sct-sg:"
RESULT=$(aws ec2 describe-security-groups --filters "Name=tag:Name,Values=sct-sg" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
check "found by tag" "$SG_ID" "$RESULT"

echo "  DescribeSecurityGroups filter tag:TestId=tag-test-001:"
RESULT=$(aws ec2 describe-security-groups --filters "Name=tag:TestId,Values=tag-test-001" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")
check "found by TestId tag" "$SG_ID" "$RESULT"

echo "  DescribeInternetGateways filter tag:Name=sct-igw:"
RESULT=$(aws ec2 describe-internet-gateways --filters "Name=tag:Name,Values=sct-igw" --query 'InternetGateways[0].InternetGatewayId' --output text 2>/dev/null || echo "None")
check "found by tag" "$IGW_ID" "$RESULT"

echo "  DescribeRouteTables filter tag:Name=sct-rtb:"
RESULT=$(aws ec2 describe-route-tables --filters "Name=tag:Name,Values=sct-rtb" --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || echo "None")
check "found by tag" "$RTB_ID" "$RESULT"

echo ""
echo "[Test Group 3] Multi-tag AND filter"
echo ""

echo "  DescribeVpcs filter tag:Name=sct-vpc AND tag:TestId=tag-test-001:"
RESULT=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=sct-vpc" "Name=tag:TestId,Values=tag-test-001" --query 'Vpcs[0].VpcId' --output text 2>/dev/null || echo "None")
check "found by two tags" "$VPC_ID" "$RESULT"

echo "  DescribeVpcs filter tag:Name=sct-vpc AND tag:TestId=WRONG:"
RESULT=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=sct-vpc" "Name=tag:TestId,Values=WRONG" --query 'Vpcs' --output text 2>/dev/null || echo "None")
if [ "$RESULT" = "None" ] || [ -z "$RESULT" ]; then
    PASS=$((PASS + 1))
    echo "  ✅ correctly returns empty for non-matching AND"
else
    FAIL=$((FAIL + 1))
    echo "  ❌ should be empty but got: $RESULT"
fi

echo ""
echo "[Test Group 4] Non-existent tag filter returns empty"
echo ""

echo "  DescribeSubnets filter tag:Name=NONEXISTENT:"
RESULT=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=NONEXISTENT" --query 'Subnets' --output text 2>/dev/null || echo "None")
if [ "$RESULT" = "None" ] || [ -z "$RESULT" ]; then
    PASS=$((PASS + 1))
    echo "  ✅ correctly returns empty"
else
    FAIL=$((FAIL + 1))
    echo "  ❌ should be empty but got: $RESULT"
fi

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ $FAIL -gt 0 ]; then
    echo ""
    echo "Failures:"
    echo -e "$ERRORS"
    exit 1
fi
