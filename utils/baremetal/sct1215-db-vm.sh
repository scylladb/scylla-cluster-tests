#!/usr/bin/env bash
# SCT-1215: the AWS DB node for the baremetal run with monster as loader + monitor.
#
#   utils/baremetal/sct1215-db-vm.sh create    # SG + i7i.2xlarge + Elastic IP, then writes the hosts config JSON
#   utils/baremetal/sct1215-db-vm.sh upload    # hosts config -> S3 keystore bucket + Secrets Manager (sct/ prefix)
#   utils/baremetal/sct1215-db-vm.sh status
#   utils/baremetal/sct1215-db-vm.sh destroy   # instance, EIP, SG, secret
#
# Required env:
#   MONSTER_LAN_IP     monster's address as SCT on monster reaches it (goes into the hosts config)
# Optional env:
#   MONSTER_ACCESS     monster's name in the lab access service; SCT then reaches it through that service
#   MONSTER_EGRESS_IP  the address AWS sees monster's traffic from; default: asks checkip from
#                      this machine, so run `create` ON MONSTER or pass it explicitly
#   REGION (eu-west-1) AZ (eu-west-1a) INSTANCE_TYPE (i7i.2xlarge) KEEP_HOURS (48)
#   OWNER (fruch)
set -euo pipefail

REGION=${REGION:-eu-west-1}
AZ=${AZ:-${REGION}a}
INSTANCE_TYPE=${INSTANCE_TYPE:-i7i.2xlarge}
KEEP_HOURS=${KEEP_HOURS:-48}
OWNER=${OWNER:-fruch}
NAME=sct1215-baremetal-db
SG_NAME=sct1215-monster
KEY_PAIR=scylla_test_id_ed25519
CONFIG_NAME=baremetal_sct1215_monster
KEYSTORE_BUCKET=scylla-qa-keystore
SM_REGION=us-east-1
STATE_FILE=${STATE_FILE:-./${NAME}.state}

ec2() { aws ec2 --region "$REGION" "$@"; }
tags() {  # tags <resource-type>
    echo "ResourceType=$1,Tags=[{Key=Name,Value=$NAME},{Key=RunByUser,Value=$OWNER},{Key=keep,Value=$KEEP_HOURS},{Key=keep_action,Value=terminate},{Key=NodeType,Value=scylla-db},{Key=JiraTicket,Value=SCT-1215}]"
}
load_state() { [[ -f $STATE_FILE ]] && source "$STATE_FILE"; true; }

create() {
    : "${MONSTER_LAN_IP:?set MONSTER_LAN_IP}"
    local egress=${MONSTER_EGRESS_IP:-$(curl -fsS https://checkip.amazonaws.com)}
    echo "monster egress IP: $egress (must be monster's, not your laptop's)"

    local vpc subnet ami sg
    vpc=$(ec2 describe-vpcs --filters Name=tag:Name,Values=SCT-2-vpc --query 'Vpcs[0].VpcId' --output text)
    subnet=$(ec2 describe-subnets --filters Name=vpc-id,Values="$vpc" Name=availability-zone,Values="$AZ" \
        "Name=tag:Name,Values=SCT-2-subnet-*" --query 'Subnets[0].SubnetId' --output text)
    ami=$(aws ssm get-parameter --region "$REGION" --query Parameter.Value --output text \
        --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id)
    echo "vpc=$vpc subnet=$subnet ami=$ami"
    [[ $vpc != None && $subnet != None ]] || { echo "SCT VPC/subnet not found in $REGION/$AZ" >&2; exit 1; }

    sg=$(ec2 describe-security-groups --filters Name=vpc-id,Values="$vpc" Name=group-name,Values="$SG_NAME" \
        --query 'SecurityGroups[0].GroupId' --output text)
    if [[ $sg == None ]]; then
        sg=$(ec2 create-security-group --vpc-id "$vpc" --group-name "$SG_NAME" \
            --description "SCT-1215 monster to baremetal DB" --tag-specifications "$(tags security-group)" \
            --query GroupId --output text)
    fi
    # All TCP from monster only: SSH, CQL 9042/19042, the Prometheus scrapes 9180/9100, REST 10000
    ec2 authorize-security-group-ingress --group-id "$sg" \
        --ip-permissions "IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=${egress}/32,Description=monster}]" \
        2>&1 | grep -v InvalidPermission.Duplicate || true

    local iid
    iid=$(ec2 run-instances --instance-type "$INSTANCE_TYPE" --image-id "$ami" --key-name "$KEY_PAIR" \
        --subnet-id "$subnet" --security-group-ids "$sg" \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=50,VolumeType=gp3,DeleteOnTermination=true}' \
        --metadata-options HttpTokens=required \
        --tag-specifications "$(tags instance)" "$(tags volume)" \
        --query 'Instances[0].InstanceId' --output text)
    echo "instance $iid, waiting for running..."
    ec2 wait instance-running --instance-ids "$iid"

    local alloc eip private
    alloc=$(ec2 allocate-address --domain vpc --tag-specifications "$(tags elastic-ip)" --query AllocationId --output text)
    ec2 associate-address --instance-id "$iid" --allocation-id "$alloc" >/dev/null
    eip=$(ec2 describe-addresses --allocation-ids "$alloc" --query 'Addresses[0].PublicIp' --output text)
    # SCT runs cqlsh on the DB node against its own public address (schema dump at test end)
    ec2 authorize-security-group-ingress --group-id "$sg" \
        --ip-permissions "IpProtocol=tcp,FromPort=0,ToPort=65535,IpRanges=[{CidrIp=${eip}/32,Description=db-node-own-public-ip}]" \
        2>&1 | grep -v InvalidPermission.Duplicate || true
    private=$(ec2 describe-instances --instance-ids "$iid" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)

    cat > "$STATE_FILE" <<EOF
INSTANCE_ID=$iid
ALLOCATION_ID=$alloc
SG_ID=$sg
DB_PUBLIC_IP=$eip
DB_PRIVATE_IP=$private
EOF
    hosts_json "$eip" "$private" > "${CONFIG_NAME}.json"
    echo "DB node: $eip (private $private). Hosts config: ./${CONFIG_NAME}.json; state: $STATE_FILE"
    echo "check from monster: ssh -i ~/.ssh/scylla_test_id_ed25519 ubuntu@$eip true"
}

hosts_json() {  # hosts_json <db-public> <db-private>
    # monster sits next to SCT (its Jenkins agent), so it needs no reverse tunnels: `private`.
    # With MONSTER_ACCESS set, SCT reaches it through the lab access service instead of the hand-added key.
    local monster="{\"public_ip\": \"$MONSTER_LAN_IP\", \"private_ip\": \"$MONSTER_LAN_IP\"${MONSTER_ACCESS:+, \"access\": \"$MONSTER_ACCESS\"}}"
    cat <<EOF
{
  "db_nodes":      {"username": "ubuntu", "ip_ssh_connections": "public",
                    "node_list": [{"public_ip": "$1", "private_ip": "$2"}]},
  "loader_nodes":  {"username": "jenkins", "ip_ssh_connections": "private", "node_list": [$monster]},
  "monitor_nodes": {"username": "jenkins", "ip_ssh_connections": "private", "node_list": [$monster]}
}
EOF
}

upload() {
    local f=${CONFIG_NAME}.json
    python3 -m json.tool "$f" >/dev/null
    aws s3 cp "$f" "s3://${KEYSTORE_BUCKET}/${f}"
    # KeyStore reads Secrets Manager by default; the S3 copy alone is not found by a normal run
    if aws secretsmanager describe-secret --region "$SM_REGION" --secret-id "sct/$f" >/dev/null 2>&1; then
        aws secretsmanager put-secret-value --region "$SM_REGION" --secret-id "sct/$f" --secret-string "file://$f"
    else
        aws secretsmanager create-secret --region "$SM_REGION" --name "sct/$f" --secret-string "file://$f" \
            --tags Key=RunByUser,Value="$OWNER" Key=JiraTicket,Value=SCT-1215
    fi
}

status() {
    load_state
    ec2 describe-instances --filters "Name=tag:Name,Values=$NAME" \
        --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress,PrivateIpAddress,LaunchTime]' --output table
}

destroy() {
    load_state
    : "${INSTANCE_ID:?no state file at $STATE_FILE}"
    ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null
    ec2 wait instance-terminated --instance-ids "$INSTANCE_ID"
    ec2 release-address --allocation-id "$ALLOCATION_ID"
    ec2 delete-security-group --group-id "$SG_ID"
    aws s3 rm "s3://${KEYSTORE_BUCKET}/${CONFIG_NAME}.json" || true
    aws secretsmanager delete-secret --region "$SM_REGION" --secret-id "sct/${CONFIG_NAME}.json" \
        --force-delete-without-recovery || true
    rm -f "$STATE_FILE"
}

case ${1:-} in
    create|upload|status|destroy) "$1" ;;
    *) sed -n '2,16p' "$0"; exit 2 ;;
esac
