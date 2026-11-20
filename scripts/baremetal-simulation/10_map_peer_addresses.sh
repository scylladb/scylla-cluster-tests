#!/bin/bash
# Step 3b -- make every node's "public" address routable from every other node.
#
#   scripts/baremetal-simulation/10_map_peer_addresses.sh
#
# On a physical host the address SCT is handed IS the host's own NIC address, so
# every node can reach every other node by the address in the JSON.  On EC2 the
# public IP is NAT'd outside the instance: a peer inside the VPC cannot reach it,
# and the instance cannot reach its own.  With ip_ssh_connections=public -- which
# the perf test case uses -- SCT hands those unreachable addresses to cqlsh and to
# cassandra-stress, which then fails with:
#
#   NoHostAvailableException: All host(s) tried for query failed
#     (/<db public ip>:9042 Cannot connect)
#
# This installs, on every host, a DNAT mapping each node's public address to its
# private one -- restoring the property a physical network gives for free.
set -euo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/_lib.sh"
sim_load_config

config_json="$(sim_require_config_json)"
mapfile -t pairs < <(python3 -c "
import json
config = json.load(open('${config_json}'))
for section in ('db_nodes', 'loader_nodes', 'monitor_nodes'):
    for node in config[section]['node_list']:
        print(node['public_ip'], node['private_ip'])
")
[[ ${#pairs[@]} -gt 0 ]] || sim_die "no nodes in ${config_json}"

rules=""
for pair in "${pairs[@]}"; do
    read -r public private <<<"${pair}"
    rules+="    ip daddr ${public} dnat to ${private}"$'\n'
done

read -r -d '' TABLE <<EOF2 || true
table ip sctsim {
  chain output {
    type nat hook output priority -100; policy accept;
${rules}  }
}
EOF2

ssh_opts=(-i "${SIM_SSH_KEY/#\~/${HOME}}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15)

for pair in "${pairs[@]}"; do
    read -r public _ <<<"${pair}"
    sim_log "mapping ${#pairs[@]} peer address(es) on ${public}"
    ssh "${ssh_opts[@]}" "${SIM_SSH_USER}@${public}" \
        "sudo nft delete table ip sctsim 2>/dev/null; printf '%s\n' '${TABLE}' | sudo nft -f -" </dev/null
done

sim_log "peer addresses mapped -- every node can now reach every other by its public address"
