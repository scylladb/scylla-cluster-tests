#!/usr/bin/env bash
set -x
set -e

MCP_DIR=$1
SCT_ROOT_DIR=$1/../..
TEST_ID=$2
# NOTE: uncomment 'set +o xtrace' below if it is needed to debug getting of the ngrok auth token
#       This command is used to avoid printing of the auth token to the output.
set +o xtrace
# TODO: reuse ngrok key and stop running hydra each time we run this script
NGROK_AUTH_TOKEN=$(script -q -c "$SCT_ROOT_DIR/docker/env/hydra.sh python -m mcp.prometheus.get_ngrok_auth_token" | tail -n 1 | tr -d '\r\n[:space:]')
set -o xtrace
python3 $MCP_DIR/setup_prometheus.py $SCT_ROOT_DIR $NGROK_AUTH_TOKEN $TEST_ID
