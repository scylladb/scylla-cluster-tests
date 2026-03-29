#!/usr/bin/env bash
# Quick Docker backend test with a Cassandra cluster.
# Usage: ./run-docker-cassandra-test.sh [cassandra_version]
#
# Examples:
#   ./run-docker-cassandra-test.sh          # default: Cassandra 5.0
#   ./run-docker-cassandra-test.sh 4.1      # Cassandra 4.1
#   ./run-docker-cassandra-test.sh 5.0      # Cassandra 5.0

set -euo pipefail

CASSANDRA_VERSION="${1:-5.0}"

export SCT_SCYLLA_VERSION="2025.3.0"
export SCT_USE_MGMT=false
export SCT_CASSANDRA_VERSION="${CASSANDRA_VERSION}"

echo "=== Running Cassandra ${CASSANDRA_VERSION} Docker provision test ==="

uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
    --backend docker \
    --config test-cases/cassandra-docker-provision-test.yaml
