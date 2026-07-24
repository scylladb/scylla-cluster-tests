#!/bin/sh

clear

DOCKERS=$(docker ps -q -a)
if [ -n "$DOCKERS" ]; then
    echo "Killing running Docker containers..."
    docker stop $DOCKERS
    docker rm -f $DOCKERS
fi
(
    SCT_APPEND_SCYLLA_ARGS="--smp 2 --memory 2G" SCT_SCYLLA_VERSION=latest SCT_VECTOR_STORE_VERSION=latest ./docker/env/hydra.sh run-test vector_store_test.VectorStoreTest.test_noop \
      --backend docker \
      --config test-cases/vector-search/vector-search-test.yaml \
      2>&1
) | tee vector_store_test.log | grep -n "QWERTY QWERTY"
echo "Done"


# works

# (
#     SCT_APPEND_SCYLLA_ARGS="--smp 2 --memory 2G" SCT_SCYLLA_VERSION=2025.4.3 SCT_VECTOR_STORE_VERSION=latest SCT_ENABLE_ARGUS=false SCT_USE_MGMT=false ./docker/env/hydra.sh run-test vector_store_test.VectorStoreTest.test_noop \
#       --backend docker \
#       --config test-cases/PR-provision-test-docker.yaml \
#       --config configurations/vector_store_provision_test.yaml \
#       2>&1
# ) | tee vector_store_test.log
