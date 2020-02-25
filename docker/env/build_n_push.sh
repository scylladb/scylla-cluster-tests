# !/usr/bin/env bash
set -e

DOCKER_ENV_DIR=$(dirname $(readlink -f $0 ))
VERSION=v$(cat ${DOCKER_ENV_DIR}/version)

SCT_DIR=$(dirname $(dirname ${DOCKER_ENV_DIR}))
PY_PREREQS_FILE=requirements-python.txt
CENTOS_PREREQS_FILE=install-prereqs.sh

if [[ ! -z "`docker images scylladb/hydra:${VERSION} -q`" ]]; then
    echo "Image up-to-date"
else
    echo "Image with version $VERSION not found. Building..."
    cd ${DOCKER_ENV_DIR}
    cp -f ${SCT_DIR}/${PY_PREREQS_FILE} .
    cp -f ${SCT_DIR}/${CENTOS_PREREQS_FILE} .
    docker build --network=host -t scylladb/hydra:${VERSION} .
    rm -f ${PY_PREREQS_FILE} ${CENTOS_PREREQS_FILE}
    cd -
fi

echo "Tagging and pushing"
docker push scylladb/hydra:${VERSION}
